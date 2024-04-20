package collector

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/wayming/sdc/dbloader"
	"golang.org/x/net/html"
)

type SACollector struct {
	dbLoader      dbloader.DBLoader
	logger        *log.Logger
	dbSchema      string
	metricsFields map[string]map[string]JsonFieldMetadata
	accessKey     string
	thisSymbol    string
}

func NewSACollector(loader dbloader.DBLoader, logger *log.Logger, schema string) *SACollector {
	loader.CreateSchema(schema)
	loader.Exec("SET search_path TO " + schema)
	collector := SACollector{
		dbLoader:      loader,
		logger:        logger,
		dbSchema:      schema,
		metricsFields: AllSAMetricsFields(),
		accessKey:     "",
		thisSymbol:    "",
	}
	return &collector
}
func (collector *SACollector) Destroy() {
	collector.dbLoader.Disconnect()
}

// For unit testing
func (collector *SACollector) SetSymbol(symbol string) {
	collector.thisSymbol = symbol
}

func (collector *SACollector) DecodeDualTableHTML(node *html.Node, dataStructTypeName string) (map[string]interface{}, error) {
	var indicatorsMap map[string]interface{}
	var err error
	if node.Type == html.ElementNode {
		for _, attr := range node.Attr {
			if attr.Key == "data-test" && attr.Val == "overview-info" {
				indicatorsMap, err = collector.DecodeSimpleTable(node, dataStructTypeName)
				if err != nil {
					return nil, errors.New("Faield to decode html table overview-info. Error: " + err.Error())
				}

			}
			if attr.Key == "data-test" && attr.Val == "overview-quote" {
				indicatorsMap, err = collector.DecodeSimpleTable(node, dataStructTypeName)
				if err != nil {
					return nil, errors.New("Faield to decode html table overview-quote. Error: " + err.Error())
				}

			}
		}
	}

	for child := node.FirstChild; child != nil; child = child.NextSibling {
		var moreIndicators map[string]interface{}
		if moreIndicators, err = collector.DecodeDualTableHTML(child, dataStructTypeName); err != nil {
			return nil, err
		}
		if indicatorsMap, err = concatMaps(indicatorsMap, moreIndicators); err != nil {
			return nil, err
		}
	}

	return indicatorsMap, nil
}

func (collector *SACollector) DecodeTimeSeriesTableHTML(node *html.Node, dataStructTypeName string) ([]map[string]interface{}, error) {
	if node.Type == html.ElementNode {
		for _, attr := range node.Attr {
			if attr.Key == "data-test" && attr.Val == "financials" {
				indicatorMaps, err := collector.DecodeTimeSeriesTable(node, dataStructTypeName)
				if err != nil {
					return nil, errors.New("Faield to decode html table financials. Error: " + err.Error())
				}
				return indicatorMaps, nil
			}
		}
	}

	for child := node.FirstChild; child != nil; child = child.NextSibling {
		indicatorMaps, err := collector.DecodeTimeSeriesTableHTML(child, dataStructTypeName)
		if err != nil {
			return nil, err
		}
		if len(indicatorMaps) > 0 {
			return indicatorMaps, nil
		}
	}

	return nil, nil
}

func SearchText(node *html.Node, text string) bool {

	if node.Type == html.TextNode {
		regex, _ := regexp.Compile(".*" + text + ".*")
		if regex.Match([]byte(node.Data)) {
			return true
		}
	}

	for child := node.FirstChild; child != nil; child = child.NextSibling {
		if SearchText(child, text) {
			return true
		}
	}

	return false
}

func FirstTextNode(node *html.Node) *html.Node {

	if node.Type == html.TextNode && len(strings.TrimSpace(node.Data)) > 0 {
		// collector.logger.Println(node.Data)
		return node
	}

	for child := node.FirstChild; child != nil; child = child.NextSibling {
		textNode := FirstTextNode(child)
		if textNode != nil {
			return textNode
		}
	}

	return nil
}

func normaliseJSONKey(key string) string {
	// lower case name
	key = strings.ToLower(key)

	// trim spaces
	key = strings.TrimSpace(key)

	// replace space with underscore
	key = strings.ReplaceAll(key, " ", "_")

	// replace ampersand with underscore
	key = strings.ReplaceAll(key, "&", "_")

	// replace slash with underscore
	key = strings.ReplaceAll(key, "/", "_")

	// replace dash with underscore
	key = strings.ReplaceAll(key, "-", "_")

	// Replace commas with underscore
	key = strings.ReplaceAll(key, ",", "_")

	// remove apostrophe
	key = strings.ReplaceAll(key, "'", "")

	// remove parenthesis
	key = strings.ReplaceAll(key, "(", "")
	key = strings.ReplaceAll(key, ")", "")

	// remove consecutive underscore
	pattern := `_+`
	re := regexp.MustCompile(pattern)
	key = re.ReplaceAllString(key, "_")

	return key
}

func stringToFloat64(value string) (any, error) {

	// Remove spaces
	value = strings.ReplaceAll(value, " ", "")

	// Handle n/a. Return 0.
	if strings.ToLower(value) == "n/a" {
		return 0, nil
	}

	// Remove double quotes
	value = strings.ReplaceAll(value, "\"", "")

	// Remove commas
	value = strings.ReplaceAll(value, ",", "")

	// Remove (.*)
	re := regexp.MustCompile(`\(.*\)`)
	value = re.ReplaceAllString(value, "")

	// Sign operator
	sign := float64(1)
	if value[0] == '-' {
		if len(value) == 1 {
			return float64(0), nil
		}
		sign = -1
		value = value[1:]
	}

	valLen := len(value)
	re = regexp.MustCompile(`^[.\d]+[BMT%]?$`)
	if re.Match([]byte(value)) {

		multiplier := float64(1)
		baseNumber := value
		switch value[valLen-1] {
		case 'M':
			multiplier = multiplier * 1000 * 1000
			baseNumber = value[:valLen-1]
		case 'B':
			multiplier = multiplier * 1000 * 1000 * 1000
			baseNumber = value[:valLen-1]
		case 'T':
			multiplier = multiplier * 1000 * 1000 * 1000 * 1000
			baseNumber = value[:valLen-1]
		case '%':
			multiplier = multiplier / 100
			baseNumber = value[:valLen-1]
		}
		valFloat, err := strconv.ParseFloat(baseNumber, 64)
		if err != nil {
			return nil, err
		}

		return sign * valFloat * multiplier, nil
	} else {
		return nil, errors.New("Failed to convert value to " + reflect.Float64.String())
	}
}

func normaliseJSONValue(value string, vType reflect.Type) (any, error) {
	var convertedValue any
	var err error

	switch vType.Kind() {
	case reflect.Float64:
		if convertedValue, err = stringToFloat64(value); err != nil {
			return nil, err
		}
	case reflect.String:
		convertedValue = value
	}

	if vType == reflect.TypeFor[time.Time]() {
		if convertedValue, err = time.Parse("2006-01-02", value); err != nil {
			return convertedValue, err
		}
	}

	return convertedValue, nil
}

func (collector *SACollector) DecodeSimpleTable(node *html.Node, dataStructTypeName string) (map[string]interface{}, error) {
	simpleTableMetrics := make(map[string]interface{})
	// tbody
	tbody := node.FirstChild

	// For each tr
	for tr := tbody.FirstChild; tr != nil; tr = tr.NextSibling {
		td := tr.FirstChild
		if td != nil {
			text1 := FirstTextNode(td)
			if text1 != nil {
				collector.logger.Println(text1.Data)
			} else {
				// No text node for this sibling, try next one
				continue
			}

			for td2 := td.NextSibling; td2 != nil; td2 = td2.NextSibling {
				text2 := FirstTextNode(td2)
				if text2 != nil {
					normKey := normaliseJSONKey(text1.Data)
					fieldType := GetFieldTypeByTag(collector.metricsFields[dataStructTypeName], normKey)
					if fieldType == nil {
						return simpleTableMetrics, errors.New("Failed to get field type for tag " + normKey)
					}

					collector.logger.Println("Normalise " + text2.Data + " to " + fieldType.Name() + " value")
					// TODO - remove n/a value from map
					normVal, err := normaliseJSONValue(text2.Data, fieldType)
					if err != nil {
						return simpleTableMetrics, err
					}

					simpleTableMetrics[normKey] = normVal
					continue
				}
			}
		}
	}

	collector.PackSymbolField(simpleTableMetrics, dataStructTypeName)
	return simpleTableMetrics, nil

}

func (collector *SACollector) PackSymbolField(metrics map[string]interface{}, dataStructTypeName string) {
	_, ok := collector.metricsFields[dataStructTypeName]["Symbol"]
	if ok {
		if _, ok := metrics["Symbol"]; !ok {
			metrics["Symbol"] = collector.thisSymbol
		}
	}
}

func (collector *SACollector) DecodeTimeSeriesTable(node *html.Node, dataStructTypeName string) ([]map[string]interface{}, error) {
	completeSeries := make([]map[string]interface{}, 0)
	// thead
	thead := node.FirstChild

	pattern := `[a-zA-Z]+`
	re := regexp.MustCompile(pattern)

	// For each tr
	for tr := thead.FirstChild; tr != nil; tr = tr.NextSibling {
		td := tr.FirstChild
		if td != nil {
			text1 := FirstTextNode(td)
			if text1 != nil {
				collector.logger.Println(text1.Data)
			}

			for td2 := td.NextSibling; td2 != nil; td2 = td2.NextSibling {
				text2 := FirstTextNode(td2)
				if text2 != nil {
					dataPoint := make(map[string]interface{})
					collector.logger.Println(text2.Data)
					if matches := re.FindAllString(text2.Data, -1); len(matches) > 0 {
						collector.logger.Println("ignore ", text2.Data)
						continue
					}

					normKey := normaliseJSONKey(text1.Data)
					fieldType := GetFieldTypeByTag(collector.metricsFields[dataStructTypeName], normKey)
					if fieldType == nil {
						return completeSeries, errors.New("Failed to get field type for tag " + normKey)
					}

					collector.logger.Println("Normalise " + text2.Data + " to " + fieldType.Name() + " value")
					normVal, err := normaliseJSONValue(text2.Data, fieldType)
					if err != nil {
						return completeSeries, err
					}

					dataPoint[normKey] = normVal
					completeSeries = append(completeSeries, dataPoint)
					continue
				}
			}
		}
	}

	// tbody
	if thead.NextSibling == nil || thead.NextSibling.NextSibling == nil {
		return nil, errors.New("unexpected structure. Can not find the tbody element")
	}
	tbody := thead.NextSibling.NextSibling
	// For each tr
	for tr := tbody.FirstChild; tr != nil; tr = tr.NextSibling {
		td := tr.FirstChild
		if td != nil {
			text1 := FirstTextNode(td)
			if text1 != nil {
				collector.logger.Println(text1.Data)
			} else {
				continue
			}

			idx := 0
			// Assumes the same amount of tds as the the thead
			for td2 := td.NextSibling; td2 != nil && idx < len(completeSeries); td2 = td2.NextSibling {
				if td2.Type == html.ElementNode && td2.Data == "td" {
					text2 := FirstTextNode(td2)
					if text2 != nil {
						collector.logger.Println(text2.Data)

						normKey := normaliseJSONKey(text1.Data)
						fieldType := GetFieldTypeByTag(collector.metricsFields[dataStructTypeName], normKey)
						if fieldType == nil {
							return completeSeries, errors.New("Failed to get field type for tag " + normKey)
						}

						collector.logger.Println("Normalise " + text2.Data + " to " + fieldType.Name() + " value")
						normVal, err := normaliseJSONValue(text2.Data, fieldType)
						if err != nil {
							return completeSeries, err
						}

						completeSeries[idx][normKey] = normVal
						idx++
					}
				}
			}

		}
	}

	// Fill symbol name
	for _, dataPoint := range completeSeries {
		collector.PackSymbolField(dataPoint, dataStructTypeName)
	}

	return completeSeries, nil

}

// Parse Stock Analysis page and return JSON text
func (collector *SACollector) ReadOverallPage(url string, params map[string]string, dataStructTypeName string) (string, error) {
	collector.logger.Println("Read " + url)

	htmlContent, err := ReadURL(url, params)
	if err != nil {
		return "", err
	}

	htmlDoc, err := html.Parse(strings.NewReader(htmlContent))
	if err != nil {
		return "", errors.New("Failed to parse the html page " + url + ". Error: " + err.Error())
	}

	collector.logger.Println("Decode html doc with JSON struct " + dataStructTypeName)
	indicatorsMap, err := collector.DecodeDualTableHTML(htmlDoc, dataStructTypeName)
	if err != nil {
		return "", errors.New("Failed to parse " + url + ". Error: " + err.Error())
	}
	if len(indicatorsMap) == 0 {
		return "", errors.New("No indicator found from overall page " + url)
	}

	mapSlice := []map[string]interface{}{indicatorsMap}

	jsonData, err := json.Marshal(mapSlice)
	if err != nil {
		return "", errors.New("Failed to marshal stock data to JSON text. Error: " + err.Error())
	} else {
		collector.logger.Println("JSON text generated - " + string(jsonData))

	}
	return string(jsonData), nil
}

func (collector *SACollector) CollectOverallMetrics(symbol string, dataStructType reflect.Type) (int64, error) {
	collector.thisSymbol = symbol
	overallUrl := "https://stockanalysis.com/stocks/" + symbol
	overalTable := "sa_overall"
	jsonText, err := collector.ReadOverallPage(overallUrl, nil, dataStructType.Name())
	if err != nil {
		return 0, errors.New("Failed to scrap data from url " + overallUrl + ". Error: " + err.Error())
	}

	numOfRows, err := collector.dbLoader.LoadByJsonText(jsonText, overalTable, reflect.TypeFor[StockOverview]())
	if err != nil {
		return 0, errors.New("Failed to load data into table " + overalTable + ". Error: " + err.Error())
	}

	collector.logger.Println(numOfRows, "rows have been loaded into", overalTable)
	return numOfRows, nil
}

// Parse Stock Analysis page and return JSON text
func (collector *SACollector) ReadTimeSeriesPage(url string, params map[string]string, dataStructTypeName string) (string, error) {
	collector.logger.Println("Load data from " + url)
	htmlContent, err := ReadURL(url, params)
	if err != nil {
		return "", err
	}

	htmlDoc, err := html.Parse(strings.NewReader(htmlContent))
	if err != nil {
		return "", errors.New("Failed to parse the html page " + url + ". Error: " + err.Error())
	}

	if SearchText(htmlDoc, "No quarterly.*available for this stock") {
		return "", errors.New("Ignore the symbol " + collector.thisSymbol + ". No quarterly data available")
	}

	indicatorsMap, err := collector.DecodeTimeSeriesTableHTML(htmlDoc, dataStructTypeName)
	if err != nil {
		return "", errors.New("Failed to parse " + url + ". Error: " + err.Error())
	}
	if len(indicatorsMap) == 0 {
		return "", errors.New("No indicator found from financials " + url)
	}

	jsonData, err := json.Marshal(indicatorsMap)
	if err != nil {
		return "", errors.New("Failed to marshal stock data to JSON text. Error: " + err.Error())
	} else {
		collector.logger.Println("JSON text generated - " + string(jsonData))

	}
	return string(jsonData), nil
}

func (collector *SACollector) CollectFinancialsIncome(symbol string, dataStructType reflect.Type) (int64, error) {
	collector.thisSymbol = symbol
	financialsIncome := "https://stockanalysis.com/stocks/" + symbol + "/financials/?p=quarterly"
	return collector.LoadTimeSeriesPage(financialsIncome, dataStructType, "sa_financials_income")
}

func (collector *SACollector) CollectFinancialsBalanceSheet(symbol string, dataStructType reflect.Type) (int64, error) {
	collector.thisSymbol = symbol
	financialsBalanceSheet := "https://stockanalysis.com/stocks/" + symbol + "/financials/balance-sheet/?p=quarterly"
	return collector.LoadTimeSeriesPage(financialsBalanceSheet, dataStructType, "sa_financials_balance_sheet")
}

func (collector *SACollector) CollectFinancialsCashFlow(symbol string, dataStructType reflect.Type) (int64, error) {
	collector.thisSymbol = symbol
	financialsICashFlow := "https://stockanalysis.com/stocks/" + symbol + "/financials/cash-flow-statement/?p=quarterly"
	return collector.LoadTimeSeriesPage(financialsICashFlow, dataStructType, "sa_financials_cash_flow")
}

func (collector *SACollector) CollectFinancialsRatios(symbol string, dataStructType reflect.Type) (int64, error) {
	collector.thisSymbol = symbol
	financialsRatios := "https://stockanalysis.com/stocks/" + symbol + "/financials/ratios/?p=quarterly"
	return collector.LoadTimeSeriesPage(financialsRatios, dataStructType, "sa_financials_ratios")
}

func (collector *SACollector) LoadTimeSeriesPage(url string, dataStructType reflect.Type, dbTableName string) (int64, error) {

	jsonText, err := collector.ReadTimeSeriesPage(url, nil, dataStructType.Name())
	if err != nil {
		return 0, errors.New("Failed to scrap data from url " + url + ". Error: " + err.Error())
	}

	numOfRows, err := collector.dbLoader.LoadByJsonText(jsonText, dbTableName, dataStructType)
	if err != nil {
		return 0, errors.New("Failed to load data into table " + dbTableName + ". Error: " + err.Error())
	}

	collector.logger.Println(numOfRows, "rows have been loaded into", dbTableName)
	return numOfRows, nil
}

func (collector *SACollector) CollectFinancialsForSymbol(symbol string) error {

	if _, err := collector.CollectOverallMetrics(symbol, reflect.TypeFor[StockOverview]()); err != nil {
		return err
	}
	if _, err := collector.CollectFinancialsIncome(symbol, reflect.TypeFor[FinancialsIncome]()); err != nil {
		return err
	}
	if _, err := collector.CollectFinancialsBalanceSheet(symbol, reflect.TypeFor[FinancialsBalanceShet]()); err != nil {
		return err
	}
	if _, err := collector.CollectFinancialsCashFlow(symbol, reflect.TypeFor[FinancialsCashFlow]()); err != nil {
		return err
	}
	if _, err := collector.CollectFinancialsRatios(symbol, reflect.TypeFor[FinancialRatios]()); err != nil {
		return err
	}
	return nil
}

func (collector *SACollector) CollectFinancialsForSymbols(symbols []string) error {
	collector.logger.Println("Begin collecting financials for [" + strings.Join(symbols, ",") + "].")
	collected := 0
	ignored := make([]string, 0)
	for _, symbol := range symbols {
		err := collector.CollectFinancialsForSymbol(symbol)
		if err != nil {
			collector.logger.Println("Failed to collect financials for symbol " + symbol + ", Error: " + err.Error())
			ignored = append(ignored, symbol)
		} else {
			collected++
		}
	}
	fmt.Println("Collected financials for " + strconv.Itoa(collected) + " symbols.")
	fmt.Println("Ignored symbols are: [" + strings.Join(ignored, ",") + "]")
	return nil
}

func CollectFinancials(schemaName string, trunkSize int) error {
	var err error
	file, _ := os.OpenFile(LOG_FILE, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	logger := log.New(file, "sdc: ", log.Ldate|log.Ltime)
	defer file.Close()

	dbLoader := dbloader.NewPGLoader(schemaName, logger)
	dbLoader.Connect(os.Getenv("PGHOST"),
		os.Getenv("PGPORT"),
		os.Getenv("PGUSER"),
		os.Getenv("PGPASSWORD"),
		os.Getenv("PGDATABASE"))
	defer dbLoader.Disconnect()

	type queryResult struct {
		Symbol string
	}

	sqlQuerySymbol := "select symbol from ms_tickers"
	results, err := dbLoader.RunQuery(sqlQuerySymbol, reflect.TypeFor[queryResult]())
	if err != nil {
		return errors.New("Failed to run query [" + sqlQuerySymbol + "]. Error: " + err.Error())
	}
	queryResults, ok := results.([]queryResult)
	if !ok {
		return errors.New("failed to run assert the query results are returned as a slice of queryResults")
	}

	symbols := make([]string, 0)
	for _, row := range queryResults {
		symbols = append(symbols, strings.ToLower(row.Symbol))
	}

	begin := 0
	errChan := make(chan error)
	defer close(errChan)
	for begin < len(symbols) {
		end := begin + trunkSize
		if end > len(symbols) {
			end = len(symbols)
		}
		go func(symbols []string, loggerFilePostfix string, schemaName string, errChan chan error) {

			file, _ := os.OpenFile(LOG_FILE+"."+loggerFilePostfix, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
			logger := log.New(file, "sdc: ", log.Ldate|log.Ltime)
			defer file.Close()

			subLoader := dbloader.NewPGLoader(schemaName, logger)
			subLoader.Connect(os.Getenv("PGHOST"),
				os.Getenv("PGPORT"),
				os.Getenv("PGUSER"),
				os.Getenv("PGPASSWORD"),
				os.Getenv("PGDATABASE"))
			defer subLoader.Disconnect()

			collector := NewSACollector(subLoader, logger, schemaName)
			if err := collector.CollectFinancialsForSymbols(symbols); err != nil {
				errChan <- err
			}
			collector.Destroy()
		}(symbols[begin:end], strconv.Itoa(begin), schemaName, errChan)
		begin = end
	}

	var message string
	for err = range errChan {
		logger.Println(err.Error())
		message = message + err.Error()
	}
	if len(message) > 0 {
		return errors.New(message)
	}

	return nil
}

func CollectFinancialsForSymbol(schemaName string, symbol string) error {
	file, _ := os.OpenFile(LOG_FILE, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	logger := log.New(file, "sdc: ", log.Ldate|log.Ltime)
	defer file.Close()

	dbLoader := dbloader.NewPGLoader(schemaName, logger)
	dbLoader.Connect(os.Getenv("PGHOST"),
		os.Getenv("PGPORT"),
		os.Getenv("PGUSER"),
		os.Getenv("PGPASSWORD"),
		os.Getenv("PGDATABASE"))
	defer dbLoader.Disconnect()

	collector := NewSACollector(dbLoader, logger, schemaName)
	if err := collector.CollectFinancialsForSymbol(symbol); err != nil {
		return err
	}
	fmt.Println("Collect financials for symbol " + symbol)

	return nil
}
