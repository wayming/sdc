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
					return nil, errors.New("Faield to decode html table overview-quote. Error: " + err.Error())
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

func (collector *SACollector) FirstTextNode(node *html.Node) *html.Node {

	if node.Type == html.TextNode && len(strings.TrimSpace(node.Data)) > 0 {
		// collector.logger.Println(node.Data)
		return node
	}

	for child := node.FirstChild; child != nil; child = child.NextSibling {
		textNode := collector.FirstTextNode(child)
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

	valLen := len(value)
	re = regexp.MustCompile(`^[.\d]+[BMT]?$`)
	if re.Match([]byte(value)) {

		multiplier := 1
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
		}

		valFloat, err := strconv.ParseFloat(baseNumber, 64)
		if err != nil {
			return nil, err
		}

		return valFloat * float64(multiplier), nil
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
			text1 := collector.FirstTextNode(td)
			if text1 != nil {
				collector.logger.Println(text1.Data)
			} else {
				// No text node for this sibling, try next one
				continue
			}

			for td2 := td.NextSibling; td2 != nil; td2 = td2.NextSibling {
				text2 := collector.FirstTextNode(td2)
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
			text1 := collector.FirstTextNode(td)
			if text1 != nil {
				collector.logger.Println(text1.Data)
			}

			for td2 := td.NextSibling; td2 != nil; td2 = td2.NextSibling {
				text2 := collector.FirstTextNode(td2)
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
			text1 := collector.FirstTextNode(td)
			if text1 != nil {
				collector.logger.Println(text1.Data)
			} else {
				continue
			}

			idx := 0
			// Assumes the same amount of tds as the the thead
			for td2 := td.NextSibling; td2 != nil && idx < len(completeSeries); td2 = td2.NextSibling {
				if td2.Type == html.ElementNode && td2.Data == "td" {
					text2 := collector.FirstTextNode(td2)
					if text2 != nil {
						collector.logger.Println(text2.Data)
						completeSeries[idx][normaliseJSONKey(text1.Data)] = text2.Data
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
	htmlContent, err := ReadURL(url, params)
	if err != nil {
		return "", err
	}

	htmlDoc, err := html.Parse(strings.NewReader(htmlContent))
	if err != nil {
		return "", errors.New("Failed to parse the html page " + url + ". Error: " + err.Error())
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

	return nil
}

func (collector *SACollector) CollectFinancialsForTrunk(symbols []string) error {
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

func (collector *SACollector) CollectFinancials(trunkSize int) error {
	type queryResult struct {
		Symbol string
	}

	sqlQuerySymbol := "select symbol from " + collector.dbSchema + "." + "ms_tickers"
	results, err := collector.dbLoader.RunQuery(sqlQuerySymbol, reflect.TypeFor[queryResult]())
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

	collector.logger.Println("Collect financials for " + strconv.Itoa(len(symbols)) + " symbols")
	return collector.CollectFinancialsForTrunk(symbols)
}

func CollectFinancials(logger *log.Logger, schemaName string, trunkSize int) error {
	dbLoader := dbloader.NewPGLoader(schemaName, logger)
	dbLoader.Connect(os.Getenv("PGHOST"),
		os.Getenv("PGPORT"),
		os.Getenv("PGUSER"),
		os.Getenv("PGPASSWORD"),
		os.Getenv("PGDATABASE"))

	collector := NewSACollector(dbLoader, logger, schemaName)
	if err := collector.CollectFinancials(trunkSize); err != nil {
		return err
	}
	return nil
}
