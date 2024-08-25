package collector

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"reflect"
	"regexp"
	"strings"

	"github.com/wayming/sdc/config"
	"github.com/wayming/sdc/dbloader"
	"github.com/wayming/sdc/sdclogger"
	"golang.org/x/net/html"
)

type SACollector struct {
	loader        dbloader.DBLoader
	reader        IHttpReader
	logger        *log.Logger
	htmlParser    *SAHTMLParser
	metricsFields map[string]map[string]JsonFieldMetadata
	thisSymbol    string
}

func NewSACollector(httpReader IHttpReader, exporters IDataExporter, db dbloader.DBLoader, l *log.Logger) *SACollector {
	logger := l
	if logger == nil {
		logger = sdclogger.SDCLoggerInstance.Logger
	}
	collector := SACollector{
		loader:        db,
		reader:        httpReader,
		logger:        logger,
		htmlParser:    NewSAHTMLParser(logger),
		metricsFields: AllSAMetricsFields(),
		thisSymbol:    "",
	}
	return &collector
}

// For unit testing
func (c *SACollector) SetSymbol(symbol string) {
	c.thisSymbol = symbol
}

func (c *SACollector) CreateTables() error {
	allTables := map[string]reflect.Type{
		SADataTables[SA_REDIRECTED_SYMBOLS]:     SADataTypes[SA_REDIRECTED_SYMBOLS],
		SADataTables[SA_STOCKOVERVIEW]:          SADataTypes[SA_STOCKOVERVIEW],
		SADataTables[SA_FINANCIALSINCOME]:       SADataTypes[SA_FINANCIALSINCOME],
		SADataTables[SA_FINANCIALSBALANCESHEET]: SADataTypes[SA_FINANCIALSBALANCESHEET],
		SADataTables[SA_FINANCIALSCASHFLOW]:     SADataTypes[SA_FINANCIALSCASHFLOW],
		SADataTables[SA_FINANCIALRATIOS]:        SADataTypes[SA_FINANCIALRATIOS],
		SADataTables[SA_ANALYSTSRATING]:         SADataTypes[SA_ANALYSTSRATING],
	}

	for k, v := range allTables {
		if err := c.loader.CreateTableByJsonStruct(k, v); err != nil {
			return err
		}
	}

	c.logger.Println("All tables created")
	return nil
}

func (c *SACollector) MapRedirectedSymbol(symbol string) (string, error) {
	redirected := c.redirectdSymbol(symbol)
	if len(redirected) == 0 {
		return "", nil
	}

	redirectMap := make(map[string]string)
	redirectMap["symbol"] = symbol
	redirectMap["redirected_symbol"] = redirected
	mapSlice := []map[string]string{redirectMap}
	jsonText, err := json.Marshal(mapSlice)
	if err != nil {
		return "", errors.New("Failed to marshal redirect map to JSON text. Error: " + err.Error())
	} else {
		c.logger.Println("JSON text generated - " + string(jsonText))
	}

	numOfRows, err := c.loader.LoadByJsonText(string(jsonText), SADataTables[SA_REDIRECTED_SYMBOLS], reflect.TypeFor[RedirectedSymbols]())
	if err != nil {
		return "", errors.New("Failed to load data into table " + SADataTables[SA_REDIRECTED_SYMBOLS] + ". Error: " + err.Error())
	}

	c.logger.Println(numOfRows, "rows have been loaded into", SADataTables[SA_REDIRECTED_SYMBOLS])
	return redirected, nil
}

// Extract and write financial overview to database.
func (c *SACollector) CollectFinancialOverview(symbol string) (int64, error) {
	c.thisSymbol = symbol

	// err := c.loader.Exec(
	// 	"DELETE FROM " + SADataTables[SA_STOCKOVERVIEW] +
	// 		" WHERE symbol = " + strconv.Quote(strings.ToUpper(symbol)))
	// if err != nil {
	// 	c.logger.Println(err.Error())
	// }

	overallUrl := "https://stockanalysis.com/stocks/" + symbol
	jsonText, err := c.readOverviewPage(overallUrl, nil)
	if err != nil {
		return 0, err
	}

	numOfRows, err := c.loader.LoadByJsonText(jsonText, SADataTables[SA_STOCKOVERVIEW], reflect.TypeFor[StockOverview]())
	if err != nil {
		return 0, errors.New("Failed to load data into table " + SADataTables[SA_STOCKOVERVIEW] + ". Error: " + err.Error())
	}

	c.logger.Println(numOfRows, "rows have been loaded into", SADataTables[SA_STOCKOVERVIEW])
	return numOfRows, nil
}

// Extract and write financial details to database. Only return the last error
func (c *SACollector) CollectFinancialDetails(symbol string) error {
	var retErr error
	if _, err := c.CollectFinancialsIncome(symbol); err != nil {
		retErr = err
	}
	if _, err := c.CollectFinancialsBalanceSheet(symbol); err != nil {
		retErr = err
	}
	if _, err := c.CollectFinancialsCashFlow(symbol); err != nil {
		retErr = err
	}
	if _, err := c.CollectFinancialsRatios(symbol); err != nil {
		retErr = err
	}
	if _, err := c.CollectAnalystRatings(symbol); err != nil {
		retErr = err
	}
	return retErr
}

func (c *SACollector) CollectFinancialsIncome(symbol string) (int64, error) {
	c.thisSymbol = symbol
	financialsIncome := "https://stockanalysis.com/stocks/" + strings.ToLower(symbol) + "/financials/?p=quarterly"
	return c.collectFinancialDetailsCommon(financialsIncome, SADataTypes[SA_FINANCIALSINCOME], SADataTables[SA_FINANCIALSINCOME])
}

func (c *SACollector) CollectFinancialsBalanceSheet(symbol string) (int64, error) {
	c.thisSymbol = symbol
	financialsBalanceSheet := "https://stockanalysis.com/stocks/" + strings.ToLower(symbol) + "/financials/balance-sheet/?p=quarterly"
	return c.collectFinancialDetailsCommon(financialsBalanceSheet, SADataTypes[SA_FINANCIALSBALANCESHEET], SADataTables[SA_FINANCIALSBALANCESHEET])
}

func (c *SACollector) CollectFinancialsCashFlow(symbol string) (int64, error) {
	c.thisSymbol = symbol
	financialsICashFlow := "https://stockanalysis.com/stocks/" + strings.ToLower(symbol) + "/financials/cash-flow-statement/?p=quarterly"
	return c.collectFinancialDetailsCommon(financialsICashFlow, SADataTypes[SA_FINANCIALSCASHFLOW], SADataTables[SA_FINANCIALSCASHFLOW])
}

func (c *SACollector) CollectFinancialsRatios(symbol string) (int64, error) {
	c.thisSymbol = symbol
	financialsRatios := "https://stockanalysis.com/stocks/" + strings.ToLower(symbol) + "/financials/ratios/?p=quarterly"
	return c.collectFinancialDetailsCommon(financialsRatios, SADataTypes[SA_FINANCIALRATIOS], SADataTables[SA_FINANCIALRATIOS])
}

func (c *SACollector) CollectAnalystRatings(symbol string) (int64, error) {
	c.thisSymbol = symbol
	url := "https://stockanalysis.com/stocks/" + strings.ToLower(symbol) + "/ratings"

	jsonText, err := c.readAnalystRatingsPage(url, nil)
	if err != nil {
		return 0, err
	}

	numOfRows, err := c.loader.LoadByJsonText(jsonText, SADataTables[SA_ANALYSTSRATING], SADataTypes[SA_ANALYSTSRATING])
	if err != nil {
		return 0, errors.New("Failed to load data into table " + SADataTables[SA_ANALYSTSRATING] + ". Error: " + err.Error())
	}

	c.logger.Println(numOfRows, "rows have been loaded into", SADataTables[SA_ANALYSTSRATING])
	return numOfRows, nil
}

func (c *SACollector) collectFinancialDetailsCommon(url string, dataStructType reflect.Type, dbTableName string) (int64, error) {

	jsonText, err := c.readFinanaceDetailsPage(url, nil, dataStructType.Name())
	if err != nil {
		return 0, err
	}

	numOfRows, err := c.loader.LoadByJsonText(jsonText, dbTableName, dataStructType)
	if err != nil {
		return 0, errors.New("Failed to load data into table " + dbTableName + ". Error: " + err.Error())
	}

	c.logger.Println(numOfRows, "rows have been loaded into", dbTableName)
	return numOfRows, nil
}

// Read page from SA and extract the information
func (c *SACollector) readAnalystRatingsPage(url string, params map[string]string) (string, error) {
	c.logger.Println("Read " + url)

	htmlContent, err := c.reader.Read(url, params)
	if err != nil {
		return "", err
	}

	htmlDoc, err := html.Parse(strings.NewReader(htmlContent))
	if err != nil {
		return "", errors.New("Failed to parse the html page " + url + ". Error: " + err.Error())
	}

	c.logger.Printf("Decode html doc with JSON struct %s", SADataTypes[SA_ANALYSTSRATING].Name())
	indicatorsMap, err := c.htmlParser.DecodeAnalystRatingsGrid(htmlDoc, SADataTypes[SA_ANALYSTSRATING].Name())

	if err != nil {
		return "", errors.New("Failed to parse " + url + ". Error: " + err.Error())
	}
	if len(indicatorsMap) == 0 {
		return "", errors.New("No indicator found from analyst ratings page " + url)
	}

	// Add symbol to the struct if needed
	c.packSymbolField(indicatorsMap, SADataTypes[SA_ANALYSTSRATING].Name())

	mapSlice := []map[string]interface{}{indicatorsMap}
	jsonData, err := json.Marshal(mapSlice)
	if err != nil {
		return "", errors.New("Failed to marshal stock data to JSON text. Error: " + err.Error())
	} else {
		c.logger.Println("JSON text generated - " + string(jsonData))

	}
	return string(jsonData), nil
}

// Read page from SA and extract the information
func (c *SACollector) readOverviewPage(url string, params map[string]string) (string, error) {
	c.logger.Println("Read " + url)

	htmlContent, err := c.reader.Read(url, params)
	if err != nil {
		return "", err
	}

	htmlDoc, err := html.Parse(strings.NewReader(htmlContent))
	if err != nil {
		return "", errors.New("Failed to parse the html page " + url + ". Error: " + err.Error())
	}

	c.logger.Println("Decode html doc with JSON struct " + SADataTypes[SA_STOCKOVERVIEW].Name())
	indicatorsMap, err := c.htmlParser.DecodeOverviewPages(htmlDoc, SADataTypes[SA_STOCKOVERVIEW].Name())
	if err != nil {
		return "", errors.New("Failed to parse " + url + ". Error: " + err.Error())
	}
	if len(indicatorsMap) == 0 {
		return "", errors.New("No indicator found from overall page " + url)
	}

	// Add symbol to the struct if needed
	c.packSymbolField(indicatorsMap, SADataTypes[SA_STOCKOVERVIEW].Name())

	mapSlice := []map[string]interface{}{indicatorsMap}

	jsonData, err := json.Marshal(mapSlice)
	if err != nil {
		return "", errors.New("Failed to marshal stock data to JSON text. Error: " + err.Error())
	} else {
		c.logger.Println("JSON text generated - " + string(jsonData))

	}
	return string(jsonData), nil
}

// Read page from SA and extract the information
func (c *SACollector) readFinanaceDetailsPage(url string, params map[string]string, dataStructTypeName string) (string, error) {
	c.logger.Println("Load data from " + url)
	htmlContent, err := c.reader.Read(url, params)
	if err != nil {
		return "", err
	}

	htmlDoc, err := html.Parse(strings.NewReader(htmlContent))
	if err != nil {
		return "", errors.New("Failed to parse the html page " + url + ". Error: " + err.Error())
	}

	if searchText(htmlDoc, "No quarterly.*available for this stock") != nil {
		return "", errors.New("Ignore the symbol " + c.thisSymbol + ". No quarterly data available")
	}

	indicatorsMap, err := c.htmlParser.DecodeFinancialsPage(htmlDoc, dataStructTypeName)
	if err != nil {
		return "", errors.New("Failed to parse " + url + ". Error: " + err.Error())
	}
	if len(indicatorsMap) == 0 {
		return "", errors.New("No indicator found from financials " + url)
	}

	// Add symbol to the struct if needed
	for _, datapoint := range indicatorsMap {
		c.packSymbolField(datapoint, dataStructTypeName)
	}

	jsonData, err := json.Marshal(indicatorsMap)
	if err != nil {
		return "", errors.New("Failed to marshal stock data to JSON text. Error: " + err.Error())
	} else {
		c.logger.Println("JSON text generated - " + string(jsonData))

	}
	return string(jsonData), nil
}

func (c *SACollector) packSymbolField(metrics map[string]interface{}, dataStructTypeName string) {
	_, ok := c.metricsFields[dataStructTypeName]["Symbol"]
	if ok {
		if _, ok := metrics["Symbol"]; !ok {
			metrics["Symbol"] = c.thisSymbol
		}
	}
}

func (c *SACollector) redirectdSymbol(symbol string) string {
	symbol = strings.ToLower(symbol)
	url := "https://stockanalysis.com/stocks/" + strings.ToLower(symbol) + "/financials/?p=quarterly"
	redirectedURL, _ := c.reader.RedirectedUrl(url)
	if redirectedURL == url {
		c.logger.Printf("no redirected symbol found for %s", symbol)
		return ""
	}

	pattern := "stocks/([A-Za-z]+)/"
	regexp, err := regexp.Compile(pattern)
	if err != nil {
		c.logger.Printf("failed to compile pattern %s", pattern)
	}

	match := regexp.FindStringSubmatch(redirectedURL)
	if len(match) > 1 {
		return match[1]
	} else {
		return ""
	}
}

// // Entry function
// func Init(schemaName string, proxyFile string, isContinue bool) error {
// 	var allSymbols int64
// 	var err error

// 	cacheManager := cache.NewCacheManager()
// 	if err := cacheManager.Connect(); err != nil {
// 		return err
// 	}
// 	defer cacheManager.Disconnect()

// 	if !isContinue {
// 		if err := ClearCache(); err != nil {
// 			return err
// 		}
// 		allSymbols, err = cache.LoadSymbols(cacheManager, CACHE_KEY_SYMBOL, schemaName)
// 		if err != nil {
// 			return errors.New("Failed to load symbols to cache. Error: " + err.Error())
// 		}
// 		sdclogger.SDCLoggerInstance.Printf("Loaded %d symbols to cache", allSymbols)

// 		allProxies, err := cache.LoadProxies(cacheManager, CACHE_KEY_PROXY, proxyFile)
// 		if err != nil {
// 			return errors.New("Failed to load proxies to cache. Error: " + err.Error())
// 		}
// 		sdclogger.SDCLoggerInstance.Printf("Loaded %d proxies to cache", allProxies)
// 	} else {
// 		if err := cacheManager.MoveSet(CACHE_KEY_SYMBOL_ERROR, CACHE_KEY_SYMBOL); err != nil {
// 			return fmt.Errorf("failed to restore the error symbols. Error: %s", err.Error())
// 		}
// 	}

// 	// Create tables
// 	dbLoader := dbloader.NewPGLoader(schemaName, sdclogger.SDCLoggerInstance.Logger)
// 	dbLoader.Connect(os.Getenv("PGHOST"),
// 		os.Getenv("PGPORT"),
// 		os.Getenv("PGUSER"),
// 		os.Getenv("PGPASSWORD"),
// 		os.Getenv("PGDATABASE"))
// 	defer dbLoader.Disconnect()
// 	collector := NewSACollector(dbLoader, nil, sdclogger.SDCLoggerInstance.Logger, schemaName)
// 	if err := c.CreateTables(); err != nil {
// 		sdclogger.SDCLoggerInstance.Printf("Failed to create tables. Error: %s", err)
// 		return err
// 	} else {
// 		sdclogger.SDCLoggerInstance.Printf("All tables created")
// 		return nil
// 	}

// }

// // Entry function
// func CollectFinancials(schemaName string, proxyFile string, parallel int, isContinue bool) (int64, error) {
// 	var allSymbols int64
// 	var errorSymbols int64

// 	if err := Init(schemaName, proxyFile, isContinue); err != nil {
// 		return 0, err
// 	}

// 	cacheManager := cache.NewCacheManager()
// 	if err := cacheManager.Connect(); err != nil {
// 		return 0, err
// 	}
// 	defer cacheManager.Disconnect()

// 	var wg sync.WaitGroup
// 	outChan := make(chan string)
// 	for i := 0; i < parallel; i++ {
// 		wg.Add(1)
// 		go func(goID string, schemaName string, outChan chan string, wg *sync.WaitGroup) {
// 			defer wg.Done()

// 			// Logger
// 			file, _ := os.OpenFile(LOG_FILE+"."+goID, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
// 			logger := log.New(file, "sdc: ", log.Ldate|log.Ltime)
// 			defer file.Close()
// 			logger.Println("[Go" + goID + "] started.")

// 			// dbloader
// 			dbLoader := dbloader.NewPGLoader(schemaName, logger)
// 			dbLoader.Connect(os.Getenv("PGHOST"),
// 				os.Getenv("PGPORT"),
// 				os.Getenv("PGUSER"),
// 				os.Getenv("PGPASSWORD"),
// 				os.Getenv("PGDATABASE"))
// 			defer dbLoader.Disconnect()

// 			// http reader
// 			proxy, err := cacheManager.GetFromSet(CACHE_KEY_PROXY)
// 			if err != nil {
// 				outChan <- err.Error()
// 				return
// 			}
// 			proxyClt, err := NewProxyClient(proxy)
// 			if err != nil {
// 				outChan <- err.Error()
// 				return
// 			}
// 			httpReader := NewHttpReader(proxyClt)

// 			collector := NewSACollector(dbLoader, httpReader, logger, schemaName)

// 			for remainingSymbols, _ := cacheManager.GetLength(CACHE_KEY_SYMBOL); remainingSymbols > 0; {

// 				if remainingProxies, _ := cacheManager.GetLength(CACHE_KEY_PROXY); remainingProxies == 0 {
// 					errorStr := fmt.Sprintf("[Go%s]No proxy server available", goID)
// 					logger.Println(errorStr)
// 					outChan <- errors.New(errorStr).Error()
// 					break
// 				}

// 				if remainingSymbols, _ := cacheManager.GetLength(CACHE_KEY_SYMBOL); remainingSymbols == 0 {
// 					logger.Printf("[Go%s]No symbol left", goID)
// 					break
// 				}

// 				nextSymbol, err := cacheManager.PopFromSet(CACHE_KEY_SYMBOL)
// 				if err != nil {
// 					logger.Printf("[Go%s] Failed to get symbol from cache. Error:%s", goID, err.Error())
// 					outChan <- err.Error()
// 					continue
// 				}
// 				if nextSymbol == "" {
// 					logger.Printf("[Go%s]No symbol left", goID)
// 					break
// 				}

// 				// If redirected
// 				redirected, err := c.MapRedirectedSymbol(nextSymbol)
// 				if err != nil {
// 					logger.Printf("[Go%s] error: %s", goID, err.Error())
// 					logger.Printf("[Go%s] Add %s to cache set %s", goID, nextSymbol, CACHE_KEY_SYMBOL_ERROR)

// 					cacheKey := CACHE_KEY_SYMBOL_ERROR
// 					e, ok := err.(HttpServerError)
// 					if ok && e.StatusCode() == HTTP_ERROR_NOT_FOUND {
// 						cacheKey := CACHE_KEY_SYMBOL_INVALID
// 						logger.Printf("Add symbol %s to cache key %s", nextSymbol, cacheKey)
// 					}

// 					if cacheError := cacheManager.AddToSet(cacheKey, nextSymbol); cacheError != nil {
// 						logger.Printf("[Go%s] error: %s", goID, cacheError.Error())
// 						outChan <- cacheError.Error()
// 					}
// 				}
// 				if len(redirected) > 0 {
// 					nextSymbol = redirected
// 				}

// 				if err := c.CollectFinancialDetails(nextSymbol); err != nil {
// 					logger.Printf("[Go%s] error: %s", goID, err.Error())
// 					logger.Printf("[Go%s] Add %s to cache set %s", goID, nextSymbol, CACHE_KEY_SYMBOL_ERROR)

// 					cacheKey := CACHE_KEY_SYMBOL_ERROR
// 					e, ok := err.(HttpServerError)
// 					if ok && e.StatusCode() == HTTP_ERROR_NOT_FOUND {
// 						cacheKey := CACHE_KEY_SYMBOL_INVALID
// 						logger.Printf("Add symbol %s to cache key %s", nextSymbol, cacheKey)
// 					}

// 					if cacheError := cacheManager.AddToSet(cacheKey, nextSymbol); cacheError != nil {
// 						logger.Printf("[Go%s] error: %s", goID, cacheError.Error())
// 						outChan <- cacheError.Error()
// 					}

// 				}
// 			}

// 			logger.Println("[Go" + goID + "] finished.")
// 		}(strconv.Itoa(i), schemaName, outChan, &wg)
// 	}

// 	go func() {
// 		wg.Wait()
// 		close(outChan)
// 	}()

// 	var errorMessage string
// 	for out := range outChan {
// 		sdclogger.SDCLoggerInstance.Printf(out)
// 		errorMessage += fmt.Sprintf("%s\n", out)
// 	}

// 	if remainingProxies, _ := cacheManager.GetLength(CACHE_KEY_PROXY); remainingProxies == 0 {
// 		errorMessage += "Running out of proxy servers.\n"
// 	}

// 	if errorLen, _ := cacheManager.GetLength(CACHE_KEY_SYMBOL_ERROR); errorLen > 0 {
// 		errorSymbols, _ := cacheManager.GetAllFromSet(CACHE_KEY_SYMBOL_ERROR)
// 		errorMessage += fmt.Sprintf("Error symbols [%s]\n", strings.Join(errorSymbols, ","))
// 	} else {
// 		sdclogger.SDCLoggerInstance.Println("All symbols processed.")
// 	}

// 	if notFoundLen, _ := cacheManager.GetLength(CACHE_KEY_SYMBOL_INVALID); notFoundLen > 0 {
// 		notFoundSymbols, _ := cacheManager.GetAllFromSet(CACHE_KEY_SYMBOL_ERROR)
// 		errorMessage += fmt.Sprintf("Not found symbols [%s]\n", strings.Join(notFoundSymbols, ","))
// 	} else {
// 		sdclogger.SDCLoggerInstance.Println("All symbols processed.")
// 	}

// 	if len(errorMessage) > 0 {
// 		return (allSymbols - errorSymbols), errors.New(errorMessage)
// 	}

// 	return (allSymbols - errorSymbols), nil
// }

func searchText(node *html.Node, text string) *html.Node {

	if node.Type == html.TextNode {
		regex, _ := regexp.Compile(".*" + text + ".*")
		if regex.Match([]byte(node.Data)) {
			return node
		}
	}

	for child := node.FirstChild; child != nil; child = child.NextSibling {
		if textNode := searchText(child, text); textNode != nil {
			return textNode
		}
	}

	return nil
}

// Entry function
func CollectFinancialsForSymbol(symbol string) error {
	// dbloader
	dbLoader := dbloader.NewPGLoader(config.SchemaName, sdclogger.SDCLoggerInstance.Logger)
	dbLoader.Connect(os.Getenv("PGHOST"),
		os.Getenv("PGPORT"),
		os.Getenv("PGUSER"),
		os.Getenv("PGPASSWORD"),
		os.Getenv("PGDATABASE"))
	defer dbLoader.Disconnect()

	// http reader
	httpReader := NewHttpReader(NewLocalClient())

	// Exporters
	var exporter MSDataExporters
	exporter.AddExporter(NewDBExporter(dbLoader, config.SchemaName))
	exporter.AddExporter(NewMSFileExporter())

	c := NewSACollector(httpReader, &exporter, dbLoader, sdclogger.SDCLoggerInstance.Logger)

	if err := c.CreateTables(); err != nil {
		sdclogger.SDCLoggerInstance.Printf("Failed to create tables. Error: %s", err)
		return err
	} else {
		sdclogger.SDCLoggerInstance.Printf("All tables created")
	}

	// If redirected
	redirected, err := c.MapRedirectedSymbol(symbol)
	if err != nil {
		e, ok := err.(HttpServerError)
		if ok && e.StatusCode() == HTTP_ERROR_NOT_FOUND {
			sdclogger.SDCLoggerInstance.Printf("Symbol %s not found", symbol)
		}
		return err
	}
	if len(redirected) > 0 {
		sdclogger.SDCLoggerInstance.Printf("Symbol %s is redirected to %s", symbol, redirected)

		symbol = redirected
	}

	if err := c.CollectFinancialDetails(symbol); err != nil {
		return err
	}
	fmt.Println("Collect financials for symbol " + symbol)

	return nil
}
