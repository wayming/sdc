package collector

import (
	"encoding/json"
	"errors"
	"log"
	"reflect"
	"regexp"
	"strings"

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
		logger = &sdclogger.SDCLoggerInstance.Logger
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
		TABLE_sa_redirected_symbols:       reflect.TypeFor[RedirectedSymbols](),
		TABLE_SA_OVERVIEW:                 reflect.TypeFor[StockOverview](),
		TABLE_SA_FINANCIALS_INCOME:        reflect.TypeFor[FinancialsIncome](),
		TABLE_SA_FINANCIALS_BALANCE_SHEET: reflect.TypeFor[FinancialsBalanceSheet](),
		TABLE_SA_FINANCIALS_CASH_FLOW:     reflect.TypeFor[FinancialsCashFlow](),
		TABLE_SA_FINANCIALS_RATIOS:        reflect.TypeFor[FinancialRatios](),
		TABLE_SA_ANALYST_RATINGS:          reflect.TypeFor[AnalystsRating](),
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
	symbol = strings.ToLower(symbol)
	redirected := c.parseRedirectedSymbol(symbol)
	if len(redirected) == 0 {
		c.logger.Printf("no redirected found for symbol %s", symbol)
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

	numOfRows, err := c.loader.LoadByJsonText(string(jsonText), TABLE_sa_redirected_symbols, reflect.TypeFor[RedirectedSymbols]())
	if err != nil {
		return "", errors.New("Failed to load data into table " + TABLE_sa_redirected_symbols + ". Error: " + err.Error())
	}

	c.logger.Println(numOfRows, "rows have been loaded into", TABLE_sa_redirected_symbols)
	return redirected, nil
}

// Extract and write financial overview to database.
func (c *SACollector) CollectFinancialOverview(symbol string, dataStructType reflect.Type) (int64, error) {
	c.thisSymbol = symbol
	overallUrl := "https://stockanalysis.com/stocks/" + symbol
	jsonText, err := c.readOverviewPage(overallUrl, nil, dataStructType.Name())
	if err != nil {
		return 0, err
	}

	numOfRows, err := c.loader.LoadByJsonText(jsonText, TABLE_SA_OVERVIEW, reflect.TypeFor[StockOverview]())
	if err != nil {
		return 0, errors.New("Failed to load data into table " + TABLE_SA_OVERVIEW + ". Error: " + err.Error())
	}

	c.logger.Println(numOfRows, "rows have been loaded into", TABLE_SA_OVERVIEW)
	return numOfRows, nil
}

// Extract and write financial details to database. Only return the last error
func (c *SACollector) CollectFinancialDetails(symbol string) error {
	var retErr error
	if _, err := c.CollectFinancialsIncome(symbol, reflect.TypeFor[FinancialsIncome]()); err != nil {
		retErr = err
	}
	if _, err := c.CollectFinancialsBalanceSheet(symbol, reflect.TypeFor[FinancialsBalanceSheet]()); err != nil {
		retErr = err
	}
	if _, err := c.CollectFinancialsCashFlow(symbol, reflect.TypeFor[FinancialsCashFlow]()); err != nil {
		retErr = err
	}
	if _, err := c.CollectFinancialsRatios(symbol, reflect.TypeFor[FinancialRatios]()); err != nil {
		retErr = err
	}
	if _, err := c.CollectAnalystRatings(symbol, reflect.TypeFor[AnalystsRating]()); err != nil {
		retErr = err
	}
	return retErr
}

func (c *SACollector) CollectFinancialsIncome(symbol string, dataStructType reflect.Type) (int64, error) {
	c.thisSymbol = symbol
	financialsIncome := "https://stockanalysis.com/stocks/" + symbol + "/financials/?p=quarterly"
	return c.collectFinancialDetailsCommon(financialsIncome, dataStructType, TABLE_SA_FINANCIALS_INCOME)
}

func (c *SACollector) CollectFinancialsBalanceSheet(symbol string, dataStructType reflect.Type) (int64, error) {
	c.thisSymbol = symbol
	financialsBalanceSheet := "https://stockanalysis.com/stocks/" + symbol + "/financials/balance-sheet/?p=quarterly"
	return c.collectFinancialDetailsCommon(financialsBalanceSheet, dataStructType, TABLE_SA_FINANCIALS_BALANCE_SHEET)
}

func (c *SACollector) CollectFinancialsCashFlow(symbol string, dataStructType reflect.Type) (int64, error) {
	c.thisSymbol = symbol
	financialsICashFlow := "https://stockanalysis.com/stocks/" + symbol + "/financials/cash-flow-statement/?p=quarterly"
	return c.collectFinancialDetailsCommon(financialsICashFlow, dataStructType, TABLE_SA_FINANCIALS_CASH_FLOW)
}

func (c *SACollector) CollectFinancialsRatios(symbol string, dataStructType reflect.Type) (int64, error) {
	c.thisSymbol = symbol
	financialsRatios := "https://stockanalysis.com/stocks/" + symbol + "/financials/ratios/?p=quarterly"
	return c.collectFinancialDetailsCommon(financialsRatios, dataStructType, TABLE_SA_FINANCIALS_RATIOS)
}

func (c *SACollector) CollectAnalystRatings(symbol string, dataStructType reflect.Type) (int64, error) {
	c.thisSymbol = symbol
	url := "https://stockanalysis.com/stocks/" + symbol + "/ratings"

	jsonText, err := c.readAnalystRatingsPage(url, nil, dataStructType.Name())
	if err != nil {
		return 0, err
	}

	numOfRows, err := c.loader.LoadByJsonText(jsonText, TABLE_SA_ANALYST_RATINGS, dataStructType)
	if err != nil {
		return 0, errors.New("Failed to load data into table " + TABLE_SA_ANALYST_RATINGS + ". Error: " + err.Error())
	}

	c.logger.Println(numOfRows, "rows have been loaded into", TABLE_SA_ANALYST_RATINGS)
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
func (c *SACollector) readAnalystRatingsPage(url string, params map[string]string, dataStructTypeName string) (string, error) {
	c.logger.Println("Read " + url)

	htmlContent, err := c.reader.Read(url, params)
	if err != nil {
		return "", err
	}

	htmlDoc, err := html.Parse(strings.NewReader(htmlContent))
	if err != nil {
		return "", errors.New("Failed to parse the html page " + url + ". Error: " + err.Error())
	}

	c.logger.Println("Decode html doc with JSON struct " + dataStructTypeName)
	indicatorsMap, err := c.htmlParser.DecodeAnalystRatingsGrid(htmlDoc, dataStructTypeName)

	if err != nil {
		return "", errors.New("Failed to parse " + url + ". Error: " + err.Error())
	}
	if len(indicatorsMap) == 0 {
		return "", errors.New("No indicator found from analyst ratings page " + url)
	}

	// Add symbol to the struct if needed
	c.packSymbolField(indicatorsMap, dataStructTypeName)

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
func (c *SACollector) readOverviewPage(url string, params map[string]string, dataStructTypeName string) (string, error) {
	c.logger.Println("Read " + url)

	htmlContent, err := c.reader.Read(url, params)
	if err != nil {
		return "", err
	}

	htmlDoc, err := html.Parse(strings.NewReader(htmlContent))
	if err != nil {
		return "", errors.New("Failed to parse the html page " + url + ". Error: " + err.Error())
	}

	c.logger.Println("Decode html doc with JSON struct " + dataStructTypeName)
	indicatorsMap, err := c.htmlParser.DecodeOverviewPages(htmlDoc, dataStructTypeName)
	if err != nil {
		return "", errors.New("Failed to parse " + url + ". Error: " + err.Error())
	}
	if len(indicatorsMap) == 0 {
		return "", errors.New("No indicator found from overall page " + url)
	}

	// Add symbol to the struct if needed
	c.packSymbolField(indicatorsMap, dataStructTypeName)

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

func (c *SACollector) parseRedirectedSymbol(symbol string) string {
	symbol = strings.ToLower(symbol)
	url := "https://stockanalysis.com/stocks/" + symbol + "/financials/?p=quarterly"
	redirectedURL, _ := c.reader.RedirectedUrl(url)
	if len(redirectedURL) == 0 {
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
	}
	return ""
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
// 	dbLoader := dbloader.NewPGLoader(schemaName, &sdclogger.SDCLoggerInstance.Logger)
// 	dbLoader.Connect(os.Getenv("PGHOST"),
// 		os.Getenv("PGPORT"),
// 		os.Getenv("PGUSER"),
// 		os.Getenv("PGPASSWORD"),
// 		os.Getenv("PGDATABASE"))
// 	defer dbLoader.Disconnect()
// 	collector := NewSACollector(dbLoader, nil, &sdclogger.SDCLoggerInstance.Logger, schemaName)
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

// func CollectFinancialDetails(schemaName string, symbol string) error {
// 	// dbloader
// 	dbLoader := dbloader.NewPGLoader(schemaName, &sdclogger.SDCLoggerInstance.Logger)
// 	dbLoader.Connect(os.Getenv("PGHOST"),
// 		os.Getenv("PGPORT"),
// 		os.Getenv("PGUSER"),
// 		os.Getenv("PGPASSWORD"),
// 		os.Getenv("PGDATABASE"))
// 	defer dbLoader.Disconnect()

// 	// http reader
// 	httpReader := NewHttpReader(NewLocalClient())

// 	collector := NewSACollector(dbLoader, httpReader, &sdclogger.SDCLoggerInstance.Logger, schemaName)

// 	if err := c.CreateTables(); err != nil {
// 		sdclogger.SDCLoggerInstance.Printf("Failed to create tables. Error: %s", err)
// 		return err
// 	} else {
// 		sdclogger.SDCLoggerInstance.Printf("All tables created")
// 	}

// 	// If redirected
// 	redirected, err := c.MapRedirectedSymbol(symbol)
// 	if err != nil {
// 		e, ok := err.(HttpServerError)
// 		if ok && e.StatusCode() == HTTP_ERROR_NOT_FOUND {
// 			sdclogger.SDCLoggerInstance.Printf("Symbol %s not found", symbol)
// 		}
// 		return err
// 	}
// 	if len(redirected) > 0 {
// 		sdclogger.SDCLoggerInstance.Printf("Symbol %s is redirected to %s", symbol, redirected)

// 		symbol = redirected
// 	}

// 	if err := c.CollectFinancialDetails(symbol); err != nil {
// 		return err
// 	}
// 	fmt.Println("Collect financials for symbol " + symbol)

// 	return nil
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
