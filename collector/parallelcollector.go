package collector

import (
	"errors"
	"fmt"
	"log"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"github.com/wayming/sdc/cache"
	"github.com/wayming/sdc/dbloader"
	"github.com/wayming/sdc/sdclogger"
)

type IParallelCollector interface {
	Execute(parallel int) (int64, error)
}

type ICollectWorker interface {
	collectorDo(col *SACollector, symbol string) error
	// collectorInit(schemaName string, proxyFile string, isContinue bool) error
}

type ParallelCollector struct {
	Worker ICollectWorker
	Schema string
}

func (pw *ParallelCollector) Execute(parallel int) (int64, error) {

	var allSymbols int64
	var errorSymbols int64
	var invalidSymbols int64

	// shared by all go routines
	cm := cache.NewCacheManager()
	if err := cm.Connect(); err != nil {
		return 0, err
	}
	defer cm.Disconnect()

	// Get total number of symbols to be processed
	if allSymbols, _ = cm.GetLength(CACHE_KEY_SYMBOL); allSymbols > 0 {
		sdclogger.SDCLoggerInstance.Printf("%d symbols to be processed in parallel(%d).", allSymbols, parallel)
	} else {
		sdclogger.SDCLoggerInstance.Println("No symbol found.")
		return 0, nil
	}

	var wg sync.WaitGroup
	outChan := make(chan string)
	for i := 0; i < parallel; i++ {
		wg.Add(1)
		go func(goID string, schemaName string, outChan chan string, wg *sync.WaitGroup) {
			defer wg.Done()

			// Logger
			file, _ := os.OpenFile(LOG_FILE+"."+goID, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
			logger := log.New(file, "sdc: ", log.Ldate|log.Ltime)
			defer file.Close()
			logger.Println("[Go" + goID + "] started.")

			// dbloader
			dbLoader := dbloader.NewPGLoader(schemaName, logger)
			dbLoader.Connect(os.Getenv("PGHOST"),
				os.Getenv("PGPORT"),
				os.Getenv("PGUSER"),
				os.Getenv("PGPASSWORD"),
				os.Getenv("PGDATABASE"))
			defer dbLoader.Disconnect()

			// http reader
			httpReader := NewHttpProxyReader(cm, CACHE_KEY_PROXY, goID)

			col := NewSACollector(dbLoader, httpReader, logger, schemaName)

			for remainingSymbols, _ := cm.GetLength(CACHE_KEY_SYMBOL); remainingSymbols > 0; {

				if remainingProxies, _ := cm.GetLength(CACHE_KEY_PROXY); remainingProxies == 0 {
					errorStr := fmt.Sprintf("[Go%s]No proxy server available", goID)
					logger.Println(errorStr)
					outChan <- errors.New(errorStr).Error()
					break
				}

				if remainingSymbols, _ := cm.GetLength(CACHE_KEY_SYMBOL); remainingSymbols == 0 {
					logger.Printf("[Go%s]No symbol left", goID)
					break
				}

				nextSymbol, err := cm.PopFromSet(CACHE_KEY_SYMBOL)
				if err != nil {
					logger.Printf("[Go%s] Failed to get symbol from cache. Error:%s", goID, err.Error())
					outChan <- err.Error()
					continue
				}
				if nextSymbol == "" {
					logger.Printf("[Go%s] No symbol left", goID)
					break
				}

				err = pw.Worker.collectorDo(col, nextSymbol)
				if err != nil {
					logger.Printf("[Go%s] error: %s", goID, err.Error())

					cacheKey := CACHE_KEY_SYMBOL_ERROR
					e, ok := err.(HttpServerError)
					if ok && e.StatusCode() == HTTP_ERROR_NOT_FOUND {
						cacheKey = CACHE_KEY_SYMBOL_INVALID
					}

					logger.Printf("[Go%s] Add %s to cache key %s", goID, nextSymbol, CACHE_KEY_SYMBOL_ERROR)
					if cacheError := cm.AddToSet(cacheKey, nextSymbol); cacheError != nil {
						logger.Printf("[Go%s] error: %s", goID, cacheError.Error())
						outChan <- cacheError.Error()
					}

					outChan <- err.Error()
				}
			}

			logger.Println("[Go" + goID + "] finished.")
		}(strconv.Itoa(i), pw.Schema, outChan, &wg)
	}

	go func() {
		wg.Wait()
		close(outChan)
	}()

	// Errors from all go routines
	var errorMessage string
	for out := range outChan {
		sdclogger.SDCLoggerInstance.Printf(out)
		errorMessage += fmt.Sprintf("%s\n", out)
	}

	// Check proxies
	if remainingProxies, _ := cm.GetLength(CACHE_KEY_PROXY); remainingProxies == 0 {
		errorMessage += "Running out of proxy servers.\n"
	}

	// Check left symbols
	if errorLen, _ := cm.GetLength(CACHE_KEY_SYMBOL); errorLen > 0 {
		errorSymbols, _ := cm.GetAllFromSet(CACHE_KEY_SYMBOL)
		errorMessage += fmt.Sprintf("Left symbols [%s]\n", strings.Join(errorSymbols, ","))
	} else {
		sdclogger.SDCLoggerInstance.Println("No left symbol.")
	}

	// Check error symbols
	if errorLen, _ := cm.GetLength(CACHE_KEY_SYMBOL_ERROR); errorLen > 0 {
		errorSymbols, _ := cm.GetAllFromSet(CACHE_KEY_SYMBOL_ERROR)
		errorMessage += fmt.Sprintf("Error symbols [%s]\n", strings.Join(errorSymbols, ","))
	} else {
		sdclogger.SDCLoggerInstance.Println("No error symbol.")
	}

	// Check invalid symbols
	if invalidLen, _ := cm.GetLength(CACHE_KEY_SYMBOL_INVALID); invalidLen > 0 {
		invalidSymbols, _ := cm.GetAllFromSet(CACHE_KEY_SYMBOL_INVALID)
		errorMessage += fmt.Sprintf("Invalid symbols [%s]\n", strings.Join(invalidSymbols, ","))
	} else {
		sdclogger.SDCLoggerInstance.Println("No invalid symbol.")
	}

	if len(errorMessage) > 0 {
		return (allSymbols - errorSymbols - invalidSymbols), errors.New(errorMessage)
	} else {
		return (allSymbols - errorSymbols - invalidSymbols), nil
	}
}

type RedirectedWorker struct {
}

type FinancialOverviewWorker struct {
}

type FinancialDetailsWorker struct {
}

func (c *RedirectedWorker) collectorInit(schemaName string, proxyFile string, isContinue bool) error {
	cacheManager := cache.NewCacheManager()
	if err := cacheManager.Connect(); err != nil {
		return err
	}
	defer cacheManager.Disconnect()

	if isContinue {
		if err := cacheManager.MoveSet(CACHE_KEY_SYMBOL_ERROR, CACHE_KEY_SYMBOL); err != nil {
			return fmt.Errorf("failed to restore the error symbols. Error: %s", err.Error())
		}
	} else {
		if err := ClearCache(); err != nil {
			return err
		}
		allSymbols, err := cache.LoadSymbols(cacheManager, CACHE_KEY_SYMBOL, schemaName)
		if err != nil {
			return errors.New("Failed to load symbols to cache. Error: " + err.Error())
		}
		sdclogger.SDCLoggerInstance.Printf("Loaded %d symbols to cache", allSymbols)

		allProxies, err := cache.LoadProxies(cacheManager, CACHE_KEY_PROXY, proxyFile)
		if err != nil {
			return errors.New("Failed to load proxies to cache. Error: " + err.Error())
		}
		sdclogger.SDCLoggerInstance.Printf("Loaded %d proxies to cache", allProxies)
	}

	// Create tables
	dbLoader := dbloader.NewPGLoader(schemaName, &sdclogger.SDCLoggerInstance.Logger)
	dbLoader.Connect(os.Getenv("PGHOST"),
		os.Getenv("PGPORT"),
		os.Getenv("PGUSER"),
		os.Getenv("PGPASSWORD"),
		os.Getenv("PGDATABASE"))
	defer dbLoader.Disconnect()
	collector := NewSACollector(dbLoader, nil, &sdclogger.SDCLoggerInstance.Logger, schemaName)
	if err := collector.CreateTables(); err != nil {
		sdclogger.SDCLoggerInstance.Printf("Failed to create tables. Error: %s", err)
		return err
	} else {
		sdclogger.SDCLoggerInstance.Printf("All tables created")
		return nil
	}
}

func (c *RedirectedWorker) collectorDo(col *SACollector, symbol string) error {
	if rsymbol, err := col.MapRedirectedSymbol(symbol); err != nil {
		return err
	} else if len(rsymbol) > 0 {
		symbol = rsymbol
	}

	cm := cache.NewCacheManager()
	if err := cm.Connect(); err != nil {
		return err
	}
	defer cm.Disconnect()
	cm.AddToSet(CACHE_KEY_SYMBOL_REDIRECTED, symbol)
	return nil
}

func (c *FinancialOverviewWorker) collectorInit(isContinue bool) error {
	cacheManager := cache.NewCacheManager()
	if err := cacheManager.Connect(); err != nil {
		return err
	}
	defer cacheManager.Disconnect()

	if isContinue {
		if err := cacheManager.MoveSet(CACHE_KEY_SYMBOL_ERROR, CACHE_KEY_SYMBOL); err != nil {
			return fmt.Errorf("failed to restore the error symbols. Error: %s", err.Error())
		}
	} else {
		if err := cacheManager.CopySet(CACHE_KEY_SYMBOL_REDIRECTED, CACHE_KEY_SYMBOL); err != nil {
			return fmt.Errorf("failed to restore the redirected symbols. Error: %s", err.Error())
		}
		allSymbols, _ := cacheManager.GetLength(CACHE_KEY_SYMBOL)
		sdclogger.SDCLoggerInstance.Printf("%d symbols to process", allSymbols)
	}
	return nil
}
func (c *FinancialOverviewWorker) collectorDo(col *SACollector, symbol string) error {
	if _, err := col.CollectOverallMetrics(symbol, reflect.TypeFor[StockOverview]()); err != nil {
		return err
	} else {
		return nil
	}
}

func (c *FinancialDetailsWorker) collectorInit(isContinue bool) error {
	cacheManager := cache.NewCacheManager()
	if err := cacheManager.Connect(); err != nil {
		return err
	}
	defer cacheManager.Disconnect()

	if isContinue {
		if err := cacheManager.MoveSet(CACHE_KEY_SYMBOL_ERROR, CACHE_KEY_SYMBOL); err != nil {
			return fmt.Errorf("failed to restore the error symbols. Error: %s", err.Error())
		}
	} else {
		if err := cacheManager.CopySet(CACHE_KEY_SYMBOL_REDIRECTED, CACHE_KEY_SYMBOL); err != nil {
			return fmt.Errorf("failed to restore the redirected symbols. Error: %s", err.Error())
		}
		allSymbols, _ := cacheManager.GetLength(CACHE_KEY_SYMBOL)
		sdclogger.SDCLoggerInstance.Printf("%d symbols to process", allSymbols)
	}
	return nil
}
func (c *FinancialDetailsWorker) collectorDo(col *SACollector, symbol string) error {
	var retErr error

	if _, err := col.CollectFinancialsIncome(symbol, reflect.TypeFor[FinancialsIncome]()); err != nil {
		retErr = err
	}
	if _, err := col.CollectFinancialsBalanceSheet(symbol, reflect.TypeFor[FinancialsBalanceShet]()); err != nil {
		retErr = err
	}
	if _, err := col.CollectFinancialsCashFlow(symbol, reflect.TypeFor[FinancialsCashFlow]()); err != nil {
		retErr = err
	}
	if _, err := col.CollectFinancialsRatios(symbol, reflect.TypeFor[FinancialRatios]()); err != nil {
		retErr = err
	}
	if _, err := col.CollectAnalystRatings(symbol, reflect.TypeFor[AnalystsRating]()); err != nil {
		retErr = err
	}

	return retErr
}

func NewRedirectedParallelCollector(schemaName string, proxyFile string, isContinue bool) IParallelCollector {
	worker := RedirectedWorker{}
	worker.collectorInit(schemaName, proxyFile, isContinue)
	return &ParallelCollector{Worker: &worker, Schema: schemaName}
}

func NewFinancialOverviewParallelCollector(schemaName string, isContinue bool) IParallelCollector {
	worker := FinancialOverviewWorker{}
	worker.collectorInit(isContinue)
	return &ParallelCollector{Worker: &worker, Schema: schemaName}
}

func NewFinancialDetailsParallelCollector(schemaName string, isContinue bool) IParallelCollector {
	worker := FinancialDetailsWorker{}
	worker.collectorInit(isContinue)
	return &ParallelCollector{Worker: &worker, Schema: schemaName}
}
