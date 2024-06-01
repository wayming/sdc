package collector

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/wayming/sdc/cache"
	"github.com/wayming/sdc/dbloader"
	"github.com/wayming/sdc/sdclogger"
)

type ParallelCollector interface {
	Execute(parallel int) (int64, error)
	collectorDo(col *SACollector, cache *cache.CacheManager, symbol string) error
}

type BaseParallelCollector struct {
}

func (c *BaseParallelCollector) collectorDo(col *SACollector, cache *cache.CacheManager, symbol string) error {
	sdclogger.SDCLoggerInstance.Panicln("collectorDo method of the BaseParallelCollector is not expectd to be used")
}

func (c *BaseParallelCollector) Execute(schemaName string, parallel int) (int64, error) {

	var allSymbols int64
	var errorSymbols int64
	var invalidSymbols int64
	var err error

	// shared by all go routines
	cm := cache.NewCacheManager()
	if err := cm.Connect(); err != nil {
		return 0, err
	}
	defer cm.Disconnect()

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
					logger.Printf("[Go%s]No symbol left", goID)
					break
				}

				err = c.collectorDo(col, cm, nextSymbol)
				if err != nil {
					logger.Printf("[Go%s] error: %s", goID, err.Error())
					outChan <- err.Error()
				}

				// // If redirected
				// redirected, err := collector.MapRedirectedSymbol(nextSymbol)
				// if err != nil {
				// 	logger.Printf("[Go%s] error: %s", goID, err.Error())
				// 	logger.Printf("[Go%s] Add %s to cache set %s", goID, nextSymbol, CACHE_KEY_SYMBOL_ERROR)

				// 	cacheKey := CACHE_KEY_SYMBOL_ERROR
				// 	e, ok := err.(HttpServerError)
				// 	if ok && e.StatusCode() == HTTP_ERROR_NOT_FOUND {
				// 		cacheKey := CACHE_KEY_SYMBOL_INVALID
				// 		logger.Printf("Add symbol %s to cache key %s", nextSymbol, cacheKey)
				// 	}

				// 	if cacheError := cm.AddToSet(cacheKey, nextSymbol); cacheError != nil {
				// 		logger.Printf("[Go%s] error: %s", goID, cacheError.Error())
				// 		outChan <- cacheError.Error()
				// 	}
				// }
				// if len(redirected) > 0 {
				// 	nextSymbol = redirected
				// }

				// if err := collector.CollectFinancialsForSymbol(nextSymbol); err != nil {
				// 	logger.Printf("[Go%s] error: %s", goID, err.Error())
				// 	logger.Printf("[Go%s] Add %s to cache set %s", goID, nextSymbol, CACHE_KEY_SYMBOL_ERROR)

				// 	cacheKey := CACHE_KEY_SYMBOL_ERROR
				// 	e, ok := err.(HttpServerError)
				// 	if ok && e.StatusCode() == HTTP_ERROR_NOT_FOUND {
				// 		cacheKey := CACHE_KEY_SYMBOL_INVALID
				// 		logger.Printf("Add symbol %s to cache key %s", nextSymbol, cacheKey)
				// 	}

				// 	if cacheError := cm.AddToSet(cacheKey, nextSymbol); cacheError != nil {
				// 		logger.Printf("[Go%s] error: %s", goID, cacheError.Error())
				// 		outChan <- cacheError.Error()
				// 	}

				// }
			}

			logger.Println("[Go" + goID + "] finished.")
		}(strconv.Itoa(i), schemaName, outChan, &wg)
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

	// Check error symbols
	if errorLen, _ := cm.GetLength(CACHE_KEY_SYMBOL_ERROR); errorLen > 0 {
		errorSymbols, _ := cm.GetAllFromSet(CACHE_KEY_SYMBOL_ERROR)
		errorMessage += fmt.Sprintf("Error symbols [%s]\n", strings.Join(errorSymbols, ","))
	} else {
		sdclogger.SDCLoggerInstance.Println("All symbols processed.")
	}

	// Check invalid symbols
	if invalidLen, _ := cm.GetLength(CACHE_KEY_SYMBOL_INVALID); invalidLen > 0 {
		invalidSymbols, _ := cm.GetAllFromSet(CACHE_KEY_SYMBOL_INVALID)
		errorMessage += fmt.Sprintf("Invalid symbols [%s]\n", strings.Join(invalidSymbols, ","))
	}

	if len(errorMessage) > 0 {
		return (allSymbols - errorSymbols - invalidSymbols), errors.New(errorMessage)
	} else {
		return (allSymbols - errorSymbols - invalidSymbols), nil
	}
}