package collector

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"

	"github.com/wayming/sdc/cache"
	"github.com/wayming/sdc/config"
	"github.com/wayming/sdc/dbloader"
	"github.com/wayming/sdc/sdclogger"
)

type ParallelCollector struct {
	NewBuilderFunc func() IWorkerBuilder
	Cache          cache.ICacheManager
	Params         PCParams
}

const (
	SUCCESS = iota
	WORKER_INIT_FAILURE
	WORKER_DONE_FAILURE
	WORKER_PROCESS_FAILURE
	SERVER_SYMBOL_NOT_VALID
)

type PCResponse struct {
	Symbol    string
	ErrorID   int
	ErrorText string
}

type PCParams struct {
	IsContinue  bool
	TickersJSON string
	ProxyFile   string
}

func (pc *ParallelCollector) workerRoutine(
	goID string,
	inChan chan string,
	outChan chan PCResponse,
	proxyChan chan string,
	wg *sync.WaitGroup,
) {

	defer wg.Done()

	// Logger
	file, _ := os.OpenFile(LOG_FILE+"."+goID, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	logger := log.New(file, "sdc: ", log.Ldate|log.Ltime)
	defer file.Close()

	logMessage := func(text string) {
		logger.Println("[Go" + goID + "] " + text)
	}

	logMessage("Begin")

	builder := pc.NewBuilderFunc()
	loop := 1
	for loop > 0 {
		// Get proxy if there is on the channel
		proxy, ok := <-proxyChan
		if ok {
			client, err := NewProxyClient(proxy)
			if err != nil {
				logMessage(err.Error())
				continue // Retry another proxy
			}
			builder.WithReader(NewHttpReader(client))
			logMessage("Established proxy reader with proxy url " + proxy)
		} else {
			builder.WithReader(NewHttpReader(NewLocalClient()))
			logMessage("Established native reader")
			loop = 0
		}

		// Build worker
		builder.WithLogger(logger)
		builder.Default()
		worker := builder.Build()

		if err := worker.Init(); err != nil {
			logMessage(err.Error())
			outChan <- PCResponse{
				"", WORKER_INIT_FAILURE, err.Error(),
			}
			return
		}

		for symbol := range inChan {
			if err := worker.Do(symbol); err != nil {
				logMessage(err.Error())

				e, ok := err.(HttpServerError)
				if ok && e.StatusCode() == HTTP_ERROR_NOT_FOUND {
					// Symbol does not exist
					outChan <- PCResponse{
						symbol, SERVER_SYMBOL_NOT_VALID, e.Error(),
					}
					continue
				}
				outChan <- PCResponse{
					symbol, WORKER_PROCESS_FAILURE, err.Error(),
				}
			} else {
				outChan <- PCResponse{
					symbol, SUCCESS, "",
				}
			}
		}

		if err := worker.Done(); err != nil {
			outChan <- PCResponse{
				"", WORKER_DONE_FAILURE, err.Error(),
			}
		}

		// Complete
		break
	}

	logMessage("Finish")
}
func (pc *ParallelCollector) Execute(parallel int) error {

	var nAll int64
	var nLeft int64
	var summary string

	builder := pc.NewBuilderFunc()
	builder.WithParams(&pc.Params)
	builder.Default()
	if err := builder.Prepare(); err != nil {
		return err
	}

	// Attach to cache
	if err := pc.Cache.Connect(); err != nil {
		return err
	}
	defer pc.Cache.Disconnect()

	// Get total number of symbols to be processed
	if nAll, _ = pc.Cache.GetLength(CACHE_KEY_SYMBOL); nAll > 0 {
		sdclogger.SDCLoggerInstance.Printf("%d symbols to be processed in parallel(%d).", nAll, parallel)
		summary += fmt.Sprint("Total: %ld", nAll)
	} else {
		sdclogger.SDCLoggerInstance.Println("No symbol found.")
		return nil
	}

	var wg sync.WaitGroup
	inChan := make(chan string, 1000*1000)
	outChan := make(chan PCResponse, 1000*1000)
	proxyChan := make(chan string, 1000)

	// Start goroutine
	i := 0
	for ; i < parallel; i++ {
		wg.Add(1)
		go pc.workerRoutine(strconv.Itoa(i), inChan, outChan, proxyChan, &wg)
	}

	// Push proxies to channel
	numProxies, _ := pc.Cache.GetLength(CACHE_KEY_PROXY)
	if numProxies > 0 {
		for {
			proxy, err := pc.Cache.PopFromSet(CACHE_KEY_PROXY)
			if err != nil {
				return err
			}
			if proxy == "" {
				sdclogger.SDCLoggerInstance.Println("All proxies are pushed into [proxy] channel.")
				break
			}
			sdclogger.SDCLoggerInstance.Printf("Push %s into [proxy] channel.", proxy)
			proxyChan <- proxy
		}
	}
	close(proxyChan)

	// Push symbols to channel
	go func() {
		defer close(inChan) // Close the inChan when done
		for {
			symbol, err := pc.Cache.PopFromSet(CACHE_KEY_SYMBOL)

			if err != nil {
				break // Exit on error
			}
			if len(symbol) == 0 {
				sdclogger.SDCLoggerInstance.Println("All symbols are pushed into [input] channel.")
				break
			}
			sdclogger.SDCLoggerInstance.Printf("Push %s into [input] channel.", symbol)
			inChan <- symbol
		}
	}()

	// Cleanup
	go func() {
		wg.Wait()
		close(outChan)
	}()

	// Handle PCResponse
	processed := 0
	succeeded := 0
	for resp := range outChan {
		processed++
		if resp.ErrorID != SUCCESS {
			sdclogger.SDCLoggerInstance.Printf("Failed to process symbol %s. Error %s", resp.Symbol, resp.ErrorText)
			if resp.ErrorID == SERVER_SYMBOL_NOT_VALID {
				pc.Cache.AddToSet(CACHE_KEY_SYMBOL_INVALID, resp.Symbol)
			} else {
				pc.Cache.AddToSet(CACHE_KEY_SYMBOL_ERROR, resp.Symbol)
			}
		} else {
			succeeded++
		}
		fmt.Printf("Processed %d, succeeded %d\n", processed, succeeded)
	}

	// Check left symbols
	if nLeft, _ := pc.Cache.GetLength(CACHE_KEY_SYMBOL); nLeft > 0 {
		lefts, _ := pc.Cache.GetAllFromSet(CACHE_KEY_SYMBOL)
		sdclogger.SDCLoggerInstance.Printf("Left symbols: [%v]", lefts)
		summary += fmt.Sprintf("Left: [%v]", lefts)
	} else {
		sdclogger.SDCLoggerInstance.Println("No left symbol.")
	}

	// Check error symbols. Symbols are valid, but fails to process.
	// These symbols can be retried.
	if errorLen, _ := pc.Cache.GetLength(CACHE_KEY_SYMBOL_ERROR); errorLen > 0 {
		errs, _ := pc.Cache.GetAllFromSet(CACHE_KEY_SYMBOL_ERROR)
		sdclogger.SDCLoggerInstance.Printf("Error Symbols: [%v]", errs)
		summary += fmt.Sprintf("Error: [%v]", errs)
	} else {
		sdclogger.SDCLoggerInstance.Println("No error symbol.")
	}

	// Check invalid symbols.
	// These symbols does not exist and should not be retired.
	if invalidLen, _ := pc.Cache.GetLength(CACHE_KEY_SYMBOL_INVALID); invalidLen > 0 {
		invalids, _ := pc.Cache.GetAllFromSet(CACHE_KEY_SYMBOL_INVALID)
		sdclogger.SDCLoggerInstance.Printf("Invalid Symbols: [%v]", invalids)
		summary += fmt.Sprintf("Invalid: [%v]", invalids)
	} else {
		sdclogger.SDCLoggerInstance.Println("No invalid symbol.")
	}

	if nLeft > 0 {
		return errors.New(summary)
	} else {
		return nil
	}
}
func (pc *ParallelCollector) Done() {

}

type RedirectSymbolWorker struct {
	collector  *SACollector
	isContinue bool
}

type FinancialOverviewWorker struct {
	collector  *SACollector
	isContinue bool
}

type FinancialDetailsWorker struct {
	collector  *SACollector
	isContinue bool
}

func (w *RedirectSymbolWorker) Init(cm cache.ICacheManager, logger *log.Logger) error {
	if w.isContinue {
		if err := cm.MoveSet(CACHE_KEY_SYMBOL_ERROR, CACHE_KEY_SYMBOL); err != nil {
			return fmt.Errorf("failed to restore the error symbols. Error: %s", err.Error())
		}
	} else {
		if err := ClearCache(); err != nil {
			return err
		}
		allSymbols, err := cache.LoadSymbols(cm, CACHE_KEY_SYMBOL, config.SchemaName)
		if err != nil {
			return errors.New("Failed to load symbols to cache. Error: " + err.Error())
		}
		sdclogger.SDCLoggerInstance.Printf("Loaded %d symbols to cache", allSymbols)

		allProxies, err := cache.LoadProxies(cm, CACHE_KEY_PROXY, config.ProxyFile)
		if err != nil {
			return errors.New("Failed to load proxies to cache. Error: " + err.Error())
		}
		sdclogger.SDCLoggerInstance.Printf("Loaded %d proxies to cache", allProxies)
	}

	// Create tables
	dbLoader := dbloader.NewPGLoader(config.SchemaName, logger)
	dbLoader.Connect(os.Getenv("PGHOST"),
		os.Getenv("PGPORT"),
		os.Getenv("PGUSER"),
		os.Getenv("PGPASSWORD"),
		os.Getenv("PGDATABASE"))
	// TODO
	// w.collector = NewSACollector(dbLoader, nil, logger, config.SchemaName)
	// if err := w.collector.CreateTables(); err != nil {
	// 	sdclogger.SDCLoggerInstance.Printf("Failed to create tables. Error: %s", err)
	// 	return err
	// } else {
	// 	sdclogger.SDCLoggerInstance.Printf("All tables created")
	// 	return nil
	// }
	return nil
}
func (w *RedirectSymbolWorker) Do(symbol string, cm cache.ICacheManager) error {
	if rsymbol, err := w.collector.MapRedirectedSymbol(symbol); err != nil {
		return err
	} else if len(rsymbol) > 0 {
		symbol = rsymbol
	}

	cm.AddToSet(CACHE_KEY_SYMBOL_REDIRECTED, symbol)
	return nil
}
func (w *RedirectSymbolWorker) Done() error {
	return nil
}

func (w *FinancialOverviewWorker) Init(cm cache.ICacheManager, logger *log.Logger) error {
	if w.isContinue {
		if err := cm.MoveSet(CACHE_KEY_SYMBOL_ERROR, CACHE_KEY_SYMBOL); err != nil {
			return fmt.Errorf("failed to restore the error symbols. Error: %s", err.Error())
		}
	} else {
		if err := cm.CopySet(CACHE_KEY_SYMBOL_REDIRECTED, CACHE_KEY_SYMBOL); err != nil {
			return fmt.Errorf("failed to restore the redirected symbols. Error: %s", err.Error())
		}
		allSymbols, _ := cm.GetLength(CACHE_KEY_SYMBOL)
		logger.Printf("%d symbols to process", allSymbols)
	}

	// Create tables
	dbLoader := dbloader.NewPGLoader(config.SchemaName, logger)
	dbLoader.Connect(os.Getenv("PGHOST"),
		os.Getenv("PGPORT"),
		os.Getenv("PGUSER"),
		os.Getenv("PGPASSWORD"),
		os.Getenv("PGDATABASE"))
	// TODO
	// w.collector = NewSACollector(dbLoader, nil, logger, config.SchemaName)
	// if err := w.collector.CreateTables(); err != nil {
	// 	sdclogger.SDCLoggerInstance.Printf("Failed to create tables. Error: %s", err)
	// 	return err
	// } else {
	// 	sdclogger.SDCLoggerInstance.Printf("All tables created")
	// 	return nil
	// }
	return nil
}
func (w *FinancialOverviewWorker) Do(symbol string, cm cache.ICacheManager) error {
	// if _, err := w.collector.CollectFinancialOverall(symbol, reflect.TypeFor[StockOverview]()); err != nil {
	// 	return err
	// } else {
	// 	return nil
	// }
	return nil
}
func (w *FinancialOverviewWorker) Done() error {
	return nil
}

func (w *FinancialDetailsWorker) Init(cm cache.ICacheManager, logger *log.Logger) error {
	if w.isContinue {
		if err := cm.MoveSet(CACHE_KEY_SYMBOL_ERROR, CACHE_KEY_SYMBOL); err != nil {
			return fmt.Errorf("failed to restore the error symbols. Error: %s", err.Error())
		}
	} else {
		if err := cm.CopySet(CACHE_KEY_SYMBOL_REDIRECTED, CACHE_KEY_SYMBOL); err != nil {
			return fmt.Errorf("failed to restore the redirected symbols. Error: %s", err.Error())
		}
		allSymbols, _ := cm.GetLength(CACHE_KEY_SYMBOL)
		logger.Printf("%d symbols to process", allSymbols)
	}
	return nil
}
func (w *FinancialDetailsWorker) Do(symbol string, cm cache.ICacheManager) error {
	var retErr error

	if _, err := w.collector.CollectFinancialsIncome(symbol); err != nil {
		retErr = err
	}
	if _, err := w.collector.CollectFinancialsBalanceSheet(symbol); err != nil {
		retErr = err
	}
	if _, err := w.collector.CollectFinancialsCashFlow(symbol); err != nil {
		retErr = err
	}
	if _, err := w.collector.CollectFinancialsRatios(symbol); err != nil {
		retErr = err
	}
	if _, err := w.collector.CollectAnalystRatings(symbol); err != nil {
		retErr = err
	}

	return retErr
}
func (w *FinancialDetailsWorker) Done() error {
	return nil
}

func NewEODParallelCollector(p PCParams) ParallelCollector {
	return ParallelCollector{
		NewYFWorkerBuilder,
		cache.NewCacheManager(),
		p,
	}
}

func NewFinancialParallelCollector(p PCParams) ParallelCollector {
	return ParallelCollector{
		NewSAWorkerBuilder,
		cache.NewCacheManager(),
		p,
	}
}
