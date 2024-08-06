package collector

import (
	"errors"
	"fmt"
	"log"
	"os"
	"reflect"
	"strconv"
	"sync"

	"github.com/wayming/sdc/cache"
	"github.com/wayming/sdc/config"
	"github.com/wayming/sdc/dbloader"
	"github.com/wayming/sdc/sdclogger"
)

type IWorker interface {
	Init(cm cache.ICacheManager, logger *log.Logger) error
	Do(symbol string, cm cache.ICacheManager) error
	Done() error
}

type ParallelCollector struct {
	Worker IWorker
}

const (
	SUCCESS = iota
	WORKER_INIT_FAILURE
	WORKER_DONE_FAILURE
	WORKER_PROCESS_FAILURE
	SERVER_SYMBOL_NOT_VALID
)

type Response struct {
	Symbol    string
	ErrorID   int
	ErrorText string
}

func (pw *ParallelCollector) worker(goID string, inChan chan string, outChan chan Response, wg *sync.WaitGroup, cm cache.ICacheManager) {

	defer wg.Done()

	// Logger
	file, _ := os.OpenFile(LOG_FILE+"."+goID, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	logger := log.New(file, "sdc: ", log.Ldate|log.Ltime)
	defer file.Close()

	logMessage := func(text string) {
		logger.Println("[Go" + goID + "] " + text)
	}

	logMessage("Begin")

	if err := pw.Worker.Init(cm, logger); err != nil {
		logMessage(err.Error())
		outChan <- Response{
			"", WORKER_INIT_FAILURE, err.Error(),
		}
		return
	}

	for symbol := range inChan {
		if err := pw.Worker.Do(symbol, cm); err != nil {
			logMessage(err.Error())

			e, ok := err.(HttpServerError)
			if ok && e.StatusCode() == HTTP_ERROR_NOT_FOUND {
				outChan <- Response{
					symbol, SERVER_SYMBOL_NOT_VALID, e.Error(),
				}
				continue
			}
			outChan <- Response{
				symbol, WORKER_PROCESS_FAILURE, err.Error(),
			}
		} else {
			outChan <- Response{
				symbol, SUCCESS, "",
			}
		}
	}

	if err := pw.Worker.Done(); err != nil {
		outChan <- Response{
			"", WORKER_DONE_FAILURE, err.Error()
		}
	}
	logMessage("Finish")
}

func (pw *ParallelCollector) Execute(parallel int) (int64, error) {

	var all int64
	var errs int64
	var invalids int64

	// shared by all go routines
	cm := cache.NewCacheManager()
	if err := cm.Connect(); err != nil {
		return 0, err
	}
	defer cm.Disconnect()

	// Get total number of symbols to be processed
	if all, _ = cm.GetLength(CACHE_KEY_SYMBOL); all > 0 {
		sdclogger.SDCLoggerInstance.Printf("%d symbols to be processed in parallel(%d).", all, parallel)
	} else {
		sdclogger.SDCLoggerInstance.Println("No symbol found.")
		return 0, nil
	}

	var wg sync.WaitGroup
	inChan := make(chan string)
	outChan := make(chan Response)

	i := 0
	for ; i < parallel; i++ {
		wg.Add(1)
		pw.worker(strconv.Itoa(i), inChan, outChan, &wg, cm)
	}

	// Cleanup
	go func() {
		wg.Wait()
		close(inChan)
		close(outChan)
	}()

	// Push symbols to channel
	for symbol, err := cm.GetFromSet(CACHE_KEY_SYMBOL); err != nil; {
		inChan <- symbol
	}

	// Handle response
	for resp := range outChan {
		if resp.ErrorID == WORKER_INIT_FAILURE {
			wg.Add(1)
			pw.worker(strconv.Itoa(i), inChan, outChan, &wg, cm)
			i++
		}

		if resp.ErrorID != SUCCESS {
			sdclogger.SDCLoggerInstance.Printf("Failed to process symbol %s. Error %s", resp.Symbol, resp.ErrorText)
		}
	}

	// Errors from all go routines
	var errorMessage string

	// Check left symbols
	if errorLen, _ := cm.GetLength(CACHE_KEY_SYMBOL); errorLen > 0 {
		lefts, _ := cm.GetAllFromSet(CACHE_KEY_SYMBOL)
		sdclogger.SDCLoggerInstance.Printf("Left symbols: [%v]", lefts)
		errorMessage += fmt.Sprintf("Left symbols: [%v]", lefts)
	} else {
		sdclogger.SDCLoggerInstance.Println("No left symbol.")
	}

	// Check error symbols. Symbols are valid, but fails to process.
	if errorLen, _ := cm.GetLength(CACHE_KEY_SYMBOL_ERROR); errorLen > 0 {
		errs, _ := cm.GetAllFromSet(CACHE_KEY_SYMBOL_ERROR)
		sdclogger.SDCLoggerInstance.Printf("Error Symbols: [%v]", errs)
		errorMessage += fmt.Sprintf("Error symbols: [%v]", errs)
	} else {
		sdclogger.SDCLoggerInstance.Println("No error symbol.")
	}

	// Check invalid symbols.
	if invalidLen, _ := cm.GetLength(CACHE_KEY_SYMBOL_INVALID); invalidLen > 0 {
		invalids, _ := cm.GetAllFromSet(CACHE_KEY_SYMBOL_INVALID)
		sdclogger.SDCLoggerInstance.Printf("Invalid Symbols: [%v]", invalids)
		errorMessage += fmt.Sprintf("Invalid symbols: [%v]", invalids)
	} else {
		sdclogger.SDCLoggerInstance.Println("No invalid symbol.")
	}

	if len(errorMessage) > 0 {
		return (all - errs - invalids), errors.New(errorMessage)
	} else {
		return (all - errs - invalids), nil
	}
}

type RedirectedWorker struct {
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

type EODWorker struct {
	logger    *log.Logger
	db        dbloader.DBLoader
	reader    IHttpReader
	collector *YFCollector
	exporter  YFDataExporter
	schema    string
}

func (w *RedirectedWorker) Init(cm cache.ICacheManager, logger *log.Logger) error {
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
	w.collector = NewSACollector(dbLoader, nil, logger, config.SchemaName)
	if err := w.collector.CreateTables(); err != nil {
		sdclogger.SDCLoggerInstance.Printf("Failed to create tables. Error: %s", err)
		return err
	} else {
		sdclogger.SDCLoggerInstance.Printf("All tables created")
		return nil
	}
}
func (w *RedirectedWorker) Do(symbol string, cm cache.ICacheManager) error {
	if rsymbol, err := w.collector.MapRedirectedSymbol(symbol); err != nil {
		return err
	} else if len(rsymbol) > 0 {
		symbol = rsymbol
	}

	cm.AddToSet(CACHE_KEY_SYMBOL_REDIRECTED, symbol)
	return nil
}
func (w *RedirectedWorker) Done() error {
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
	w.collector = NewSACollector(dbLoader, nil, logger, config.SchemaName)
	if err := w.collector.CreateTables(); err != nil {
		sdclogger.SDCLoggerInstance.Printf("Failed to create tables. Error: %s", err)
		return err
	} else {
		sdclogger.SDCLoggerInstance.Printf("All tables created")
		return nil
	}
}
func (w *FinancialOverviewWorker) Do(symbol string, cm cache.ICacheManager) error {
	if _, err := w.collector.CollectOverallMetrics(symbol, reflect.TypeFor[StockOverview]()); err != nil {
		return err
	} else {
		return nil
	}
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

	if _, err := w.collector.CollectFinancialsIncome(symbol, reflect.TypeFor[FinancialsIncome]()); err != nil {
		retErr = err
	}
	if _, err := w.collector.CollectFinancialsBalanceSheet(symbol, reflect.TypeFor[FinancialsBalanceShet]()); err != nil {
		retErr = err
	}
	if _, err := w.collector.CollectFinancialsCashFlow(symbol, reflect.TypeFor[FinancialsCashFlow]()); err != nil {
		retErr = err
	}
	if _, err := w.collector.CollectFinancialsRatios(symbol, reflect.TypeFor[FinancialRatios]()); err != nil {
		retErr = err
	}
	if _, err := w.collector.CollectAnalystRatings(symbol, reflect.TypeFor[AnalystsRating]()); err != nil {
		retErr = err
	}

	return retErr
}
func (w *FinancialDetailsWorker) Done() error {
	return nil
}

func (w *EODWorker) Init(cm cache.ICacheManager, logger *log.Logger) error {
	// db
	w.db = dbloader.NewPGLoader(config.SchemaName, w.logger)
	w.db.Connect(os.Getenv("PGHOST"),
		os.Getenv("PGPORT"),
		os.Getenv("PGUSER"),
		os.Getenv("PGPASSWORD"),
		os.Getenv("PGDATABASE"))

	// http reader
	w.reader = NewHttpReader(NewLocalClient())

	// Data exporter
	w.exporter = YFDataExporter{}
	w.exporter.AddExporter(NewYFDBExporter(w.db, w.schema))

	// Collector
	w.collector = NewYFCollector(w.reader, &w.exporter, w.db, logger)
	return nil
}
func (w *EODWorker) Do(symbol string, cm cache.ICacheManager) error {
	if err := w.collector.EODForSymbol(symbol); err != nil {
		return err
	}
	return nil
}
func (w *EODWorker) Done() error {
	w.db.Disconnect()
	return nil
}

func NewRedirectedParallelCollector(isContinue bool) ParallelCollector {
	w := RedirectedWorker{isContinue: isContinue}
	return ParallelCollector{Worker: &w}
}

func NewFinancialOverviewParallelCollector(isContinue bool) ParallelCollector {
	w := FinancialOverviewWorker{}
	return ParallelCollector{Worker: &w}
}

func NewFinancialDetailsParallelCollector(isContinue bool) ParallelCollector {
	w := FinancialDetailsWorker{}
	return ParallelCollector{Worker: &w}
}
