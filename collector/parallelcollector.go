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
	builder IWorkerBuilder
	cache   cache.ICacheManager
}

type IWorkerBuilder interface {
	WithLogger(l *log.Logger)
	WithDB(db dbloader.DBLoader)
	WithExporter(exp IDataExporter)
	WithReader(r IHttpReader)
	WithContinue(c bool)
	WithCache(cm cache.ICacheManager)
	Prepare() error
	Build() IWorker
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

func (pc *ParallelCollector) workerRoutine(goID string, inChan chan string, outChan chan Response, wg *sync.WaitGroup, cm cache.ICacheManager) {

	defer wg.Done()

	// Logger
	file, _ := os.OpenFile(LOG_FILE+"."+goID, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	logger := log.New(file, "sdc: ", log.Ldate|log.Ltime)
	defer file.Close()

	logMessage := func(text string) {
		logger.Println("[Go" + goID + "] " + text)
	}

	logMessage("Begin")
	pc.builder.WithLogger(logger)
	worker := pc.builder.Build()
	if err := worker.Init(cm, logger); err != nil {
		logMessage(err.Error())
		outChan <- Response{
			"", WORKER_INIT_FAILURE, err.Error(),
		}
		return
	}

	for symbol := range inChan {
		if err := worker.Do(symbol, cm); err != nil {
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

	if err := worker.Done(); err != nil {
		outChan <- Response{
			"", WORKER_DONE_FAILURE, err.Error(),
		}
	}
	logMessage("Finish")
}
func (pc *ParallelCollector) Execute(parallel int) error {

	var nAll int64
	var nLeft int64
	var summary string

	if err := pc.cache.Connect(); err != nil {
		return err
	}

	if err := pc.builder.Prepare(); err != nil {
		return err
	}

	// Get total number of symbols to be processed
	if nAll, _ = pc.cache.GetLength(CACHE_KEY_SYMBOL); nAll > 0 {
		sdclogger.SDCLoggerInstance.Printf("%d symbols to be processed in parallel(%d).", nAll, parallel)
		summary += fmt.Sprint("Total: %ld", nAll)
	} else {
		sdclogger.SDCLoggerInstance.Println("No symbol found.")
		return nil
	}

	var wg sync.WaitGroup
	inChan := make(chan string, 1000*1000)
	outChan := make(chan Response, 1000*1000)

	// Start goroutine
	i := 0
	for ; i < parallel; i++ {
		wg.Add(1)
		go pc.workerRoutine(strconv.Itoa(i), inChan, outChan, &wg, pc.cache)
	}

	// Push symbols to channel
	go func() {
		defer close(inChan) // Close the inChan when done
		for {
			symbol, err := pc.cache.PopFromSet(CACHE_KEY_SYMBOL)

			if err != nil {
				break // Exit on error
			}
			if len(symbol) == 0 {
				sdclogger.SDCLoggerInstance.Println("All symbols are pushed into input channel.")
				break
			}
			sdclogger.SDCLoggerInstance.Printf("Push %s into input channel.", symbol)
			inChan <- symbol
		}
	}()

	// Cleanup
	go func() {
		wg.Wait()
		close(outChan)
	}()

	// Handle response
	processed := 0
	succeeded := 0
	for resp := range outChan {
		// if resp.ErrorID == WORKER_INIT_FAILURE {
		// 	wg.Add(1)
		// 	pc.workerRoutine(strconv.Itoa(i), inChan, outChan, &wg, pc.cache)
		// 	i++
		// }
		processed++
		if resp.ErrorID != SUCCESS {
			sdclogger.SDCLoggerInstance.Printf("Failed to process symbol %s. Error %s", resp.Symbol, resp.ErrorText)
		} else {
			succeeded++
		}
		fmt.Printf("Processed %d, succeeded %d\n", processed, succeeded)
	}

	// Check left symbols
	if nLeft, _ := pc.cache.GetLength(CACHE_KEY_SYMBOL); nLeft > 0 {
		lefts, _ := pc.cache.GetAllFromSet(CACHE_KEY_SYMBOL)
		sdclogger.SDCLoggerInstance.Printf("Left symbols: [%v]", lefts)
		summary += fmt.Sprintf("Left: [%v]", lefts)
	} else {
		sdclogger.SDCLoggerInstance.Println("No left symbol.")
	}

	// Check error symbols. Symbols are valid, but fails to process.
	if errorLen, _ := pc.cache.GetLength(CACHE_KEY_SYMBOL_ERROR); errorLen > 0 {
		errs, _ := pc.cache.GetAllFromSet(CACHE_KEY_SYMBOL_ERROR)
		sdclogger.SDCLoggerInstance.Printf("Error Symbols: [%v]", errs)
		summary += fmt.Sprintf("Error: [%v]", errs)
	} else {
		sdclogger.SDCLoggerInstance.Println("No error symbol.")
	}

	// Check invalid symbols.
	if invalidLen, _ := pc.cache.GetLength(CACHE_KEY_SYMBOL_INVALID); invalidLen > 0 {
		invalids, _ := pc.cache.GetAllFromSet(CACHE_KEY_SYMBOL_INVALID)
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
func (pc *ParallelCollector) SetWorkerBuilder(b IWorkerBuilder) {
	pc.builder = b
}
func (pc *ParallelCollector) SetCacheManager(cm cache.ICacheManager) {
	pc.cache = cm
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

type CommonWorkerBuilder struct {
	logger     *log.Logger
	db         dbloader.DBLoader
	reader     IHttpReader
	exporter   IDataExporter
	cache      cache.ICacheManager
	isContinue bool
}

func (b *CommonWorkerBuilder) WithLogger(l *log.Logger) {
	b.logger = l
}
func (b *CommonWorkerBuilder) WithDB(db dbloader.DBLoader) {
	b.db = db
}
func (b *CommonWorkerBuilder) WithExporter(exp IDataExporter) {
	b.exporter = exp
}
func (b *CommonWorkerBuilder) WithReader(r IHttpReader) {
	b.reader = r
}
func (b *CommonWorkerBuilder) WithContinue(c bool) {
	b.isContinue = c
}
func (b *CommonWorkerBuilder) WithCache(cm cache.ICacheManager) {
	b.cache = cm
}

type RedirectedWorkerBuilder struct {
	CommonWorkerBuilder
}

type FinancialOverviewWorkerBuilder struct {
	CommonWorkerBuilder
}

type FinancialDetailsPWorkerBuilder struct {
	CommonWorkerBuilder
}

func NewEODParallelCollector(isContinue bool) ParallelCollector {
	c := cache.NewCacheManager()
	b := YFWorkerBuilder{}
	b.WithContinue(isContinue)
	b.WithCache(c)

	return ParallelCollector{&b, c}
}

func NewRedirectedParallelCollector(isContinue bool) ParallelCollector {
	return ParallelCollector{&YFWorkerBuilder{}, cache.NewCacheManager()}
}

func NewFinancialOverviewParallelCollector(isContinue bool) ParallelCollector {
	return ParallelCollector{&YFWorkerBuilder{}, cache.NewCacheManager()}
}

func NewFinancialDetailsParallelCollector(isContinue bool) ParallelCollector {
	return ParallelCollector{&YFWorkerBuilder{}, cache.NewCacheManager()}
}
