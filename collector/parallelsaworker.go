package collector

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"os"
	"reflect"

	"github.com/wayming/sdc/cache"
	"github.com/wayming/sdc/config"
	"github.com/wayming/sdc/dbloader"
	"github.com/wayming/sdc/sdclogger"
)

type SAWorker struct {
	db        dbloader.DBLoader
	reader    IHttpReader
	exporter  IDataExporter
	cache     cache.ICacheManager
	collector *SACollector
	logger    *log.Logger
}

type SAWorkerBuilder struct {
	CommonWorkerBuilder
}

func (w *SAWorker) Init() error {
	// Collector
	w.collector = NewSACollector(w.reader, w.exporter, w.db, w.logger)
	if err := w.collector.CreateTables(); err != nil {
		return err
	}
	return nil
}
func (w *SAWorker) Do(symbol string) error {
	redirectedSymbol, err := w.collector.MapRedirectedSymbol(symbol)
	if err != nil {
		return err
	}

	if len(redirectedSymbol) > 0 {
		symbol = redirectedSymbol
	}

	if _, err := w.collector.CollectFinancialOverview(symbol); err != nil {
		return err
	}

	if err := w.collector.CollectFinancialDetails(symbol); err != nil {
		return err
	}
	return nil
}
func (w *SAWorker) Done() error {
	return nil
}
func (b *SAWorkerBuilder) Default() error {
	if b.logger == nil {
		b.logger = &sdclogger.SDCLoggerInstance.Logger
	}

	if b.db == nil {
		b.db = dbloader.NewPGLoader(config.SchemaName, b.logger)
		b.db.Connect(os.Getenv("PGHOST"),
			os.Getenv("PGPORT"),
			os.Getenv("PGUSER"),
			os.Getenv("PGPASSWORD"),
			os.Getenv("PGDATABASE"))
	}

	if b.exporter == nil {
		b.exporter = NewDBExporter(b.db, config.SchemaName)
	}

	if b.reader == nil {
		b.reader = NewHttpReader(NewLocalClient())
	}

	if b.cache == nil {
		b.cache = cache.NewCacheManager()
		b.cache.Connect()
	}

	// b.TickersJson defaults to nil. Load from database by default.

	return nil
}

func (b *SAWorkerBuilder) loadSymFromFile(f string) error {
	reader, err := os.OpenFile(f, os.O_RDONLY, 0666)
	if err != nil {
		return err
	}

	textJSON, err := io.ReadAll(reader)
	if err != nil {
		return err
	}

	var stocksStruct []FYTickers
	if err := json.Unmarshal(textJSON, &stocksStruct); err != nil {
		return err
	}

	for _, stock := range stocksStruct {
		if len(stock.Symbol) > 0 {
			if err := b.cache.AddToSet(CACHE_KEY_SYMBOL, stock.Symbol); err != nil {
				return err
			}
		} else {
			b.logger.Printf("Ignore the empty symbol.")
		}
	}
	return nil
}

func (b *SAWorkerBuilder) loadSymFromDB(tableName string) error {

	type queryResult struct {
		Symbol string
	}

	sql := "SELECT symbol FROM " + tableName
	results, err := b.db.RunQuery(sql, reflect.TypeFor[queryResult]())
	if err != nil {
		return errors.New("Failed to run query [" + sql + "]. Error: " + err.Error())
	}
	queryResults, ok := results.([]queryResult)
	if !ok {
		return errors.New("failed to assert the slice of queryResults")
	} else {
		b.logger.Printf("%d symbols retrieved from table %s", len(queryResults), tableName)
	}

	for _, row := range queryResults {
		if row.Symbol == "" {
			b.logger.Printf("Ignore the empty symbol.")
			continue
		}
		if err := b.cache.AddToSet(CACHE_KEY_SYMBOL, row.Symbol); err != nil {
			return err
		}
	}
	return nil
}

func (b *SAWorkerBuilder) Prepare() error {

	if len(b.Params.TickersJSON) > 0 {
		if err := b.loadSymFromFile(b.Params.TickersJSON); err != nil {
			return err
		}
	} else {
		if err := b.loadSymFromDB(FYDataTables[FY_TICKERS]); err != nil {
			return err
		}
	}

	return nil
}

func (b *SAWorkerBuilder) Build() IWorker {
	return &SAWorker{
		db:       b.db,
		reader:   b.reader,
		exporter: b.exporter,
		cache:    b.cache,
		logger:   b.logger,
	}
}

func NewSAWorkerBuilder() IWorkerBuilder {
	b := SAWorkerBuilder{}
	return &b
}
