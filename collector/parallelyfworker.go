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

type YFEODWorker struct {
	logger     *log.Logger
	db         dbloader.DBLoader
	reader     IHttpReader
	exporter   IDataExporter
	collector  *YFCollector
	isContinue bool
}

type YFWorkCache struct {
}

type YFWorkerBuilder struct {
	CommonWorkerBuilder
}

func (w *YFEODWorker) Init(cm cache.ICacheManager, logger *log.Logger) error {
	// Collector
	w.collector = NewYFCollector(w.reader, w.exporter, w.db, logger)

	return nil
}
func (w *YFEODWorker) Do(symbol string, cm cache.ICacheManager) error {
	if err := w.collector.EODForSymbol(symbol); err != nil {
		return err
	}
	return nil
}
func (w *YFEODWorker) Done() error {
	w.db.Disconnect()
	return nil
}
func (b *YFWorkerBuilder) Default() error {
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
		b.exporter = NewYFDBExporter(b.db, config.SchemaName)
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

func (b *YFWorkerBuilder) loadSymFromFile(f string) error {
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

func (b *YFWorkerBuilder) loadSymFromDB(tableName string) error {

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

func (b *YFWorkerBuilder) Prepare() error {

	if len(b.tickersJSON) > 0 {
		if err := b.loadSymFromFile(b.tickersJSON); err != nil {
			return err
		}
	} else {
		if err := b.loadSymFromDB(FYDataTables[FY_TICKERS]); err != nil {
			return err
		}
	}

	return nil
}

func (b *YFWorkerBuilder) Build() IWorker {
	return &YFEODWorker{
		logger:     b.logger,
		db:         b.db,
		reader:     b.reader,
		exporter:   b.exporter,
		isContinue: b.isContinue,
	}
}

func (c *YFWorkCache) Init(isContinue bool) {

}

func NewYFWorkerBuilder() IWorkerBuilder {
	b := YFWorkerBuilder{}
	b.Default()
	return &b
}
