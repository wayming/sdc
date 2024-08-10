package collector

import (
	"errors"
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
	return nil
}

func (b *YFWorkerBuilder) Prepare() error {

	type queryResult struct {
		Symbol string
	}

	b.Default()

	if !b.isContinue {
		sql := "SELECT symbol FROM " + FYDataTables[FY_TICKERS]
		results, err := b.db.RunQuery(sql, reflect.TypeFor[queryResult]())
		if err != nil {
			return errors.New("Failed to run query [" + sql + "]. Error: " + err.Error())
		}
		queryResults, ok := results.([]queryResult)
		if !ok {
			return errors.New("failed to assert the slice of queryResults")
		} else {
			b.logger.Printf("%d symbols retrieved from table %s", len(queryResults), FYDataTables[FY_TICKERS])
		}

		for _, row := range queryResults {
			if err := b.cache.AddToSet(CACHE_KEY_SYMBOL, row.Symbol); err != nil {
				return err
			}
		}
	}

	return nil
}

func (b *YFWorkerBuilder) Build() IWorker {
	b.Default()
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
