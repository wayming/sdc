package collector

import (
	"log"
	"os"

	"github.com/wayming/sdc/cache"
	"github.com/wayming/sdc/config"
	"github.com/wayming/sdc/dbloader"
	"github.com/wayming/sdc/sdclogger"
)

type YFEODWorker struct {
	db        dbloader.DBLoader
	reader    IHttpReader
	exporter  IDataExporter
	cache     cache.ICacheManager
	collector *YFCollector
	logger    *log.Logger
}

type YFWorkerBuilder struct {
	CommonWorkerBuilder
}

func (w *YFEODWorker) Init() error {
	// Collector
	w.collector = NewYFCollector(w.reader, w.exporter, w.db, w.logger)
	return nil
}
func (w *YFEODWorker) Do(symbol string) error {
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

func (b *YFWorkerBuilder) Build() IWorker {
	return &YFEODWorker{
		db:       b.db,
		reader:   b.reader,
		exporter: b.exporter,
		cache:    b.cache,
		logger:   b.logger,
	}
}

func NewYFWorkerBuilder() IWorkerBuilder {
	b := YFWorkerBuilder{}
	return &b
}
