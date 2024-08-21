package collector

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"os"
	"reflect"

	"github.com/wayming/sdc/cache"
	"github.com/wayming/sdc/dbloader"
)

type IWorker interface {
	Init() error
	Do(symbol string) error
	Done() error
}

type IWorkerBuilder interface {
	WithLogger(l *log.Logger)
	WithDB(db dbloader.DBLoader)
	WithExporter(exp IDataExporter)
	WithReader(r IHttpReader)
	WithParams(p *PCParams)
	WithCache(cm cache.ICacheManager)
	Default() error
	Prepare() error
	Build() IWorker
}

type CommonWorkerBuilder struct {
	db       dbloader.DBLoader
	reader   IHttpReader
	exporter IDataExporter
	cache    cache.ICacheManager
	logger   *log.Logger
	Params   *PCParams
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
func (b *CommonWorkerBuilder) WithParams(p *PCParams) {
	b.Params = p
}
func (b *CommonWorkerBuilder) WithCache(cm cache.ICacheManager) {
	b.cache = cm
}

func (b *CommonWorkerBuilder) loadSymFromFile(f string) error {
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

func (b *CommonWorkerBuilder) loadSymFromDB(tableName string) error {

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

func (b *CommonWorkerBuilder) Prepare() error {

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
