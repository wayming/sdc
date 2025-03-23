package collector

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"reflect"
	"regexp"

	"github.com/wayming/sdc/cache"
	"github.com/wayming/sdc/dbloader"
)

type IWorker interface {
	Init() error
	Do(IWorkItem) error
	Done() error
}

type IWorkerFactory interface {
	MakeWorker(*log.Logger) IWorker
}

type IWorkerBuilder interface {
	WithLogger(l *log.Logger) IWorkerBuilder
	WithDB(db dbloader.DBLoader) IWorkerBuilder
	WithExporter(exp IDataExporter) IWorkerBuilder
	WithReader(r IHttpReader) IWorkerBuilder
	WithParams(p *PCParams) IWorkerBuilder
	WithCache(cm cache.ICacheManager) IWorkerBuilder
	Prepare() error
	NewWorker() (IWorker, error)
}

type PCParams struct {
	IsContinue  bool
	TickersJSON string
	ProxyFile   string
}

type BaseWorkerBuilder struct {
	db        dbloader.DBLoader
	reader    IHttpReader
	exporters IDataExporter
	cache     cache.ICacheManager
	logger    *log.Logger
	Params    *PCParams
}

func (b *BaseWorkerBuilder) WithLogger(l *log.Logger) IWorkerBuilder {
	b.logger = l
	return b
}
func (b *BaseWorkerBuilder) WithDB(db dbloader.DBLoader) IWorkerBuilder {
	b.db = db
	return b
}
func (b *BaseWorkerBuilder) WithExporter(exp IDataExporter) IWorkerBuilder {
	b.exporters = exp
	return b
}
func (b *BaseWorkerBuilder) WithReader(r IHttpReader) IWorkerBuilder {
	b.reader = r
	return b
}
func (b *BaseWorkerBuilder) WithParams(p *PCParams) IWorkerBuilder {
	b.Params = p
	return b
}
func (b *BaseWorkerBuilder) WithCache(cm cache.ICacheManager) IWorkerBuilder {
	b.cache = cm
	return b
}
func (b *BaseWorkerBuilder) NewWorker() (IWorker, error) {
	log.Panic("BaseWorkerBuilder::NewWorker() should never be called")
	return nil, nil
}

func (b *BaseWorkerBuilder) loadSymFromFile(f string) error {
	reader, err := os.OpenFile(f, os.O_RDONLY, 0666)
	if err != nil {
		return err
	}

	textJSON, err := io.ReadAll(reader)
	if err != nil {
		return err
	}

	// Filter symbol variations
	textFiltered, err := FilterSymbolVariations(string(textJSON))
	if err != nil {
		return fmt.Errorf("failed filter symbol variations, error %v", err)
	}

	var stocksStruct []YFTickers
	if err := json.Unmarshal([]byte(textFiltered), &stocksStruct); err != nil {
		return err
	}

	symbolPattern := `\.|\$`
	reSymbol := regexp.MustCompile(symbolPattern)
	namePattern := `- Warrants`
	reName := regexp.MustCompile(namePattern)
	for _, stock := range stocksStruct {
		if len(stock.Symbol) > 0 {
			match := reSymbol.FindString(stock.Symbol)
			if len(match) > 0 {
				b.logger.Printf("Ignore symbol %s.", stock.Symbol)
				continue
			}
			match = reName.FindString(stock.Name)
			if len(match) > 0 {
				b.logger.Printf("Ignore symbol %s, name %s.", stock.Symbol, stock.Name)
				continue
			}
			if err := b.cache.AddToSet(CACHE_KEY_SYMBOL, stock.Symbol); err != nil {
				return err
			}
		} else {
			b.logger.Printf("Ignore the empty symbol.")
		}
	}
	return nil
}

func (b *BaseWorkerBuilder) loadSymFromDB(tableName string) error {

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

func (b *BaseWorkerBuilder) loadSymFromCache(setName string) error {
	if err := b.cache.MoveSet(setName, CACHE_KEY_SYMBOL); err != nil {
		return fmt.Errorf("failed to restore the error symbols. Error: %s", err.Error())
	}
	return nil
}

func (b *BaseWorkerBuilder) loadProxyFromFile(fname string) error {
	num, err := cache.LoadProxies(b.cache, CACHE_KEY_PROXY, fname)

	if err != nil {
		return err
	} else {
		b.logger.Printf("%d proxies loaded to cache", num)
		return nil
	}
}
func (b *BaseWorkerBuilder) Prepare() error {

	if len(b.Params.TickersJSON) > 0 {
		if err := b.loadSymFromFile(b.Params.TickersJSON); err != nil {
			return err
		}
	} else if b.Params.IsContinue {
		if err := b.loadSymFromCache(CACHE_KEY_SYMBOL_ERROR); err != nil {
			return err
		}
	} else {
		if err := b.loadSymFromDB(YFDataTables[YF_TICKERS]); err != nil {
			return err
		}
	}

	if len(b.Params.ProxyFile) > 0 {
		if err := b.loadProxyFromFile(b.Params.ProxyFile); err != nil {
			return err
		}
	}
	return nil
}
