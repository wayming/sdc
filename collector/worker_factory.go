package collector

import (
	"log"
)

type IWorker interface {
	Init() error
	Do(IWorkItem) error
	Retry(error) bool
	Done() error
}

type IWorkerFactory interface {
	MakeWorker(*log.Logger) IWorker
}

// type IWorkerBuilder interface {
// 	WithLogger(l *log.Logger) IWorkerBuilder
// 	WithDB(db dbloader.DBLoader) IWorkerBuilder
// 	WithExporter(exp IDataExporter) IWorkerBuilder
// 	WithReader(r IHttpReader) IWorkerBuilder
// 	WithParams(p *PCParams) IWorkerBuilder
// 	WithCache(cm cache.ICacheManager) IWorkerBuilder
// 	Prepare() error
// 	NewWorker() (IWorker, error)
// }

// type PCParams struct {
// 	IsContinue  bool
// 	TickersJSON string
// 	ProxyFile   string
// }

// type BaseWorkerBuilder struct {
// 	db        dbloader.DBLoader
// 	reader    IHttpReader
// 	exporters IDataExporter
// 	cache     cache.ICacheManager
// 	logger    *log.Logger
// 	Params    *PCParams
// }

// func (b *BaseWorkerBuilder) WithLogger(l *log.Logger) IWorkerBuilder {
// 	b.logger = l
// 	return b
// }
// func (b *BaseWorkerBuilder) WithDB(db dbloader.DBLoader) IWorkerBuilder {
// 	b.db = db
// 	return b
// }
// func (b *BaseWorkerBuilder) WithExporter(exp IDataExporter) IWorkerBuilder {
// 	b.exporters = exp
// 	return b
// }
// func (b *BaseWorkerBuilder) WithReader(r IHttpReader) IWorkerBuilder {
// 	b.reader = r
// 	return b
// }
// func (b *BaseWorkerBuilder) WithParams(p *PCParams) IWorkerBuilder {
// 	b.Params = p
// 	return b
// }
// func (b *BaseWorkerBuilder) WithCache(cm cache.ICacheManager) IWorkerBuilder {
// 	b.cache = cm
// 	return b
// }
// func (b *BaseWorkerBuilder) NewWorker() (IWorker, error) {
// 	log.Panic("BaseWorkerBuilder::NewWorker() should never be called")
// 	return nil, nil
// }

// func (b *BaseWorkerBuilder) loadSymFromFile(f string) error {
// 	reader, err := os.OpenFile(f, os.O_RDONLY, 0666)
// 	if err != nil {
// 		return err
// 	}

// 	textJSON, err := io.ReadAll(reader)
// 	if err != nil {
// 		return err
// 	}

// 	// Filter symbol variations
// 	textFiltered, err := FilterSymbolVariations(string(textJSON))
// 	if err != nil {
// 		return fmt.Errorf("failed filter symbol variations, error %v", err)
// 	}

// 	var stocksStruct []YFTickers
// 	if err := json.Unmarshal([]byte(textFiltered), &stocksStruct); err != nil {
// 		return err
// 	}

// 	symbolPattern := `\.|\$`
// 	reSymbol := regexp.MustCompile(symbolPattern)
// 	namePattern := `- Warrants`
// 	reName := regexp.MustCompile(namePattern)
// 	for _, stock := range stocksStruct {
// 		if len(stock.Symbol) > 0 {
// 			match := reSymbol.FindString(stock.Symbol)
// 			if len(match) > 0 {
// 				b.logger.Printf("Ignore symbol %s.", stock.Symbol)
// 				continue
// 			}
// 			match = reName.FindString(stock.Name)
// 			if len(match) > 0 {
// 				b.logger.Printf("Ignore symbol %s, name %s.", stock.Symbol, stock.Name)
// 				continue
// 			}
// 			if err := b.cache.AddToSet(CACHE_KEY_SYMBOL, stock.Symbol); err != nil {
// 				return err
// 			}
// 		} else {
// 			b.logger.Printf("Ignore the empty symbol.")
// 		}
// 	}
// 	return nil
// }
