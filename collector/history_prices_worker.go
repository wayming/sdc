package collector

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/wayming/sdc/cache"
	"github.com/wayming/sdc/common"
	"github.com/wayming/sdc/config"
	"github.com/wayming/sdc/dbloader"
	"github.com/wayming/sdc/sdclogger"
)

type HistPriceWorkItem struct {
	symbol string
}

type HistPriceWorkItemManager struct {
	cache     cache.ICacheManager
	db        dbloader.DBLoader
	logger    *log.Logger
	singleSym string

	nProcessed int
}

type HistPriceDownloader struct {
	logger   *log.Logger
	reader   IHttpReader
	exporter IDataExporter
	norm     *SAJsonNormaliser
}

type HistPriceDownloaderFactory struct {
}

//
// Work Item Methods
//

func (swi HistPriceWorkItem) ToString() string {
	return swi.symbol
}

//
// Work Item Manager Methods
//

func (m *HistPriceWorkItemManager) Prepare() error {
	if len(m.singleSym) > 0 {
		m.logger.Printf("download pages for symbol %s", m.singleSym)
		if err := m.cache.AddToSet(config.CACHE_KEY_OPENBB_SYMBOLS, m.singleSym); err != nil {
			return fmt.Errorf("failed to prepare work item to process. Error: %v", err)
		}
	} else {
		if length, _ := m.cache.GetLength(config.CACHE_KEY_SYMBOLS); length > 0 {
			if err := LoadSymbolFromCahce(config.CACHE_KEY_SYMBOLS, config.CACHE_KEY_OPENBB_SYMBOLS); err != nil {
				return fmt.Errorf("failed to load symbols from cache key %s. Error: %v", config.CACHE_KEY_SYMBOLS, err)
			}
			m.logger.Printf("download pages for symbols from cache key %s", config.CACHE_KEY_OPENBB_SYMBOLS)
		} else {
			if err := LoadSymbolFromDBToCahce(NDSymDataTables[ND_TICKERS], config.CACHE_KEY_SYMBOLS); err != nil {
				return fmt.Errorf("failed to load symbols from database table %s. Error: %v", NDSymDataTables[ND_TICKERS], err)
			}
			m.logger.Printf("download pages for symbols from database table %s", NDSymDataTables[ND_TICKERS])

		}
	}
	return nil
}

func (m *HistPriceWorkItemManager) Next() (IWorkItem, error) {
	sym, err := m.cache.PopFromSet(config.CACHE_KEY_OPENBB_SYMBOLS)
	if err != nil || sym == "" {
		return nil, err
	} else {
		return HistPriceWorkItem{symbol: sym}, nil
	}
}

func (m *HistPriceWorkItemManager) Size() int64 {
	size, _ := m.cache.GetLength(config.CACHE_KEY_OPENBB_SYMBOLS)
	return size
}

func (m *HistPriceWorkItemManager) OnProcessError(wi IWorkItem, err error) error {
	m.nProcessed++

	swi, ok := wi.(HistPriceWorkItem)
	if !ok {
		return fmt.Errorf("failed to convert the work item to HistPriceDownloader work item")
	}

	if err := m.cache.AddToSet(config.CACHE_KEY_HTML_FILES_ERROR, swi.symbol); err != nil {
		return err
	}
	return nil
}

func (m *HistPriceWorkItemManager) OnProcessSuccess(wi IWorkItem) error {
	m.nProcessed++

	// Do nothing
	return nil
}

func (m *HistPriceWorkItemManager) Summary() string {
	nLeft, _ := m.cache.GetLength(config.CACHE_KEY_OPENBB_SYMBOLS)
	nError, _ := m.cache.GetLength(config.CACHE_KEY_OPENBB_SYMBOLS_ERROR)

	summary := fmt.Sprintf("Processed: %d, Left: %d, Error: %d", m.nProcessed, nLeft, nError)
	return summary
}

//
// Worker Factory Methods
//

func (f *HistPriceDownloaderFactory) MakeWorker(l *log.Logger) IWorker {
	dbLoader := dbloader.NewPGLoader(config.SCHEMA_NAME, l)
	dbLoader.Connect(os.Getenv("PGHOST"),
		os.Getenv("PGPORT"),
		os.Getenv("PGUSER"),
		os.Getenv("PGPASSWORD"),
		os.Getenv("PGDATABASE"))

	dateStr := time.Now().Format("20060102")
	outputDir := config.DATA_DIR + "/" + dateStr

	if err := common.CreateDirIfNotExists(outputDir); err != nil {
		return nil
	}

	var e DataExporters
	e.AddExporter(NewDBExporter(dbLoader, config.SCHEMA_NAME)).
		AddExporter(&FileExporter{path: outputDir})
	clt := NewLocalClient()
	return &HistPriceDownloader{logger: l, reader: NewHttpReader(clt), exporter: &e, norm: &SAJsonNormaliser{}}
}

//
// Worker Methods
//

func (d *HistPriceDownloader) Init() error {
	return nil
}

func (d *HistPriceDownloader) normaliseJSONText(JSONText string) (string, error) {

	// Unmarshal the JSON string
	var response map[string][]map[string]interface{}
	err := json.Unmarshal([]byte(JSONText), &response)
	if err != nil {
		return "", fmt.Errorf("failed to unmarshall json response. Error: %v", err)
	}

	results, ok := response["results"]
	if !ok {
		return "", fmt.Errorf("no results found from the JSON text")
	}
	// var normObjs []map[string]interface{}
	// for _, pairs := range results {
	// 	normPairs := make(map[string]interface{})
	// 	for k, v := range pairs {
	// 		normKey := d.norm.NormaliseJSONKey(k)
	// 		fieldType := GetFieldTypeByTag(GetJsonStructMetadata(OpenBBDataTypes[OPENBB_HIST_PRICE_INTRADAY]), normKey)
	// 		if fieldType == nil {
	// 			return "", fmt.Errorf("failed to find the type of field for JSON key %s in the struct %v", normKey, OpenBBDataTypes[OPENBB_HIST_PRICE_INTRADAY])
	// 		}
	// 		strVal, ok := v.(string)
	// 		if ok {
	// 			normVal, err := d.norm.NormaliseJSONValue(strVal, fieldType)
	// 			if err == nil {
	// 				normPairs[normKey] = normVal
	// 			} else {
	// 				return "", fmt.Errorf("failed to normalise string %s, type %s. Error: %v", strVal, fieldType, err)

	// 			}
	// 		} else {
	// 			return "", fmt.Errorf("%v is not a string", v)
	// 		}
	// 	}

	// 	normObjs = append(normObjs, normPairs)
	// }

	// if len(normObjs) == 0 {
	// 	return "", fmt.Errorf("no normalised data")
	// }
	// Marshal the object back into a pretty-printed JSON string
	prettyJSON, err := json.MarshalIndent(results, "", "    ")
	if err != nil {
		d.logger.Fatalf("Failed to marshall json response with prettier format. Error: %v", err)
	}

	return string(prettyJSON), nil
}

func (d *HistPriceDownloader) Do(wi IWorkItem) error {
	swi, ok := wi.(HistPriceWorkItem)
	if !ok {
		return fmt.Errorf("failed to convert the work item to HistPriceDownloader work item")
	}

	d.logger.Printf("process symbol %s", swi.symbol)

	params := map[string]string{
		"chart":           "false",
		"provider":        "yfinance",
		"symbol":          swi.symbol,
		"start_date":      "2010-01-01",
		"interval":        "1d",
		"adjustment":      "splits_only",
		"extended_hours":  "false",
		"use_cache":       "true",
		"timezone":        "America%2FNew_York",
		"source":          "realtime",
		"sort":            "asc",
		"limit":           "49999",
		"include_actions": "true",
	}

	JSONText, err := d.reader.Read("http://openbb:6900/api/v1/equity/price/historical", params)
	if err != nil {
		return fmt.Errorf("Failed to read history price information from OpenBB for symbol %s", swi.symbol)
	}
	d.logger.Printf("JSON Text: %s", JSONText)

	normalised, err := d.normaliseJSONText(JSONText)
	if err != nil {
		return fmt.Errorf("failed to process symbol %s. Error: %v", swi.symbol, err)
	}

	d.logger.Printf("Normalised JSON Text: %s", normalised)

	if err := d.exporter.Export(
		OpenBBDataTypes[OPENBB_HIST_PRICE_INTRADAY],
		OpenBBDataTables[OPENBB_HIST_PRICE_INTRADAY],
		normalised, swi.symbol); err != nil {
		return fmt.Errorf("failed to export data for symbol %s. Error: %v", swi.symbol, err)
	}

	return nil
}

func (d *HistPriceDownloader) Done() error {
	// Nothing to do
	return nil
}

func (d *HistPriceDownloader) Retry(err error) bool {
	// No retry
	return false
}

// Creator functions
func NewHistPriceWorkItemManager(singleSym string) IWorkItemManager {
	dbLoader := dbloader.NewPGLoader(config.SCHEMA_NAME, sdclogger.SDCLoggerInstance)
	dbLoader.Connect(os.Getenv("PGHOST"),
		os.Getenv("PGPORT"),
		os.Getenv("PGUSER"),
		os.Getenv("PGPASSWORD"),
		os.Getenv("PGDATABASE"))

	return &HistPriceWorkItemManager{
		db:        dbLoader,
		cache:     cache.NewCacheManager(),
		logger:    sdclogger.SDCLoggerInstance,
		singleSym: singleSym,
	}
}

func NewHistPriceDownloaderFactory(outDir string) IWorkerFactory {
	return &HistPriceDownloaderFactory{}
}

func NewParallelHistPriceDownloader(wFac IWorkerFactory, wim IWorkItemManager) *ParallelWorker {
	return &ParallelWorker{wFac: wFac, wim: wim}
}
