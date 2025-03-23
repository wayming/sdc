// Load the downloaded file of https://www.nasdaq.com/market-activity/stocks/screener
// CSV Formats:
// Symbol || Name || Last Sale || Net Change % Change || Market Cap || Country || IPO Year || Volume || Sector || Industry

// Remove the duplicate stocks
package collector

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/wayming/sdc/common"
	"github.com/wayming/sdc/config"
	"github.com/wayming/sdc/sdclogger"
)

type NDSymbolsLoaderWorkItem struct {
	symbol    string
	tickerRow string
	keys      []string
}

type NDSymbolsLoaderWorkItemManager struct {
	tickers    map[string]string
	keys       []string
	nProcessed int
	nSucceeded int
	nFailed    int
}

type NDSymbolsLoader struct {
	exporter  IDataExporter
	logger    *log.Logger
	exportDir string
}

type NDSymbolsLoaderBuilder struct {
	BaseWorkerBuilder
}

func RemoveDuplicateRows(inData []string) (map[string]string, error) {
	outData := make(map[string]string)
	shortNameToSym := make(map[string]string)
	for _, row := range inData {
		if len(row) == 0 {
			continue
		}

		fields := strings.Split(row, ",")
		if len(fields) < 2 {
			return nil, fmt.Errorf("invalid number of columns in row %s", row)
		}

		namefields := strings.Split(fields[1], " ")
		sym, ok := shortNameToSym[namefields[0]]
		if ok {
			// A symbol with longer name of a similiar company exists
			if strings.Contains(sym, fields[0]) {
				shortNameToSym[namefields[0]] = fields[0] // Keep the shorter symbol
				// Replace the key in the outData map with the shorter key
				delete(outData, sym)
				outData[fields[0]] = row
			} // Do not update the outData if a shorter symbol already exists
		} else {
			shortNameToSym[namefields[0]] = fields[0]
			outData[fields[0]] = row
		}
	}
	sdclogger.SDCLoggerInstance.Printf("Number of total rows is %d. Number of rows after removing duplication is %d", len(inData), len(outData))
	return outData, nil
}

func (wi NDSymbolsLoaderWorkItem) ToString() string {
	return wi.tickerRow
}

func (wim *NDSymbolsLoaderWorkItemManager) Next() (IWorkItem, error) {
	if len(wim.tickers) == 0 {
		return nil, nil
	}

	var wi IWorkItem
	for symbol, row := range wim.tickers {
		wim.nProcessed++
		wi = NDSymbolsLoaderWorkItem{symbol: symbol, tickerRow: row, keys: wim.keys}
		delete(wim.tickers, symbol)
		break
	}

	return wi, nil
}

func (wim *NDSymbolsLoaderWorkItemManager) Size() int64 {
	return int64(len(wim.tickers))
}

func (wim *NDSymbolsLoaderWorkItemManager) OnProcessError(wi IWorkItem, err error) error {
	wim.nFailed++

	// Do nothing
	return nil
}

func (wim *NDSymbolsLoaderWorkItemManager) OnProcessSuccess(wi IWorkItem) error {
	wim.nSucceeded++

	// Do nothing
	return nil
}

func (wim *NDSymbolsLoaderWorkItemManager) Summary() string {
	summary := fmt.Sprintf("Processed: %d, Succeeded: %d, Failure: %d, Left: %d", wim.nProcessed, wim.nSucceeded, wim.nFailed, len(wim.tickers))
	return summary
}

func (swb *NDSymbolsLoaderBuilder) NewWorker() (IWorker, error) {
	return &NDSymbolsLoader{logger: swb.logger, exporter: swb.exporters}, nil
}

func (sl *NDSymbolsLoader) Init() error {
	dateStr := time.Now().Format("20060102")
	sl.exportDir = config.DATA_DIR + "/" + dateStr + "/tickers"

	if err := common.CreateDirIfNotExists(sl.exportDir); err != nil {
		return err
	}
	return nil
}

func (sl *NDSymbolsLoader) Do(wi IWorkItem) error {
	ndwi, ok := wi.(NDSymbolsLoaderWorkItem)
	if !ok {
		return fmt.Errorf("failed to convert the work item to NDSymbolsLoader work item")
	}

	fields := strings.Split(ndwi.tickerRow, ",")
	if len(fields) != len(ndwi.keys) {
		return fmt.Errorf("inconsistent keys and values found from %s. Expected keys %v", ndwi.symbol, ndwi.keys)
	}

	var extractedDataArray []map[string]string
	extractedData := make(map[string]string)
	structType := NDSymDataTypes[ND_TICKERS]
	for idx, key := range ndwi.keys {
		_, ok := structType.FieldByName(key)
		if ok {
			extractedData[key] = common.RemoveCarriageReturn(fields[idx])
		}
	}

	// Single line
	extractedDataArray = append(extractedDataArray, extractedData)
	jsonText, err := json.Marshal(extractedDataArray)
	if err != nil {
		return err
	}

	sl.exporter.Export(NDSymDataTypes[ND_TICKERS], NDSymDataTables[ND_TICKERS], string(jsonText), ndwi.symbol)
	return nil
}

func (sl *NDSymbolsLoader) Done() error {
	// Do nothing
	return nil
}

// NewNDSymbolsLoaderWorkItem creates and returns a new NDSymbolsLoaderWorkItem instance.
func NewNDSymbolsLoaderWorkItem(symbol, tickerRow string, keys []string) NDSymbolsLoaderWorkItem {
	return NDSymbolsLoaderWorkItem{
		symbol:    symbol,
		tickerRow: tickerRow,
		keys:      keys,
	}
}

// NewNDSymbolsLoader creates and returns a new NDSymbolsLoader instance.
func NewNDSymbolsLoader(exporter IDataExporter, logger *log.Logger) *NDSymbolsLoader {
	return &NDSymbolsLoader{
		exporter: exporter,
		logger:   logger,
	}
}

// NewNDSymbolsLoaderBuilder creates and returns a new NDSymbolsLoaderBuilder instance.
func NewNDSymbolsLoaderBuilder() *NDSymbolsLoaderBuilder {
	return &NDSymbolsLoaderBuilder{}
}

func NewNDSymbolsLoaderWorkItemManager(fname string) (IWorkItemManager, error) {
	data, err := os.ReadFile(fname)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s. Error: %v", fname, err)
	}
	rows := strings.Split(string(data), "\n")
	// head line
	var keys []string
	fieldNames := strings.Split(rows[0], ",")
	for _, fieldName := range fieldNames {
		keys = append(keys, common.RemoveAllWhitespace(fieldName))
	}
	tickers, err := RemoveDuplicateRows(rows[1:])
	if err != nil {
		return nil, err
	}
	return &NDSymbolsLoaderWorkItemManager{tickers: tickers, keys: keys}, nil
}

func NewParallelNDSymbolsLoader(wb IWorkerBuilder, wim IWorkItemManager) *ParallelWorker {
	return &ParallelWorker{wb: wb, wim: wim}
}
