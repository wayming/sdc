// Load the downloaded file of https://www.nasdaq.com/market-activity/stocks/screener
// Remove the duplicate stocks
package collector

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/wayming/sdc/common"
	"github.com/wayming/sdc/config"
)

type NDSymbolsLoaderWorkItem struct {
	symbol    string
	tickerRow string
}

type NDSymbolsLoaderWorkItemManager struct {
	tickers    map[string]string
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
	CommonWorkerBuilder
}

func RemoveDuplicateRows(inData []string) (map[string]string, error) {
	outData := make(map[string]string)
	shortNameToSym := make(map[string]string)
	for _, row := range inData {
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
	return outData, nil
}

func NewNDSymbolsLoaderWorkItemManager(fname string) (IWorkItemManager, error) {
	data, err := os.ReadFile(fname)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s. Error: %v", fname, err)
	}
	rows := strings.Split(string(data), "\n")
	tickers, _ := RemoveDuplicateRows(rows)
	return &NDSymbolsLoaderWorkItemManager{tickers: tickers}, nil
}

func (wi NDSymbolsLoaderWorkItem) ToString() string {
	return wi.tickerRow
}

func (wim *NDSymbolsLoaderWorkItemManager) Next() (IWorkItem, error) {
	if len(wim.tickers) == 0 {
		return nil, nil
	}

	for symbol, row := range wim.tickers {
		wim.nProcessed++
		return &NDSymbolsLoaderWorkItem{symbol: symbol, tickerRow: row}, nil
	}

	return nil, fmt.Errorf("no ticker to return")
}

func (wim *NDSymbolsLoaderWorkItemManager) Size() (int64, error) {
	return int64(len(wim.tickers)), nil
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
	sl.exportDir = config.DataDir + "/" + dateStr + "/tickers"

	if err := common.CreateDirIfNotExists(sl.exportDir); err != nil {
		return err
	}
	return nil
}

func (sl *NDSymbolsLoader) Do(wi IWorkItem) error {
	_, ok := wi.(NDSymbolsLoaderWorkItem)
	if !ok {
		return fmt.Errorf("failed to convert the work item to NDSymbolsLoader work item")
	}

	return nil
}

func (sl *NDSymbolsLoader) Done() error {
	// Do nothing
	return nil
}
