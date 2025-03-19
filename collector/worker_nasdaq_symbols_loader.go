// Load the downloaded file of https://www.nasdaq.com/market-activity/stocks/screener
// Remove the duplicate stocks
package collector

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/wayming/sdc/common"
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
	exporter IDataExporter
	logger   *log.Logger
}

type NDSymbolsLoaderBuilder struct {
	CommonWorkerBuilder
}

func RemoveDuplicateRows(inData []string) (map[string]string, error) {
	var outData map[string]string
	var shortNameToSym map[string]string
	for _, row := range inData {
		fields := strings.Split(row, ",")
		if len(fields) < 2 {
			return nil, fmt.Errorf("Invalid number of columns in row %s", row)
		}

		namefields := strings.Split(fields[1], " ")
		sym, ok := shortNameToSym[namefields[0]]
		if ok {
			// Check if a shorter symbol for a similiar company already exists
			if strings.Contains(sym, fields[0]) {
				shortNameToSym[namefields[0]] = fields[0] // Keep the shorter symbol
				// Replace the key in the outData map with the shorter key
				if _, ok := outData[sym]; ok {
					delete(outData, sym)
				}
			}
		} else {
			shortNameToSym[namefields[0]] = fields[0]
		}
		outData[fields[0]] = row
	}
	return outData, nil
}

func NewNDSymbolsLoaderWorkItemManager(fname string) (IWorkItemManager, error) {
	data, err := os.ReadFile(fname)
	if err != nil {
		return nil, fmt.Errorf("Failed to read file %s. Error: %v", fname, err)
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

	return nil, fmt.Errorf("No ticker to return")
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

func (nsl *NDSymbolsLoader) Init() error {
	return nil
}

func (nsl *NDSymbolsLoader) Do(wi IWorkItem) error {
	swi, ok := wi.(NDSymbolsLoaderWorkItem)
	if !ok {
		return fmt.Errorf("Failed to convert the work item to NDSymbolsLoader work item")
	}

	dir := se.exportDir + "/" + swi.symbol
	if err := common.CreateDirIfNotExists(dir); err != nil {
		return err
	}

	url := "https://stockanalysis.com/stocks/" + strings.ToLower(swi.symbol) + "/financials/?p=quarterly"
	file := dir + "/" + "financial_income.html"
	if err := se.ExportNDSymbolsLoader(url, file); err != nil {
		return fmt.Errorf("Failed to download page %s, error %v", url, err)
	}
	return nil
}

func (nsl *NDSymbolsLoader) Done() error {
	// Do nothing
	return nil
}

func (nsl *NDSymbolsLoader) ExportNDSymbolsLoader(url string, file string) error {

	html, err := se.reader.Read(url, nil)
	if err != nil {
		return err
	}

	if err := os.WriteFile(file, []byte(html), 0644); err != nil {
		return err
	}
}
