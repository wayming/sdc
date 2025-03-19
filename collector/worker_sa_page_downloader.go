package collector

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/wayming/sdc/cache"
	"github.com/wayming/sdc/common"
	"github.com/wayming/sdc/config"
)

type SAPageWorkItem struct {
	symbol string
}

type SAPageWorkItemManager struct {
	cache      cache.ICacheManager
	nProcessed int
}

type SAPageDownloader struct {
	reader      IHttpReader
	logger      *log.Logger
	downloadDir string
}

type SAPageWorkBuilder struct {
	CommonWorkerBuilder
}

func (swi SAPageWorkItem) ToString() string {
	return swi.symbol
}

func (swim *SAPageWorkItemManager) Next() (IWorkItem, error) {
	symbol, err := swim.cache.PopFromSet(config.CacheKeySymbols)
	if err != nil {
		return nil, err
	} else {
		return SAPageWorkItem{symbol}, nil
	}
}

func (swim *SAPageWorkItemManager) Size() (int64, error) {
	return swim.cache.GetLength(config.CacheKeySymbols)
}

func (swim *SAPageWorkItemManager) OnProcessError(wi IWorkItem, err error) error {
	swim.nProcessed++

	swi, ok := wi.(SAPageWorkItem)
	if !ok {
		return fmt.Errorf("Failed to convert the work item to SAPageDownloader work item")
	}

	if err := swim.cache.AddToSet(config.CacheKeySymbolsError, swi.symbol); err != nil {
		return err
	}
	return nil
}

func (swim *SAPageWorkItemManager) OnProcessSuccess(wi IWorkItem) error {
	swim.nProcessed++

	// Do nothing
	return nil
}

func (swim *SAPageWorkItemManager) Summary() string {
	nLeft, _ := swim.cache.GetLength(config.CacheKeySymbols)
	nError, _ := swim.cache.GetLength(config.CacheKeySymbolsError)

	summary := fmt.Sprintf("Processed: %d, Left: %d, Error: %d", swim.nProcessed, nLeft, nError)
	return summary
}

func (swb *SAPageWorkBuilder) NewWorker() (IWorker, error) {
	var client http.Client
	return &SAPageDownloader{logger: swb.logger, reader: NewHttpReader(&client)}, nil
}

func (se *SAPageDownloader) Init() error {
	dateStr := time.Now().Format("20060102")
	se.downloadDir = config.DataDir + "/" + dateStr

	if err := common.CreateDirIfNotExists(se.downloadDir); err != nil {
		return err
	}
	return nil
}

func (se *SAPageDownloader) Do(wi IWorkItem) error {
	swi, ok := wi.(SAPageWorkItem)
	if !ok {
		return fmt.Errorf("Failed to convert the work item to SAPageDownloader work item")
	}

	dir := se.downloadDir + "/" + swi.symbol
	if err := common.CreateDirIfNotExists(dir); err != nil {
		return err
	}

	url := "https://stockanalysis.com/stocks/" + strings.ToLower(swi.symbol) + "/financials/?p=quarterly"
	file := dir + "/" + "financial_income.html"
	if err := se.ExportSAPage(url, file); err != nil {
		return fmt.Errorf("Failed to download page %s, error %v", url, err)
	}
	return nil
}

func (se *SAPageDownloader) Done() error {
	// Do nothing
	return nil
}

func (se *SAPageDownloader) ExportSAPage(url string, file string) error {

	html, err := se.reader.Read(url, nil)
	if err != nil {
		return err
	}

	if err := os.WriteFile(file, []byte(html), 0644); err != nil {
		return err
	}

	return nil
}
