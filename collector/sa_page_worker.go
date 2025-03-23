package collector

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/wayming/sdc/cache"
	"github.com/wayming/sdc/common"
	"github.com/wayming/sdc/config"
	"github.com/wayming/sdc/dbloader"
	"github.com/wayming/sdc/sdclogger"
)

type SAPageWorkItem struct {
	symbol string
}

type SAPageWorkItemManager struct {
	cache      cache.ICacheManager
	db         dbloader.DBLoader
	logger     *log.Logger
	proxyFile  string
	nProcessed int
}

type SAPageDownloader struct {
	reader      IHttpReader
	logger      *log.Logger
	downloadDir string
	proxy       string
}

type SAPageDownloaderFactory struct {
	cache cache.ICacheManager
}

//
// Work Item Methods
//

func (swi SAPageWorkItem) ToString() string {
	return swi.symbol
}

//
// Work Item Manager Methods
//

func (m *SAPageWorkItemManager) loadSymFromDB(tableName string) error {

	type queryResult struct {
		Symbol string
	}

	sql := "SELECT symbol FROM " + tableName
	results, err := m.db.RunQuery(sql, reflect.TypeFor[queryResult]())
	if err != nil {
		return errors.New("Failed to run query [" + sql + "]. Error: " + err.Error())
	}
	queryResults, ok := results.([]queryResult)
	if !ok {
		return errors.New("failed to assert the slice of queryResults")
	} else {
		m.logger.Printf("%d symbols retrieved from table %s", len(queryResults), tableName)
	}

	for _, row := range queryResults {
		if row.Symbol == "" {
			m.logger.Printf("Ignore the empty symbol.")
			continue
		}
		if err := m.cache.AddToSet(config.CACHE_KEY_SA_SYMBOLS, row.Symbol); err != nil {
			return err
		}
	}
	return nil
}

func (m *SAPageWorkItemManager) loadSymFromCache(setName string) error {
	if err := m.cache.MoveSet(setName, config.CACHE_KEY_SA_SYMBOLS); err != nil {
		return fmt.Errorf("failed to restore the error symbols. Error: %s", err.Error())
	}
	return nil
}

func (m *SAPageWorkItemManager) loadProxyFromFile(fname string) error {
	num, err := cache.LoadProxies(m.cache, config.CACHE_KEY_PROXIES, fname)

	if err != nil {
		return err
	} else {
		m.logger.Printf("%d proxies loaded to cache", num)
		return nil
	}
}

func (m *SAPageWorkItemManager) Prepare() error {
	if length, _ := m.cache.GetLength(config.CACHE_KEY_SYMBOLS); length > 0 {
		if err := m.loadSymFromCache(config.CACHE_KEY_SYMBOLS); err != nil {
			return fmt.Errorf("Failed to load symbols from cache key %s. Error: %v", config.CACHE_KEY_SYMBOLS, err)
		}
	} else {
		if err := m.loadSymFromDB(NDSymDataTables[ND_TICKERS]); err != nil {
			return fmt.Errorf("Failed to load symbols from database table %s. Error: %v", NDSymDataTables[ND_TICKERS], err)
		}
	}

	if _, err := os.Stat(m.proxyFile); err != nil {
		return fmt.Errorf("No proxy file found from %s", m.proxyFile)
	}

	if err := m.loadProxyFromFile(m.proxyFile); err != nil {
		return fmt.Errorf("Failed to load proxies from file %s. Error: %v", m.proxyFile, err)
	}
	return nil
}

func (m *SAPageWorkItemManager) Next() (IWorkItem, error) {
	symbol, err := m.cache.PopFromSet(config.CACHE_KEY_SYMBOLS)
	if err != nil {
		return nil, err
	} else {
		return SAPageWorkItem{symbol}, nil
	}
}

func (m *SAPageWorkItemManager) Size() int64 {
	size, _ := m.cache.GetLength(config.CACHE_KEY_SYMBOLS)
	return size
}

func (m *SAPageWorkItemManager) OnProcessError(wi IWorkItem, err error) error {
	m.nProcessed++

	swi, ok := wi.(SAPageWorkItem)
	if !ok {
		return fmt.Errorf("failed to convert the work item to SAPageDownloader work item")
	}

	if err := m.cache.AddToSet(config.CACHE_KEY_SA_SYMBOLS_ERROR, swi.symbol); err != nil {
		return err
	}
	return nil
}

func (m *SAPageWorkItemManager) OnProcessSuccess(wi IWorkItem) error {
	m.nProcessed++

	// Do nothing
	return nil
}

func (m *SAPageWorkItemManager) Summary() string {
	nLeft, _ := m.cache.GetLength(config.CACHE_KEY_SYMBOLS)
	nError, _ := m.cache.GetLength(config.CACHE_KEY_SYMBOLS_ERROR)

	summary := fmt.Sprintf("Processed: %d, Left: %d, Error: %d", m.nProcessed, nLeft, nError)
	return summary
}

//
// Worker Factory Methods
//

func (f *SAPageDownloaderFactory) MakeWorker(l *log.Logger) IWorker {
	proxy, err := f.cache.PopFromSet(config.CACHE_KEY_PROXIES)
	if len(proxy) == 0 {
		sdclogger.SDCLoggerInstance.Fatalf("No usable proxy found. Error: %v", err)
	}
	return &SAPageDownloader{logger: l, proxy: proxy}
}

//
// Worker Methods
//

func (d *SAPageDownloader) Init() error {
	dateStr := time.Now().Format("20060102")
	d.downloadDir = config.DATA_DIR + "/" + dateStr

	if err := common.CreateDirIfNotExists(d.downloadDir); err != nil {
		return err
	}

	return nil
}

func (d *SAPageDownloader) Do(wi IWorkItem) error {
	swi, ok := wi.(SAPageWorkItem)
	if !ok {
		return fmt.Errorf("Failed to convert the work item to SAPageDownloader work item")
	}

	dir := d.downloadDir + "/" + swi.symbol
	if err := common.CreateDirIfNotExists(dir); err != nil {
		return err
	}

	url := "https://stockanalysis.com/stocks/" + strings.ToLower(swi.symbol) + "/financials/?p=quarterly"
	file := dir + "/" + "financial_income.html"
	if err := d.ExportSAPage(url, file); err != nil {
		return fmt.Errorf("failed to download page %s, error %v", url, err)
	}
	return nil
}

func (d *SAPageDownloader) Done() error {
	// Do nothing
	return nil
}

func (d *SAPageDownloader) ExportSAPage(url string, file string) error {

	html, err := d.reader.Read(url, nil)
	if err != nil {
		return err
	}

	if err := os.WriteFile(file, []byte(html), 0644); err != nil {
		return err
	}

	return nil
}

func (d *SAPageDownloader) Retry(err error) bool {
	e, ok := err.(HttpServerError)
	if ok {
		if e.StatusCode() == http.StatusNotFound {
			// Symbol does not exist
			log.Println("Symbol Not Valid.")
			return false
		}

		if e.StatusCode() == http.StatusTooManyRequests {
			log.Println("Too many request. Retry.")
			return true
		}
	}

	return false
}
