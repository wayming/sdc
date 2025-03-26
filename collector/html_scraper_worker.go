package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/wayming/sdc/cache"
	ScraperProto "github.com/wayming/sdc/collector/proto"
	"github.com/wayming/sdc/common"
	"github.com/wayming/sdc/config"
	"github.com/wayming/sdc/sdclogger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type HtmlScraperWorkItem struct {
	path string
}

type HtmlScraperWorkItemManager struct {
	cache      cache.ICacheManager
	logger     *log.Logger
	inputDir   string
	nProcessed int
}

type HtmlScraper struct {
	logger   *log.Logger
	exporter IDataExporter
	conn     *grpc.ClientConn
}

type HtmlScraperFactory struct {
}

//
// Work Item Methods
//

func (swi HtmlScraperWorkItem) ToString() string {
	return swi.path
}

//
// Work Item Manager Methods
//

func (m *HtmlScraperWorkItemManager) Prepare() error {

	matches, err := filepath.Glob(m.inputDir + "/*/*.html")
	if err != nil {
		return fmt.Errorf("failed to find html files under %s", m.inputDir)
	}

	for _, path := range matches {
		m.logger.Printf("add html page %s to cache key %s", path, config.CACHE_KEY_HTML_FILES)
		if err := m.cache.AddToSet(config.CACHE_KEY_HTML_FILES, path); err != nil {
			return fmt.Errorf("failed to add html page %s. Error: %v", path, err)
		}
	}

	m.logger.Printf("%d html pages added", len(matches))

	return nil
}

func (m *HtmlScraperWorkItemManager) Next() (IWorkItem, error) {
	path, err := m.cache.PopFromSet(config.CACHE_KEY_HTML_FILES)
	if err != nil || path == "" {
		return nil, err
	} else {
		return HtmlScraperWorkItem{path: path}, nil
	}
}

func (m *HtmlScraperWorkItemManager) Size() int64 {
	size, _ := m.cache.GetLength(config.CACHE_KEY_HTML_FILES)
	return size
}

func (m *HtmlScraperWorkItemManager) OnProcessError(wi IWorkItem, err error) error {
	m.nProcessed++

	swi, ok := wi.(HtmlScraperWorkItem)
	if !ok {
		return fmt.Errorf("failed to convert the work item to HtmlScraper work item")
	}

	if err := m.cache.AddToSet(config.CACHE_KEY_HTML_FILES_ERROR, swi.path); err != nil {
		return err
	}
	return nil
}

func (m *HtmlScraperWorkItemManager) OnProcessSuccess(wi IWorkItem) error {
	m.nProcessed++

	// Do nothing
	return nil
}

func (m *HtmlScraperWorkItemManager) Summary() string {
	nLeft, _ := m.cache.GetLength(config.CACHE_KEY_HTML_FILES)
	nError, _ := m.cache.GetLength(config.CACHE_KEY_HTML_FILES_ERROR)

	summary := fmt.Sprintf("Processed: %d, Left: %d, Error: %d", m.nProcessed, nLeft, nError)
	return summary
}

//
// Worker Factory Methods
//

func (f *HtmlScraperFactory) MakeWorker(l *log.Logger) IWorker {
	return &HtmlScraper{logger: l, exporter: &FileExporter{}}
}

//
// Worker Methods
//

func (d *HtmlScraper) Init() error {

	conn, err := grpc.NewClient(
		config.SCRAPER_HOST+":"+config.SCRAPER_PORT,              // Server address
		grpc.WithTransportCredentials(insecure.NewCredentials()), // For insecure connection (no TLS)
	)
	if err != nil {
		log.Fatalf("Failed to connect to scraper server %s. Error: %v", config.SCRAPER_HOST+":"+config.SCRAPER_PORT, err)
	}

	d.conn = conn
	return nil
}

func (d *HtmlScraper) Do(wi IWorkItem) error {
	swi, ok := wi.(HtmlScraperWorkItem)
	if !ok {
		return fmt.Errorf("failed to convert the work item to HtmlScraper work item")
	}

	d.logger.Printf("process file %s", swi.path)
	// Create a new HtmlScraper client
	client := ScraperProto.NewHtmlScraperClient(d.conn)

	// Read a html file
	content, err := os.ReadFile(swi.path)
	if err != nil {
		log.Fatal(err)
	}

	// Create a sample Request to send
	request := &ScraperProto.Request{
		HtmlText: string(content),
		PageType: "finanical_table",
	}

	// Call ProcessPage method from the HtmlScraper service
	response, err := client.ProcessPage(context.Background(), request)
	if err != nil || response.Status != ScraperProto.StatusCode_OK {
		log.Fatalf("Failed to process file %s. Response status %s. Error: %v",
			swi.path, ScraperProto.StatusCode_name[int32(response.Status)], err)
	}
	log.Println("Response JSON Data:", string(response.GetJsonData()))

	// Unmarshal the JSON string
	var obj []map[string]interface{}
	err = json.Unmarshal([]byte(string(response.GetJsonData())), &obj)
	if err != nil {
		log.Fatalf("Failed to unmarshall json response. Error: %v", err)
	}

	// Marshal the object back into a pretty-printed JSON string
	prettyJSON, err := json.MarshalIndent(obj, "", "    ")
	if err != nil {
		log.Fatalf("Failed to marshall json response with prettier format. Error: %v", err)
	}

	log.Println("Response Status:", response.GetStatus())
	log.Println("Response JSON Data:", string(prettyJSON))

	parts := strings.Split(swi.path, "/")
	symbol := parts[len(parts)-2] // the second-to-last part

	// Extract table name, popu
	baseName := filepath.Base(swi.path)
	ext := filepath.Ext(swi.path)
	saCategory := "SA" + common.ConvertToPascalCase(strings.TrimSuffix(baseName, ext))
	d.exporter.Export(SADataTypes[saCategory], SADataTables[saCategory], string(prettyJSON), symbol)
	return nil
}

func (d *HtmlScraper) Done() error {
	d.conn.Close()
	return nil
}

func (d *HtmlScraper) Retry(err error) bool {
	// No retry
	return false
}

// Creator functions
func NewHtmlScraperWorkItemManager(inputDir string) IWorkItemManager {
	return &HtmlScraperWorkItemManager{
		cache:    cache.NewCacheManager(),
		logger:   sdclogger.SDCLoggerInstance,
		inputDir: inputDir,
	}
}

func NewHtmlScraperFactory() IWorkerFactory {
	return &HtmlScraperFactory{}
}

func NewParallelHtmlScraper(wFac IWorkerFactory, wim IWorkItemManager) *ParallelWorker {
	return &ParallelWorker{wFac: wFac, wim: wim}
}
