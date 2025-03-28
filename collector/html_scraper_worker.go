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
	"github.com/wayming/sdc/config"
	"github.com/wayming/sdc/dbloader"
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
	logger     *log.Logger
	exporter   IDataExporter
	norm       *SAJsonNormaliser
	structMeta map[string]map[string]JsonFieldMetadata

	conn *grpc.ClientConn
}

type HtmlScraperFactory struct {
	outputBaseDir string
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
	dbLoader := dbloader.NewPGLoader(config.SCHEMA_NAME, l)
	dbLoader.Connect(os.Getenv("PGHOST"),
		os.Getenv("PGPORT"),
		os.Getenv("PGUSER"),
		os.Getenv("PGPASSWORD"),
		os.Getenv("PGDATABASE"))

	var e DataExporters
	e.AddExporter(NewDBExporter(dbLoader, config.SCHEMA_NAME)).
		AddExporter(&FileExporter{path: f.outputBaseDir})

	return &HtmlScraper{logger: l, exporter: &e, norm: &SAJsonNormaliser{}, structMeta: AllSAMetricsFields()}
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

func (d *HtmlScraper) getDataCategory(filePath string) string {
	// Populate data category by it is html file name
	baseName := filepath.Base(filePath)
	ext := filepath.Ext(filePath)
	return strings.TrimSuffix(baseName, ext)
}

func (d *HtmlScraper) normaliseJSONText(jsonText string, dataCtg string) (string, error) {
	// Unmarshal the JSON string
	var objs []map[string]interface{}
	err := json.Unmarshal([]byte(jsonText), &objs)
	if err != nil {
		d.logger.Fatalf("Failed to unmarshall json response. Error: %v", err)
	}

	var normObjs []map[string]interface{}
	for _, pairs := range objs {
		normPairs := make(map[string]interface{})
		for k, v := range pairs {
			normKey := d.norm.NormaliseJSONKey(k)
			fieldType := GetFieldTypeByTag(d.structMeta[SADataTypes[dataCtg].Name()], normKey)
			if fieldType == nil {
				return "", fmt.Errorf("failed to find the type of field for JSON key %s in the struct %v", normKey, SADataTypes[dataCtg])
			}
			strVal, ok := v.(string)
			if ok {
				normVal, err := d.norm.NormaliseJSONValue(strVal, fieldType)
				if err == nil {
					normPairs[normKey] = normVal
				} else {
					return "", fmt.Errorf("failed to normalise string %s, type %s. Error: %v", strVal, fieldType, err)

				}
			} else {
				return "", fmt.Errorf("%v is not a string", v)
			}

		}
		normObjs = append(normObjs, normPairs)
	}

	// Marshal the object back into a pretty-printed JSON string
	prettyJSON, err := json.MarshalIndent(normObjs, "", "    ")
	if err != nil {
		d.logger.Fatalf("Failed to marshall json response with prettier format. Error: %v", err)
	}

	return string(prettyJSON), nil
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
		d.logger.Fatal(err)
	}

	// Create a sample Request to send
	request := &ScraperProto.Request{
		HtmlText: string(content),
		PageType: "finanical_table",
	}

	// Call ProcessPage method from the HtmlScraper service
	response, err := client.ProcessPage(context.Background(), request)
	if err != nil {
		d.logger.Fatalf("Failed to process file %s. Response status %s.  Response body %s. Error: %v",
			swi.path, ScraperProto.StatusCode_name[int32(response.Status)], string(response.GetJsonData()), err)
	}

	if response.Status != ScraperProto.StatusCode_OK {
		d.logger.Println("Response JSON Data:", string(response.GetJsonData()))
		return fmt.Errorf("failed to process file %s. Response status %s.  Response body %s",
			swi.path, ScraperProto.StatusCode_name[int32(response.Status)], string(response.GetJsonData()))
	}

	dataCtg := d.getDataCategory(swi.path)
	noralisedJSON, err := d.normaliseJSONText(string(response.GetJsonData()), dataCtg)

	d.logger.Println("Response Status:", response.GetStatus())
	d.logger.Println("Response JSON Data:", noralisedJSON)

	parts := strings.Split(swi.path, "/")
	symbol := parts[len(parts)-2] // the second-to-last part

	d.exporter.Export(SADataTypes[dataCtg], SADataTables[dataCtg], string(noralisedJSON), symbol)
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

func NewHtmlScraperFactory(outDir string) IWorkerFactory {
	return &HtmlScraperFactory{outputBaseDir: outDir}
}

func NewParallelHtmlScraper(wFac IWorkerFactory, wim IWorkItemManager) *ParallelWorker {
	return &ParallelWorker{wFac: wFac, wim: wim}
}
