package collector

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"reflect"
	"strings"

	"github.com/wayming/sdc/dbloader"
	"golang.org/x/net/html"
)

const LOG_FILE = "logs/collector.log"
const SCHEMA_NAME = "msdata"

type MSCollector struct {
	dbSchema    string
	dbLoader    dbloader.DBLoader
	logger      *log.Logger
	msAccessKey string
}

func NewMSCollector() *MSCollector {
	file, err := os.OpenFile(LOG_FILE, os.O_CREATE|os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		log.Fatal("Failed to open log file ", LOG_FILE, ". Error: ", err)
	}
	logger := log.New(file, "mscollector: ", log.Ldate|log.Ltime)
	loader := dbloader.NewPGLoader(SCHEMA_NAME, logger)

	loader.Connect(os.Getenv("PGHOST"),
		os.Getenv("PGPORT"),
		os.Getenv("PGUSER"),
		os.Getenv("PGPASSWORD"),
		os.Getenv("PGDATABASE"))
	loader.CreateSchema(SCHEMA_NAME)

	collector := MSCollector{
		dbSchema:    SCHEMA_NAME,
		dbLoader:    loader,
		logger:      logger,
		msAccessKey: os.Getenv("MSACCESSKEY"),
	}

	return &collector
}

func (collector *MSCollector) ReadURL(url string, params map[string]string) (string, error) {
	var htmlContent string

	httpClient := http.Client{}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return htmlContent, errors.New("Failed to create GET request for url" + url + ", Error: " + err.Error())
	}

	q := req.URL.Query()
	q.Add("access_key", collector.msAccessKey)
	for key, val := range params {
		q.Add(key, val)
	}
	req.URL.RawQuery = q.Encode()

	res, err := httpClient.Do(req)
	if err != nil {
		return htmlContent, errors.New("Failed to perform request to url" + url + ", Error: " + err.Error())
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return string(body), err
	}
	return string(body), nil
}

func (collector *MSCollector) TraverseHTML(node *html.Node) {
	if node.Type == html.ElementNode {
		for _, attr := range node.Attr {
			if attr.Key == "data-test" && attr.Val == "overview-info" {
				collector.TraverseTable(node)
			}

			if attr.Key == "data-test" && attr.Val == "financials" {
				collector.TraverseTable(node)
			}
		}
	}

	for child := node.FirstChild; child != nil; child = child.NextSibling {
		collector.TraverseHTML(child)
	}
}
func (collector *MSCollector) TraverseTable(node *html.Node) {
	if node.Type == html.TextNode {
		collector.logger.Println(node.Data)
	}

	for child := node.FirstChild; child != nil; child = child.NextSibling {
		collector.TraverseTable(child)
	}
}

func (collector *MSCollector) ReadStockAnalysisPage(url string, params map[string]string) (string, error) {
	htmlContent, err := collector.ReadURL(url, params)
	var jsonText string
	if err != nil {
		return jsonText, err
	}

	htmlDoc, err := html.Parse(strings.NewReader(htmlContent))
	if err != nil {
		return jsonText, errors.New("Failed to parse the html page " + url + ". Error: " + err.Error())
	}

	collector.TraverseHTML(htmlDoc)

	return jsonText, nil
}

func (collector *MSCollector) CollectTickers() error {
	apiURL := "http://api.marketstack.com/v1/tickers"
	tickersTable := "sdc_tickers"
	jsonText, err := collector.ReadURL(apiURL, nil)
	if err != nil {
		return errors.New("Failed to load data from url " + apiURL + ", Error: " + err.Error())
	}

	var data TickersBody
	if err := json.Unmarshal([]byte(jsonText), &data); err != nil {
		return errors.New("Failed to unmarshal json text, Error: " + err.Error())
	}
	dataJsonText, err := json.Marshal(data.Data)
	if err != nil {
		return errors.New("Failed to marshal json struct, Error: " + err.Error())
	}

	var tickers Tickers
	numOfRows, err := collector.dbLoader.LoadByJsonText(string(dataJsonText), tickersTable, reflect.TypeOf(tickers))
	if err != nil {
		return errors.New("Failed to load json text to table " + tickersTable + ". Error: " + err.Error())
	}
	collector.logger.Println(numOfRows, "rows were loaded into ", collector.dbSchema, ":"+tickersTable+" table")
	return nil
}

func (collector *MSCollector) CollectEOD() error {
	type queryResult struct {
		Symbol string
	}

	apiURL := "http://api.marketstack.com/v1/eod"
	eodTable := "sdc_eod"

	sqlQuerySymbol := "select symbol from " + collector.dbSchema + "." + "sdc_tickers limit 20"
	results, err := collector.dbLoader.RunQuery(sqlQuerySymbol, reflect.TypeFor[queryResult]())
	if err != nil {
		return errors.New("Failed to run query [" + sqlQuerySymbol + "]. Error: " + err.Error())
	}
	queryResults, ok := results.([]queryResult)
	if !ok {
		return errors.New("failed to run assert the query results are returned as a slice of queryResults")

	}

	for _, row := range queryResults {
		collector.logger.Println("Load EDO for symbool", row.Symbol)
		jsonText, err := collector.ReadURL(apiURL, map[string]string{"symbols": row.Symbol})
		if err != nil {
			return errors.New("Failed to load data from url " + apiURL + ", Error: " + err.Error())
		}
		var data EODBody
		if err := json.Unmarshal([]byte(jsonText), &data); err != nil {
			return errors.New("Failed to unmarshal json text, Error: " + err.Error())
		}

		if len(data.Data) > 0 {

			dataJsonText, err := json.Marshal(data.Data)
			if err != nil {
				return errors.New("Failed to marshal json struct, Error: " + err.Error())
			}

			var eod EOD
			numOfRows, err := collector.dbLoader.LoadByJsonText(string(dataJsonText), eodTable, reflect.TypeOf(eod))
			if err != nil {
				return errors.New("Failed to load json text to table " + eodTable + ". Error: " + err.Error())
			}
			collector.logger.Println(numOfRows, "rows were loaded into ", collector.dbSchema, ":"+eodTable+" table")
		} else {
			collector.logger.Println("No data found for symbol", row.Symbol)
		}

	}

	return nil
}
