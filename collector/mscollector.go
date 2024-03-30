package collector

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"reflect"

	"github.com/wayming/sdc/dbloader"
)

const LOG_FILE = "logs/collector.log"
const SCHEMA_NAME = "msdata"

type MSCollector struct {
	dbSchema    string
	dbLoader    *dbloader.PGLoader
	logger      *log.Logger
	msAccessKey string
}

func NewMSCollector() *MSCollector {
	file, err := os.OpenFile(LOG_FILE, os.O_CREATE|os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		log.Fatal("Failed to open log file ", LOG_FILE, ". Error: ", err)
	}
	logger := log.New(file, "mscollector: ", log.Ldate|log.Ltime)
	dbLoader := dbloader.NewPGLoader(logger)

	dbLoader.Connect(os.Getenv("PGHOST"),
		os.Getenv("PGPORT"),
		os.Getenv("PGUSER"),
		os.Getenv("PGPASSWORD"),
		os.Getenv("PGDATABASE"))
	dbLoader.CreateSchema(SCHEMA_NAME)

	collector := MSCollector{
		dbSchema:    SCHEMA_NAME,
		dbLoader:    dbLoader,
		logger:      logger,
		msAccessKey: os.Getenv("MSACCESSKEY"),
	}

	return &collector
}

func (collector *MSCollector) ReadURL(url string) (string, error) {
	var jsonBody string

	httpClient := http.Client{}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return jsonBody, errors.New("Failed to create GET request for url" + url + ", Error: " + err.Error())
	}

	q := req.URL.Query()
	q.Add("access_key", collector.msAccessKey)
	req.URL.RawQuery = q.Encode()

	res, err := httpClient.Do(req)
	if err != nil {
		return jsonBody, errors.New("Failed to perform request to url" + url + ", Error: " + err.Error())
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return string(body), err
	}
	return string(body), nil
}

func (collector *MSCollector) CollectTickers() error {
	apiURL := "http://api.marketstack.com/v1/tickers"
	tickersTable := "sdc_tickers"
	jsonText, err := collector.ReadURL(apiURL)
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
