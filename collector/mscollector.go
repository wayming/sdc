package collector

import (
	"encoding/json"
	"errors"
	"log"
	"os"
	"reflect"

	"github.com/wayming/sdc/dbloader"
)

type MSCollector struct {
	dbLoader    dbloader.DBLoader
	logger      *log.Logger
	dbSchema    string
	msAccessKey string
}

func NewMSCollector(loader dbloader.DBLoader, logger *log.Logger, schema string) *MSCollector {
	loader.CreateSchema(schema)
	collector := MSCollector{
		dbLoader:    loader,
		logger:      logger,
		dbSchema:    schema,
		msAccessKey: os.Getenv("MSACCESSKEY"),
	}
	return &collector
}

func (collector *MSCollector) CollectTickers() error {
	apiURL := "http://api.marketstack.com/v1/tickers"
	tickersTable := "sdc_tickers"
	jsonText, err := ReadURL(apiURL, nil)
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
		jsonText, err := ReadURL(apiURL, map[string]string{"symbols": row.Symbol})
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
