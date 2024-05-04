package collector

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"os"
	"reflect"

	"github.com/wayming/sdc/dbloader"
	"github.com/wayming/sdc/sdclogger"
)

type MSCollector struct {
	dbLoader    dbloader.DBLoader
	reader      HttpReader
	logger      *log.Logger
	dbSchema    string
	msAccessKey string
}

func NewMSCollector(loader dbloader.DBLoader, httpReader HttpReader, logger *log.Logger, schema string) *MSCollector {
	loader.CreateSchema(schema)
	loader.Exec("SET search_path TO " + schema)
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
	jsonText, err := collector.reader.Read(apiURL, nil)
	if err != nil {
		return errors.New("Failed to load data from url " + apiURL + ", Error: " + err.Error())
	}

	return collector.LoadTickers(string(jsonText))
}

func (collector *MSCollector) LoadTickers(jsonText string) error {
	var data TickersBody
	if err := json.Unmarshal([]byte(jsonText), &data); err != nil {
		return errors.New("Failed to unmarshal json text, Error: " + err.Error())
	}
	dataJSONText, err := json.Marshal(data.Data)
	if err != nil {
		return errors.New("Failed to marshal json struct, Error: " + err.Error())
	}

	var tickers Tickers
	tickersTable := "ms_tickers"
	numOfRows, err := collector.dbLoader.LoadByJsonText(string(dataJSONText), tickersTable, reflect.TypeOf(tickers))
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
	eodTable := "ms_eod"

	sqlQuerySymbol := "select symbol from " + collector.dbSchema + "." + "ms_tickers limit 20"
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
		jsonText, err := collector.reader.Read(apiURL, map[string]string{"symbols": row.Symbol})
		if err != nil {
			return errors.New("Failed to load data from url " + apiURL + ", Error: " + err.Error())
		}
		var data EODBody
		if err := json.Unmarshal([]byte(jsonText), &data); err != nil {
			return errors.New("Failed to unmarshal json text, Error: " + err.Error())
		}

		if len(data.Data) > 0 {

			dataJSONText, err := json.Marshal(data.Data)
			if err != nil {
				return errors.New("Failed to marshal json struct, Error: " + err.Error())
			}

			var eod EOD
			numOfRows, err := collector.dbLoader.LoadByJsonText(string(dataJSONText), eodTable, reflect.TypeOf(eod))
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

func CollectTickers(schemaName string, csvFile string) error {
	dbLoader := dbloader.NewPGLoader(schemaName, &sdclogger.SDCLoggerInstance.Logger)
	dbLoader.Connect(os.Getenv("PGHOST"),
		os.Getenv("PGPORT"),
		os.Getenv("PGUSER"),
		os.Getenv("PGPASSWORD"),
		os.Getenv("PGDATABASE"))
	reader := NewHttpDirectReader()
	collector := NewMSCollector(dbLoader, reader, &sdclogger.SDCLoggerInstance.Logger, schemaName)
	if len(csvFile) > 0 {
		reader, err := os.OpenFile(csvFile, os.O_RDONLY, 0666)
		if err != nil {
			return errors.New("Failed to open file " + csvFile)
		}

		csv, err := io.ReadAll(reader)
		if err != nil {
			return errors.New("Failed to read file " + csvFile)
		}

		return collector.LoadTickers(string(csv))
	} else {
		return collector.CollectTickers()
	}
}
