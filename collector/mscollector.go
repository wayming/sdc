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
	reader      IHttpReader
	logger      *log.Logger
	dbSchema    string
	msAccessKey string
}

func NewMSCollector(loader dbloader.DBLoader, httpReader IHttpReader, logger *log.Logger, schema string) *MSCollector {
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

func (collector *MSCollector) CollectTickers() (int64, error) {
	apiURL := "http://api.marketstack.com/v1/tickers"
	jsonText, err := collector.reader.Read(apiURL, nil)
	if err != nil {
		return 0, errors.New("Failed to load data from url " + apiURL + ", Error: " + err.Error())
	}

	if err := collector.dbLoader.CreateTableByJsonStruct(TABLE_MS_TICKERS, reflect.TypeFor[Tickers]()); err != nil {
		return 0, err
	}
	return collector.LoadToDB(string(jsonText))
}

func (collector *MSCollector) LoadToDB(jsonText string) (int64, error) {
	var data TickersBody
	if err := json.Unmarshal([]byte(jsonText), &data); err != nil {
		return 0, errors.New("Failed to unmarshal json text, Error: " + err.Error())
	}
	dataJSONText, err := json.Marshal(data.Data)
	if err != nil {
		return 0, errors.New("Failed to marshal json struct, Error: " + err.Error())
	}

	numOfRows, err := collector.dbLoader.LoadByJsonText(string(dataJSONText), TABLE_MS_TICKERS, reflect.TypeFor[Tickers]())
	if err != nil {
		return 0, errors.New("Failed to load json text to table " + TABLE_MS_TICKERS + ". Error: " + err.Error())
	}
	collector.logger.Println(numOfRows, "rows were loaded into ", collector.dbSchema, ":"+TABLE_MS_TICKERS+" table")
	return numOfRows, nil

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

// Entry Function
func CollectTickers(schemaName string, fileJSON string) (int64, error) {
	dbLoader := dbloader.NewPGLoader(schemaName, &sdclogger.SDCLoggerInstance.Logger)
	dbLoader.Connect(os.Getenv("PGHOST"),
		os.Getenv("PGPORT"),
		os.Getenv("PGUSER"),
		os.Getenv("PGPASSWORD"),
		os.Getenv("PGDATABASE"))
	reader := NewHttpReader(NewLocalClient())
	collector := NewMSCollector(dbLoader, reader, &sdclogger.SDCLoggerInstance.Logger, schemaName)
	if len(fileJSON) > 0 {
		reader, err := os.OpenFile(fileJSON, os.O_RDONLY, 0666)
		if err != nil {
			return 0, errors.New("Failed to open file " + fileJSON)
		}

		textJSON, err := io.ReadAll(reader)
		if err != nil {
			return 0, errors.New("Failed to read file " + fileJSON)
		}

		if err := collector.dbLoader.CreateTableByJsonStruct(TABLE_MS_TICKERS, reflect.TypeFor[Tickers]()); err != nil {
			return 0, err
		}

		return collector.dbLoader.LoadByJsonText(string(textJSON), TABLE_MS_TICKERS, reflect.TypeFor[Tickers]())
	} else {
		return collector.CollectTickers()
	}
}
