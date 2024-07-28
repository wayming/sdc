package collector

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"reflect"

	"github.com/wayming/sdc/dbloader"
	"github.com/wayming/sdc/sdclogger"
)

type YFCollector struct {
	dbLoader    dbloader.DBLoader
	reader      HttpReader
	logger      *log.Logger
	dbSchema    string
	msAccessKey string
}

func NewYFCollector(loader dbloader.DBLoader, httpReader HttpReader, logger *log.Logger, schema string) *YFCollector {
	loader.CreateSchema(schema)
	loader.Exec("SET search_path TO " + schema)
	return &YFCollector{
		dbLoader:    loader,
		logger:      logger,
		dbSchema:    schema,
		msAccessKey: os.Getenv("MSACCESSKEY"),
	}
	return &collector
}

func (c *YFCollector) Tickers() (int64, error) {
	apiURL := "http://localhost:8001/api/v1/equity/search?provider=nasdaq&is_symbol=false&use_cache=true&active=true&limit=100000&is_fund=false"
	jsonText, err := c.reader.Read(apiURL, nil)
	if err != nil {
		return 0, fmt.Errorf("Failed to load data from %s: %v ", apiURL, err)
	}

	if err := c.dbLoader.CreateTableByJsonStruct(TABLE_MS_TICKERS, reflect.TypeFor[Tickers]()); err != nil {
		return 0, err
	}
	return c.toDB(string(jsonText))
}

func (c *YFCollector) EOD() error {
	type queryResult struct {
		Symbol string
	}

	apiURL := "http://api.marketstack.com/v1/eod"
	eodTable := "ms_eod"

	sqlQuerySymbol := "select symbol from " + c.dbSchema + "." + "ms_tickers limit 20"
	results, err := c.dbLoader.RunQuery(sqlQuerySymbol, reflect.TypeFor[queryResult]())
	if err != nil {
		return errors.New("Failed to run query [" + sqlQuerySymbol + "]. Error: " + err.Error())
	}
	queryResults, ok := results.([]queryResult)
	if !ok {
		return errors.New("failed to run assert the query results are returned as a slice of queryResults")

	}

	for _, row := range queryResults {
		c.logger.Println("Load EDO for symbool", row.Symbol)
		jsonText, err := c.reader.Read(apiURL, map[string]string{"symbols": row.Symbol})
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
			numOfRows, err := c.dbLoader.LoadByJsonText(string(dataJSONText), eodTable, reflect.TypeOf(eod))
			if err != nil {
				return errors.New("Failed to load json text to table " + eodTable + ". Error: " + err.Error())
			}
			c.logger.Println(numOfRows, "rows were loaded into ", c.dbSchema, ":"+eodTable+" table")
		} else {
			c.logger.Println("No data found for symbol", row.Symbol)
		}

	}

	return nil
}

func (c *YFCollector) toDB(jsonText string) (int64, error) {
	var data TickersBody
	if err := json.Unmarshal([]byte(jsonText), &data); err != nil {
		return 0, errors.New("Failed to unmarshal json text, Error: " + err.Error())
	}
	dataJSONText, err := json.Marshal(data.Data)
	if err != nil {
		return 0, errors.New("Failed to marshal json struct, Error: " + err.Error())
	}

	numOfRows, err := c.dbLoader.LoadByJsonText(string(dataJSONText), TABLE_MS_TICKERS, reflect.TypeFor[Tickers]())
	if err != nil {
		return 0, errors.New("Failed to load json text to table " + TABLE_MS_TICKERS + ". Error: " + err.Error())
	}
	c.logger.Println(numOfRows, "rows were loaded into ", c.dbSchema, ":"+TABLE_MS_TICKERS+" table")
	return numOfRows, nil

}

// Entry Function
func CollectTickers(schemaName string, csvFile string) (int64, error) {
	dbLoader := dbloader.NewPGLoader(schemaName, &sdclogger.SDCLoggerInstance.Logger)
	dbLoader.Connect(os.Getenv("PGHOST"),
		os.Getenv("PGPORT"),
		os.Getenv("PGUSER"),
		os.Getenv("PGPASSWORD"),
		os.Getenv("PGDATABASE"))
	reader := NewHttpDirectReader()
	collector := NewYFCollector(dbLoader, reader, &sdclogger.SDCLoggerInstance.Logger, schemaName)
	if len(csvFile) > 0 {
		reader, err := os.OpenFile(csvFile, os.O_RDONLY, 0666)
		if err != nil {
			return 0, errors.New("Failed to open file " + csvFile)
		}

		csv, err := io.ReadAll(reader)
		if err != nil {
			return 0, errors.New("Failed to read file " + csvFile)
		}

		if err := collector.dbLoader.CreateTableByJsonStruct(TABLE_MS_TICKERS, reflect.TypeFor[Tickers]()); err != nil {
			return 0, err
		}

		return collector.LoadTickers(string(csv))
	} else {
		return collector.CollectTickers()
	}
}
