package collector

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"

	"github.com/wayming/sdc/dbloader"
	"github.com/wayming/sdc/sdclogger"
)

type YFCollector struct {
	reader    IHttpReader
	exporters IDataExporter
	db        dbloader.DBLoader
}

func NewYFCollector(httpReader IHttpReader, exporters IDataExporter, db dbloader.DBLoader) *YFCollector {
	return &YFCollector{
		reader:    httpReader,
		exporters: exporters,
		db:        db,
	}
}

func (c *YFCollector) Tickers() error {
	apiURL := "http://openbb:8001/api/v1/equity/search?provider=nasdaq&is_symbol=false&use_cache=true&active=true&limit=100000&is_fund=false"
	textJSON, err := c.reader.Read(apiURL, nil)
	if err != nil {
		return fmt.Errorf("failed to load data from %s: %v ", apiURL, err)
	}
	textJSON = strings.ReplaceAll(textJSON, "`", "")
	// textJSON = `{
	// 	"results": [
	// 		{
	// 			"symbol": "A",
	// 			"name": "Agilent Technologies, Inc. Common Stock",
	// 			"nasdaq_traded": "Y",
	// 			"exchange": "N",
	// 			"market_category": null,
	// 			"etf": "N",
	// 			"round_lot_size": 100,
	// 			"test_issue": "N",
	// 			"financial_status": null,
	// 			"cqs_symbol": "A",
	// 			"nasdaq_symbol": "A",
	// 			"next_shares": "N"
	// 		}
	// 	],
	// 	"provider": "nasdaq",
	// 	"warnings": [
	// 		{
	// 			"category": "OpenBBWarning",
	// 			"message": "Parameter 'limit' is not supported by nasdaq. Available for: intrinio."
	// 		},
	// 		{
	// 			"category": "FutureWarning",
	// 			"message": "Downcasting object dtype arrays on .fillna, .ffill, .bfill is deprecated and will change in a future version. Call result.infer_objects(copy=False) instead. To opt-in to the future behavior, set "
	// 		}
	// 	],
	// 	"chart": null,
	// 	"extra": {
	// 		"metadata": {
	// 			"arguments": {
	// 				"provider_choices": {
	// 					"provider": "nasdaq"
	// 				},
	// 				"standard_params": {
	// 					"query": "",
	// 					"is_symbol": false,
	// 					"use_cache": true
	// 				},
	// 				"extra_params": {
	// 					"active": true,
	// 					"limit": 100000,
	// 					"is_etf": null,
	// 					"is_fund": false
	// 				}
	// 			},
	// 			"duration": 4196819148,
	// 			"route": "/equity/search",
	// 			"timestamp": "2024-07-30T12:56:09.154604"
	// 		}
	// 	}
	// }`
	dataText, err := ExtractData(textJSON, reflect.TypeFor[FYTickersResponse]())
	if err != nil {
		return err
	}

	if err := c.exporters.Export(FY_TICKERS, dataText); err != nil {
		return err
	}

	return nil
}

func (c *YFCollector) EOD() error {
	type queryResult struct {
		Symbol string
	}

	// /http://localhost:8001/api/v1/equity/price/historical?chart=false&provider=yfinance&symbol=MSFT&interval=1d&adjustment=splits_only&extended_hours=false&adjusted=false&use_cache=true&timezone=America%2FNew_York&source=realtime&sort=asc&limit=100000&include_actions=true&prepost=false
	apiURL := "http://openbb:8001/api/v1/equity/price/historical?chart=false&provider=yfinance&interval=1d&adjustment=splits_only&extended_hours=false&adjusted=false&use_cache=true&timezone=America%2FNew_York&source=realtime&sort=asc&limit=100000&include_actions=true&prepost=false"

	sql := "select symbol from " + FYDataTables[FY_EOD] + " limit 20"
	results, err := c.db.RunQuery(sql, reflect.TypeFor[queryResult]())
	if err != nil {
		return errors.New("Failed to run query [" + sql + "]. Error: " + err.Error())
	}
	queryResults, ok := results.([]queryResult)
	if !ok {
		return errors.New("failed to run assert the query results are returned as a slice of queryResults")

	}

	for _, row := range queryResults {
		sdclogger.SDCLoggerInstance.Println("Load EDO for symbool", row.Symbol)
		textJSON, err := c.reader.Read(apiURL, map[string]string{"symbols": row.Symbol})
		if err != nil {
			return errors.New("Failed to load data from url " + apiURL + ", Error: " + err.Error())
		}

		dataText, err := ExtractData(textJSON, reflect.TypeFor[FYEODBody]())
		if err != nil {
			return err
		}

		if len(dataText) > 0 {
			if err := c.exporters.Export(FY_EOD, dataText); err != nil {
				return err
			}
			sdclogger.SDCLoggerInstance.Printf("Loaded EOD rows to %s", FYDataTables[FY_EOD])
		} else {
			sdclogger.SDCLoggerInstance.Printf("No data found for %s", row.Symbol)
		}

	}

	return nil
}

func ExtractData(textJSON string, t reflect.Type) (string, error) {
	structJSON := reflect.New(t).Elem()
	if err := json.Unmarshal([]byte(textJSON), structJSON.Addr().Interface()); err != nil {
		return "", errors.New("Failed to unmarshal json text, Error: " + err.Error())
	}

	resultsField := structJSON.FieldByName("Results")
	if !resultsField.IsValid() {
		return "", errors.New("field 'Results' does not exist in the struct")
	}

	resultsText, err := json.Marshal(resultsField.Interface())
	if err != nil {
		return "", errors.New("Failed to marshal json struct, Error: " + err.Error())
	}

	return string(resultsText), nil
}

// Entry Function
func YFCollect(schemaName string, csvFile string) error {
	db := dbloader.NewPGLoader(schemaName, &sdclogger.SDCLoggerInstance.Logger)
	db.Connect(os.Getenv("PGHOST"),
		os.Getenv("PGPORT"),
		os.Getenv("PGUSER"),
		os.Getenv("PGPASSWORD"),
		os.Getenv("PGDATABASE"))

	reader := NewHttpReader(NewLocalClient())

	var exports YFDataExporter
	exports.AddExporter(NewYFFileExporter())
	exports.AddExporter(NewYFDBExporter(db, schemaName))

	cl := NewYFCollector(reader, &exports, db)

	if len(csvFile) > 0 {
		reader, err := os.OpenFile(csvFile, os.O_RDONLY, 0666)
		if err != nil {
			return errors.New("Failed to open file " + csvFile)
		}

		text, err := io.ReadAll(reader)
		if err != nil {
			return errors.New("Failed to read file " + csvFile)
		}

		if err := exports.Export(FY_EOD, string(text)); err != nil {
			return err
		}

		if err := cl.EOD(); err != nil {
			return err
		}
	} else {
		if err := cl.Tickers(); err != nil {
			return err
		}
		if err := cl.EOD(); err != nil {
			return err
		}
	}
	return nil
}
