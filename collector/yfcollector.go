package collector

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"reflect"
	"strings"

	"github.com/wayming/sdc/config"
	"github.com/wayming/sdc/dbloader"
	"github.com/wayming/sdc/sdclogger"
)

type YFCollector struct {
	reader    IHttpReader
	exporters IDataExporter
	db        dbloader.DBLoader
	logger    *log.Logger
}

func NewYFCollector(httpReader IHttpReader, exporters IDataExporter, db dbloader.DBLoader, l *log.Logger) *YFCollector {
	logger := l
	if logger == nil {
		logger = sdclogger.SDCLoggerInstance.Logger
	}
	return &YFCollector{
		reader:    httpReader,
		exporters: exporters,
		db:        db,
		logger:    logger,
	}
}

func (c *YFCollector) Tickers() error {
	apiURL := "http://openbb:8001/api/v1/equity/search?provider=nasdaq&is_symbol=false&use_cache=true&active=true&limit=100000&is_fund=false"
	textJSON, err := c.reader.Read(apiURL, nil)
	if err != nil {
		return fmt.Errorf("failed to load data from %s: %v ", apiURL, err)
	}
	textJSON = strings.ReplaceAll(textJSON, "`", "")
	dataText, err := ExtractData(textJSON, reflect.TypeFor[FYTickersResponse]())
	if err != nil {
		return err
	}

	if err := c.exporters.Export(YF_TICKERS, strings.ToLower(YFDataTables[YF_TICKERS]), dataText); err != nil {
		return err
	}

	return nil
}

func (c *YFCollector) EODForSymbol(symbol string) error {
	baseURL := "http://openbb:8001/api/v1/equity/price/historical"
	params := map[string]string{
		"chart":           "false",
		"provider":        "yfinance",
		"interval":        "1d",
		"start_date":      "2000-01-01",
		"adjustment":      "splits_only",
		"extended_hours":  "false",
		"adjusted":        "false",
		"use_cache":       "false",
		"timezone":        "America/New_York",
		"source":          "realtime",
		"sort":            "asc",
		"limit":           "49999",
		"include_actions": "true",
		"prepost":         "false",
	}

	c.logger.Println("Load EDO for symbool", symbol)

	params["symbol"] = symbol
	textJSON, err := c.reader.Read(baseURL, params)
	if err != nil {
		if serverError, ok := err.(HttpServerError); ok {
			if serverError.status == http.StatusBadRequest {
				c.logger.Printf("No data found for %s, continue processing.", symbol)
				return nil
			}
		}
		return errors.New("Failed to load data from url " + baseURL + ", Error: " + err.Error())
	}
	c.logger.Printf("EOD received:\n%s", textJSON)

	dataText, err := ExtractData(textJSON, reflect.TypeFor[YFEODResponse]())
	if err != nil {
		return err
	}

	if len(dataText) > 0 {
		tableName := strings.ToLower(YFDataTables[YF_EOD] + "_" + symbol)
		if err := c.exporters.Export(YF_EOD, tableName, dataText); err != nil {
			return err
		}
		c.logger.Printf("Successfully loaded EOD rows to %s", tableName)
	} else {
		c.logger.Printf("No data found for %s", symbol)
	}

	return nil
}

func (c *YFCollector) EOD() error {
	type queryResult struct {
		Symbol string
	}

	sql := "select symbol from " + YFDataTables[YF_TICKERS] + " limit 10"
	results, err := c.db.RunQuery(sql, reflect.TypeFor[queryResult]())
	if err != nil {
		return errors.New("Failed to run query [" + sql + "]. Error: " + err.Error())
	}
	queryResults, ok := results.([]queryResult)
	if !ok {
		return errors.New("failed to assert the slice of queryResults")
	} else {
		c.logger.Printf("%d symbols retrieved from table %s", len(queryResults), YFDataTables[YF_TICKERS])
	}

	for _, row := range queryResults {
		if err := c.EODForSymbol(row.Symbol); err != nil {
			return err
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
func YFCollect(fileJSON string, loadTickers bool, loadEOD bool) error {
	db := dbloader.NewPGLoader(config.SchemaName, sdclogger.SDCLoggerInstance.Logger)
	db.Connect(os.Getenv("PGHOST"),
		os.Getenv("PGPORT"),
		os.Getenv("PGUSER"),
		os.Getenv("PGPASSWORD"),
		os.Getenv("PGDATABASE"))

	reader := NewHttpReader(NewLocalClient())

	var exporters YFDataExporters
	exporters.AddExporter(NewDBExporter(db, config.SchemaName))
	exporters.AddExporter(NewYFFileExporter())

	cl := NewYFCollector(reader, &exporters, db, sdclogger.SDCLoggerInstance.Logger)

	if loadTickers {
		if len(fileJSON) > 0 {
			reader, err := os.OpenFile(fileJSON, os.O_RDONLY, 0666)
			if err != nil {
				return errors.New("Failed to open file " + fileJSON)
			}

			text, err := io.ReadAll(reader)
			if err != nil {
				return errors.New("Failed to read file " + fileJSON)
			}

			if err := exporters.Export(YF_EOD, path.Base(fileJSON), string(text)); err != nil {
				return err
			}
		} else {
			if err := cl.Tickers(); err != nil {
				return err
			}
		}
	}

	if loadEOD {
		if err := cl.EOD(); err != nil {
			return err
		}
	}

	return nil
}
