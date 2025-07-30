package collector

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"reflect"
	"regexp"
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
	apiURL := "http://openbb:8001/api/v1/equity/search?provider=nasdaq&is_symbol=true&use_cache=true&active=true&is_etf=false&is_fund=false"

	textJSON, err := c.reader.Read(apiURL, nil)
	if err != nil {
		return fmt.Errorf("failed to load data from %s: %v ", apiURL, err)
	}
	textJSON = strings.ReplaceAll(textJSON, "`", "")
	dataText, err := ExtractData(textJSON, reflect.TypeFor[YFTickersResponse]())
	if err != nil {
		return err
	}

	dataText, err = FilterSymbolVariations(dataText)
	if err != nil {
		return err
	}

	if err := c.db.CreateTableByJsonStruct(YFDataTables[YF_TICKERS], YFDataTypes[YF_TICKERS]); err != nil {
		return err
	}

	if err := c.exporters.Export(YFDataTypes[YF_TICKERS], strings.ToLower(YFDataTables[YF_TICKERS]), dataText, ""); err != nil {
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
		if err := c.db.CreateTableByJsonStruct(tableName, YFDataTypes[YF_EOD]); err != nil {
			return err
		}

		if err := c.exporters.Export(YFDataTypes[YF_EOD], tableName, dataText, symbol); err != nil {
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

func FilterSymbolVariations(textJSON string) (string, error) {
	var tickers []YFTickers
	if err := json.Unmarshal([]byte(textJSON), &tickers); err != nil {
		return "", errors.New("Failed to unmarshal json text, Error: " + err.Error())
	}

	var filtered []YFTickers
	symbolPattern := `\.|\$`
	reSymbol := regexp.MustCompile(symbolPattern)
	namePattern := `- Warrants`
	reName := regexp.MustCompile(namePattern)

	for _, ticker := range tickers {
		matchSymbol := reSymbol.FindString(ticker.Symbol)
		matchName := reName.FindString(ticker.Name)
		if len(matchSymbol) == 0 && len(matchName) == 0 {
			filtered = append(filtered, ticker)
		}
	}

	results, err := json.Marshal(filtered)
	if err != nil {
		return "", errors.New("Failed to marshal json struct, Error: " + err.Error())
	}

	return string(results), nil
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
	var yfExporters DataExporters
	yfExporters.AddExporter(NewDBExporter(db, config.SchemaName))

	if loadTickers && len(fileJSON) > 0 {

		fileReader, err := os.OpenFile(fileJSON, os.O_RDONLY, 0666)
		if err != nil {
			return errors.New("Failed to open file " + fileJSON)
		}

		// Read in all tickers
		text, err := io.ReadAll(fileReader)
		if err != nil {
			return errors.New("Failed to read file " + fileJSON)
		}

		// Filter symbol variations
		textFiltered, err := FilterSymbolVariations(string(text))
		if err != nil {
			return fmt.Errorf("failed filter symbol variations, error %v", err)
		}

		db.CreateTableByJsonStruct(YFDataTables[YF_TICKERS], YFDataTypes[YF_TICKERS])
		if err := yfExporters.Export(YFDataTypes[YF_TICKERS], YFDataTables[YF_TICKERS], textFiltered, ""); err != nil {
			return err
		}
		return nil
	}

	cl := NewYFCollector(reader, &yfExporters, db, sdclogger.SDCLoggerInstance.Logger)
	yfExporters.AddExporter(NewYFFileExporter())
	if loadTickers {
		if err := cl.Tickers(); err != nil {
			return err
		}

	}
	if loadEOD {
		if err := cl.EOD(); err != nil {
			return err
		}
	}

	return nil
}
