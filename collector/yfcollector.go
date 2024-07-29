package collector

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"

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
	apiURL := "http://localhost:8001/api/v1/equity/search?provider=nasdaq&is_symbol=false&use_cache=true&active=true&limit=100000&is_fund=false"
	jsonText, err := c.reader.Read(apiURL, nil)
	if err != nil {
		return fmt.Errorf("Failed to load data from %s: %v ", apiURL, err)
	}

	if err := c.exporters.Export(FY_TICKERS, jsonText); err != nil {
		return err
	}

	return nil
}

func (c *YFCollector) EOD() error {
	type queryResult struct {
		Symbol string
	}

	// /http://localhost:8001/api/v1/equity/price/historical?chart=false&provider=yfinance&symbol=MSFT&interval=1d&adjustment=splits_only&extended_hours=false&adjusted=false&use_cache=true&timezone=America%2FNew_York&source=realtime&sort=asc&limit=100000&include_actions=true&prepost=false
	apiURL := "http://localhost:8001/api/v1/equity/price/historical?chart=false&provider=yfinance&interval=1d&adjustment=splits_only&extended_hours=false&adjusted=false&use_cache=true&timezone=America%2FNew_York&source=realtime&sort=asc&limit=100000&include_actions=true&prepost=false"

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
		jsonText, err := c.reader.Read(apiURL, map[string]string{"symbols": row.Symbol})
		if err != nil {
			return errors.New("Failed to load data from url " + apiURL + ", Error: " + err.Error())
		}
		var body FYEODBody
		if err := json.Unmarshal([]byte(jsonText), &body); err != nil {
			return errors.New("Failed to unmarshal json text, Error: " + err.Error())
		}

		if len(body.Data) > 0 {

			eodsText, err := json.Marshal(body.Data)
			if err != nil {
				return errors.New("Failed to marshal json struct, Error: " + err.Error())
			}

			if err := c.exporters.Export(FY_EOD, string(eodsText)); err != nil {
				return err
			}
			sdclogger.SDCLoggerInstance.Printf("Loaded EOD rows to %s", FYDataTables[FY_EOD])
		} else {
			sdclogger.SDCLoggerInstance.Printf("No data found for %s", row.Symbol)
		}

	}

	return nil
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
