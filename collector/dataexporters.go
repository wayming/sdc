package collector

import (
	"fmt"
	"os"
	"reflect"

	"github.com/wayming/sdc/dbloader"
	"github.com/wayming/sdc/sdclogger"
)

type IDataExporter interface {
	Export(entityType reflect.Type, table string, data string, symbol string) error
}

type FileExporter struct {
	path string
}

func NewYFFileExporter() *FileExporter {
	dir := os.Getenv("SDC_HOME") + "/data/YF"
	if err := os.MkdirAll(dir, 0755); err != nil {
		sdclogger.SDCLoggerInstance.Fatalf("Failed to create directory %s: %v", dir, err)
	}
	return &FileExporter{path: dir}
}

func NewMSFileExporter() *FileExporter {
	dir := os.Getenv("SDC_HOME") + "/data/MS"
	if err := os.MkdirAll(dir, 0755); err != nil {
		sdclogger.SDCLoggerInstance.Fatalf("Failed to create directory %s: %v", dir, err)
	}
	return &FileExporter{path: dir}
}

func NewSAFileExporter() *FileExporter {
	dir := os.Getenv("SDC_HOME") + "/data/SA"
	if err := os.MkdirAll(dir, 0755); err != nil {
		sdclogger.SDCLoggerInstance.Fatalf("Failed to create directory %s: %v", dir, err)
	}
	return &FileExporter{path: dir}
}

func (e FileExporter) Export(entityType reflect.Type, table string, data string, symbol string) error {
	dir := e.path + "/"
	if len(symbol) > 0 {
		dir += symbol
		if err := os.MkdirAll(dir, 0755); err != nil {
			sdclogger.SDCLoggerInstance.Fatalf("Failed to create directory %s: %v", dir, err)
		}
	}

	fileName := dir + "/" + table + ".json"
	if err := os.WriteFile(fileName, []byte(data), 0644); err != nil {
		sdclogger.SDCLoggerInstance.Fatalf("Failed to write to file %s: %v", fileName, err)
	}
	return nil
}

type DBExporter struct {
	db     dbloader.DBLoader
	schema string
}

func NewDBExporter(db dbloader.DBLoader, schema string) *DBExporter {
	db.CreateSchema(schema)
	db.Exec("SET search_path TO " + schema)
	return &DBExporter{
		db:     db,
		schema: schema}
}

func (e DBExporter) Export(entityType reflect.Type, table string, data string, symbol string) error {
	numOfRows, err := e.db.LoadByJsonText(data, table, entityType)
	if err != nil {
		return fmt.Errorf("failed to load json text to table %s: %v", table, err)
	}
	sdclogger.SDCLoggerInstance.Printf("%d rows were loaded into %s:%s", numOfRows, e.schema, table)
	return nil
}

type DataExporters struct {
	exporters []IDataExporter
}

func (e *DataExporters) AddExporter(exp IDataExporter) {
	e.exporters = append(e.exporters, exp)
}
func (e *DataExporters) Export(entityType reflect.Type, table string, data string, symbol string) error {
	for _, exporter := range e.exporters {
		if err := exporter.Export(entityType, table, data, symbol); err != nil {
			return err
		}
	}
	return nil
}
