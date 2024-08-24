package collector

import (
	"fmt"
	"os"

	"github.com/wayming/sdc/dbloader"
	"github.com/wayming/sdc/sdclogger"
)

type IDataExporter interface {
	Export(entity string, table string, data string) error
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

func (e FileExporter) Export(entity string, table string, data string) error {
	fileName := e.path + "/" + table + ".json"
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

func (e DBExporter) Export(entity string, table string, data string) error {
	if err := e.db.CreateTableByJsonStruct(table, YFDataTypes[entity]); err != nil {
		return err
	}

	numOfRows, err := e.db.LoadByJsonText(data, table, YFDataTypes[entity])
	if err != nil {
		return fmt.Errorf("failed to load json text to table %s: %v", table, err)
	}
	sdclogger.SDCLoggerInstance.Printf("%d rows were loaded into %s:%s", numOfRows, e.schema, table)
	return nil
}

type CommonDataExporters struct {
	exporters []IDataExporter
}

func (e *CommonDataExporters) AddExporter(exp IDataExporter) {
	e.exporters = append(e.exporters, exp)
}
func (e *CommonDataExporters) Export(entity string, table string, data string) error {
	for _, exporter := range e.exporters {
		if err := exporter.Export(entity, table, data); err != nil {
			return err
		}
	}
	return nil
}

type YFDataExporters struct {
	CommonDataExporters
}

type MSDataExporters struct {
	CommonDataExporters
}
