package collector

import (
	"fmt"

	"github.com/wayming/sdc/dbloader"
	"github.com/wayming/sdc/sdclogger"
)

type IDataExporter interface {
}

type FileExporter struct {
}

func (e FileExporter) Export(entity string, data string) error {

}

type YFDBExporter struct {
	db     dbloader.DBLoader
	schema string
}

func NewYFDBExporter(db dbloader.DBLoader, schema string) *YFDBExporter {
	db.CreateSchema(schema)
	db.Exec("SET search_path TO " + schema)
	return &YFDBExporter{
		db:     db,
		schema: schema}
}

func (e YFDBExporter) Export(entity string, data string) error {
	if err := e.db.CreateTableByJsonStruct(FYDataTables[entity], FYDataTypes[entity]); err != nil {
		return err
	}

	numOfRows, err := e.db.LoadByJsonText(data, FYDataTables[entity], FYDataTypes[entity])
	if err != nil {
		return fmt.Errorf("Failed to load json text to table %s: %w", FYDataTables[entity], err)
	}
	sdclogger.SDCLoggerInstance.Printf("%d rows were loaded into %s:%s", numOfRows, c.dbSchema, FYDataTables[entity])
	return nil
}
