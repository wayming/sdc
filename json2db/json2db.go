package json2db

import "reflect"

type JsonToSQLConverter interface {
	GenCreateSchema(schema string) string
	GenDropSchema(schema string) string
	GenCreateTable(tableName string, responseType reflect.Type) (string, error)
	GenInsertSQL(jsonText string, tableName string, entityStructType reflect.Type) (string, [][]interface{}, error)
	GenBulkInsertSQL(jsonText string, tableName string, entityStructType reflect.Type) (string, error)
}
