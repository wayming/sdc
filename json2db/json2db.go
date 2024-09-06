package json2db

import "reflect"

type JsonToSQLConverter interface {
	GenCreateSchema(schema string) string
	GenDropSchema(schema string) string
	GenCreateTable(tableName string, responseType reflect.Type) (string, error)
	SQLData(jsonText string, tableName string, jsonStructType reflect.Type) ([]string, [][]interface{}, error)
	GenInsertSQL(jsonText string, tableName string, jsonStructType reflect.Type) (string, [][]interface{}, error)
}
