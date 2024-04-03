package json2db

import "reflect"

type JsonObject map[string]interface{}

type JsonToSQLConverter interface {
	GenCreateSchema(schema string) string
	GenDropSchema(schema string) string
	GenCreateTable(tableName string, responseType reflect.Type) (string, error)
	GenBulkInsert(jsonText string, tableName string, jsonStructType reflect.Type) ([]string, [][]interface{}, error)
	GenInsert(jsonText string, tableName string, jsonStructType reflect.Type) (string, [][]interface{}, error)
}
