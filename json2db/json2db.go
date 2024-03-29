package json2db

import "reflect"

type JsonObject map[string]interface{}

type JsonToSQLConverter interface {
	GenCreateSchema(schema string) string
	GenDropSchema(schema string) string
	GenCreateTableSQLByJson(jsonText string, tableName string) string
	GenCreateTableSQLByObj(obj JsonObject, tableName string) string
	GenInsertSQL(jsonText string, tableName string, jsonStructType reflect.Type) (string, [][]interface{}, error)
	GenBulkInsertSQLByJsonObjs(jsonObjs []JsonObject, tableName string) string
	FlattenJsonArrayText(jsonText string, rootTable string) map[string][]JsonObject
	FlattenJsonArrayObjs(jsonObjs []JsonObject, rootTable string) map[string][]JsonObject
}
