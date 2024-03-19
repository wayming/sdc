package json2db

type JsonObject map[string]interface{}

type JsonToSQLConverter interface {
	GenCreateTableSQLByJson(jsonText string, tableName string) string
	GenCreateTableSQLByObj(obj JsonObject, tableName string) string
	GenInsertSQL(jsonText string, tableName string) (string, [][]interface{})
	GenInsertSQLByJsonObjs(jsonObjs []JsonObject, tableName string) (string, [][]interface{})
	FlattenJsonArray(jsonText string, rootTable string) map[string][]JsonObject
	FlattenJsonArrayObjs(jsonObjs []JsonObject, rootTable string) map[string][]JsonObject
}
