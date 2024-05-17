package dbloader

import "reflect"

type DBLoader interface {
	Connect(host string, port string, user string, password string, dbname string)
	Disconnect()
	CreateSchema(schema string)
	DropSchema(schema string)
	RunQuery(sql string, structType reflect.Type, args ...any) (interface{}, error)
	Exec(sql string) error
	LoadByJsonText(jsonText string, tableName string, jsonStructType reflect.Type) (int64, error)
	CreateTableByJsonStruct(tableName string, jsonStructType reflect.Type) error
}
