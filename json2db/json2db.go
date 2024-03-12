package json2db

type JsonToSQLConverter interface {
	CreateTableSQL(string, string) []string
}
