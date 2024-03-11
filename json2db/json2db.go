package json2db

type JsonToSQLConverter interface {
	CreateTable(string, string) []string
}
