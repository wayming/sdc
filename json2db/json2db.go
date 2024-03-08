package json2db

type DDLGen interface {
	Gen(string) string
}
