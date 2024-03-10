package json2db

type DDLGenerator interface {
	do(string) []string
}
