package json2db

import {
	"fmt"
}

type DDLGen interface {
	Gen(string) string
}