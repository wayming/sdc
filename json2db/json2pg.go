package json2db

import (
	"encoding/json"
	"log"
)

type DDLGenPG struct {
}

func NewDDLGenPG() *DDLGenPG {
	log.SetFlags(log.Ldate | log.Ltime)
	return &DDLGenPG{}
}

func (d *DDLGenPG) Gen(jsonText string) string {
	var data map[string]interface{}
	var ddl string
	err := json.Unmarshal([]byte(jsonText), &data)
	if err != nil {
		log.Fatal("Failed to parse json string ", jsonText, ", error ", err)
	} else {
		log.Println("Parse results ", data)
	}
	return ddl
}
