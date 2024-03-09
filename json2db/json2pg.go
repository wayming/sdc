package json2db

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"
)

const MAX_CHAR_SIZE = 1024

type PGDDLGenrator struct {
}

func NewPGDDLGenrator() *PGDDLGenrator {
	log.SetFlags(log.Ldate | log.Ltime)
	return &PGDDLGenrator{}
}

func (d *PGDDLGenrator) Do(jsonText string, tableName string) string {
	var data map[string]interface{}
	ddl := "CREATE TABLE " + tableName + "("

	err := json.Unmarshal([]byte(jsonText), &data)
	if err != nil {
		log.Fatal("Failed to parse json string ", jsonText, ", error ", err)
		return ""
	}
	log.Println("Parse results ", data)

	for key, value := range data {
		colType, err := d.deriveColType(value)
		if err != nil {
			log.Fatal("Failed to derive type for ", value, ", error ", err)
			return ""
		}
		ddl += key + " " + colType + ", "
	}

	ddlS := ddl[:len(ddl)-1]
	return ddlS + ")"
}

func (d *PGDDLGenrator) deriveColType(value interface{}) (string, error) {
	var err error
	var colType string
	switch v := value.(type) {
	case int:
		colType = "integer"
	case float64:
		colType = "double"
	case bool:
		colType = "boolean"
	case time.Time:
		colType = "timestamp"
	case map[string]interface{}:
		colType = "text"
	case string:
		if len(v) <= MAX_CHAR_SIZE {
			colType = "vchar(" + fmt.Sprint(MAX_CHAR_SIZE) + ")"
		} else {
			colType = "text"
		}
	case nil:
		colType = "text"
	default:
		err = errors.New("unknown type")
	}

	return colType, err
}
