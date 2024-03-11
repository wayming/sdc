package json2db

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"time"
)

const MAX_CHAR_SIZE = 1024

type JsonToPGSQLConverter struct {
	tableFieldsMap map[string][]string
}

func NewJsonToPGSQLConverter() *JsonToPGSQLConverter {
	log.SetFlags(log.Ldate | log.Ltime)
	return &JsonToPGSQLConverter{}
}

func (d *JsonToPGSQLConverter) CreateTable(jsonText string, tableName string) []string {
	var data map[string]interface{}
	var ddls []string
	mainDDL := "CREATE TABLE " + tableName + " ("

	err := json.Unmarshal([]byte(jsonText), &data)
	if err != nil {
		log.Fatal("Failed to parse json string ", jsonText, ", error ", err)
		return ddls
	}
	log.Println("Parse results ", data)

	fields := d.tableFieldsMap[tableName]
	for _, key := range orderedKeys(data) {
		fields = append(fields, key)
		value := data[key]
		colType, err := d.deriveColType(value)
		if err != nil {
			log.Fatal("Failed to derive type for ", value, ", error ", err)
			return ddls
		}

		if colType == "table" {
			subTable, ok := value.(map[string]interface{})
			if ok {
				if _, ok = subTable["name"]; ok {
					subJSON, err := json.Marshal(subTable)
					if err == nil {
						ddls = append(ddls, d.CreateTable(string(subJSON), "sdc_"+key)...)
					} else {
						log.Fatal("Failed to marshal ", subTable, " to JSON, error ", err)
					}
				} else {
					log.Fatal("Failed to find the [name] key from the map ", subTable)
				}
			} else {
				log.Fatal("Failed to convert value ", value, " to map[string]interface{}")
			}
			key = key + "_name"
			colType = "string"
		}
		mainDDL += key + " " + colType + ", "
	}

	ddls = append(ddls, mainDDL[:len(mainDDL)-2]+");")
	return ddls
}

func (d *JsonToPGSQLConverter) InsertRows(jsonText string, tableName string) []string {
	var sqls []string
	sql := "INSERT INTO " + tableName + "(" + strings.Join(d.tableFieldsMap[tableName], ", ") + ")" + " VALUES ("

	var data []map[string]interface{}
	err := json.Unmarshal([]byte(jsonText), &data)
	if err != nil {
		log.Fatal("Failed to parse json string ", jsonText, ", error ", err)
		return sqls
	}
	for index := range d.tableFieldsMap[tableName] {
		if index > 0 {
			sql = sql + ", "
		}
		sql = sql + "$" + strconv.Itoa(index+1)
	}
	sql = sql + ")"
	log.Println("Parse results ", data)
	// for _, row := data {
	// 	var rowValues string
	// 	for _, field := d.tableFieldsMap[tableName] {
	// 		if len(rowValues) > 0 {
	// 			rowValue = rowValue + ", " + row
	// 		}

	// 	}
	// }
	sqls = append(sqls, sql)
	return sqls
}

func (d *JsonToPGSQLConverter) deriveColType(value interface{}) (string, error) {
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
		colType = "table"
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

func orderedKeys(m map[string]interface{}) []string {
	var keys []string
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
