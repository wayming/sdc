package json2db

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
)

const MAX_CHAR_SIZE = 1024

type JsonToPGSQLConverter struct {
	schema         string
	tableFieldsMap map[string][]string
}

func NewJsonToPGSQLConverter() *JsonToPGSQLConverter {
	log.SetFlags(log.Ldate | log.Ltime)
	return &JsonToPGSQLConverter{tableFieldsMap: make(map[string][]string)}
}

func (d *JsonToPGSQLConverter) GenCreateSchema(schema string) string {
	d.schema = schema
	sql := "CREATE SCHEMA IF NOT EXISTS " + schema
	return sql
}

func (d *JsonToPGSQLConverter) GenDropSchema(schema string) string {
	d.schema = schema
	sql := "DROP SCHEMA IF EXISTS " + schema + " CASCADE"
	return sql
}

// Assume a flat json text string
func (d *JsonToPGSQLConverter) GenCreateTableSQLByJson(jsonText string, tableName string) string {
	var obj JsonObject
	err := json.Unmarshal([]byte(jsonText), &obj)
	if err != nil {
		log.Fatal("Failed to parse json string ", jsonText, ", error ", err)
	}
	return d.GenCreateTableSQLByObj(obj, tableName)
}

func (d *JsonToPGSQLConverter) GenCreateTableSQLByObj(obj JsonObject, tableName string) string {
	ddl := "CREATE TABLE IF NOT EXISTS " + tableName + " ("
	for _, key := range orderedKeys(obj) {
		if _, ok := d.tableFieldsMap[tableName]; !ok {
			d.tableFieldsMap[tableName] = make([]string, 0)
		}
		d.tableFieldsMap[tableName] = append(d.tableFieldsMap[tableName], key)
		value := obj[key]
		colType, err := d.deriveColType(value)
		if err != nil {
			log.Fatal("Failed to derive type for ", value, ", error ", err)
			return ""
		}
		ddl += key + " " + colType + ", "
	}
	ddl = ddl[:len(ddl)-2] + ");"

	return ddl
}

// Assume a flat json text string for the same table
func (d *JsonToPGSQLConverter) GenInsertSQL(jsonText string, tableName string) (string, [][]interface{}) {
	var objs []JsonObject
	err := json.Unmarshal([]byte(jsonText), &objs)
	if err != nil {
		log.Fatal("Failed to parse json string ", jsonText, ", error ", err)
	}

	return d.GenInsertSQLByJsonObjs(objs, tableName)
}

func (d *JsonToPGSQLConverter) GenInsertSQLByJsonObjs(jsonObjs []JsonObject, tableName string) (string, [][]interface{}) {
	// Generage SQL
	fields := d.tableFieldsMap[tableName]
	sql := "INSERT INTO " + tableName + " (" + strings.Join(fields, ", ") + ") VALUES ("
	for index := range fields {
		if index > 0 {
			sql = sql + ", "
		}
		sql = sql + "$" + strconv.Itoa(index+1)
	}
	sql = sql + ") ON CONFLICT DO NOTHING"

	// Generate Bind Variables
	var bindVars [][]interface{}
	for _, obj := range jsonObjs {
		var bindVarsForObj []interface{}
		for _, field := range fields {
			bindVarsForObj = append(bindVarsForObj, obj[field])
		}
		bindVars = append(bindVars, bindVarsForObj)
	}

	return sql, bindVars
}

func (d *JsonToPGSQLConverter) GenBulkInsertSQLByJsonObjs(jsonObjs []JsonObject, tableName string) string {
	// Generage SQL
	fields := d.tableFieldsMap[tableName]
	tableFileName := tableName + ".csv"
	sql := "COPY " + tableName + " FROM '" + tableFileName + "' DELIMITER ',' CSV"

	file, err := os.Create(tableFileName)
	if err != nil {
		log.Fatal("Failed to creatge file ", tableFileName, ". Error ", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Generate Bind Variables
	for _, obj := range jsonObjs {
		var record []string
		for _, field := range fields {
			record = append(record, fmt.Sprintf("%v", NVL(obj[field], "")))
		}

		if err := writer.Write(record); err != nil {
			log.Fatal("Faield to insert record ", record, ". Error ", err)
		}
	}

	return sql
}

func (d *JsonToPGSQLConverter) FlattenJsonArrayText(jsonText string, rootTable string) map[string][]JsonObject {
	var objs []JsonObject
	var allObjs map[string][]JsonObject
	err := json.Unmarshal([]byte(jsonText), &objs)
	if err != nil {
		log.Fatal("Failed to parse json string ", jsonText, ", error ", err)
		return allObjs
	}
	return d.FlattenJsonArrayObjs(objs, rootTable)
}

// Flatten one level of nested object
func (d *JsonToPGSQLConverter) FlattenJsonArrayObjs(jsonObjs []JsonObject, rootTable string) map[string][]JsonObject {
	allObjs := make(map[string][]JsonObject)

	for _, obj := range jsonObjs {
		for key, val := range obj {
			if nestObj, nestOK := val.(map[string]interface{}); nestOK {
				name, nameOK := nestObj["name"]
				if nameOK {
					// Override the nested object with the name
					obj[key] = name
					if !existsInSlice(allObjs["sdc_"+key], nestObj) {
						allObjs["sdc_"+key] = append(allObjs["sdc_"+key], nestObj)
					}
				} else {
					log.Fatal("Could not find the [name] key from nested object. ", nestObj)
				}
			}
		}
	}
	allObjs[rootTable] = jsonObjs
	return allObjs
}

func existsInSlice(s []JsonObject, m JsonObject) bool {
	for _, element := range s {
		if reflect.DeepEqual(m, element) {
			return true
		}
	}
	return false
}

func (d *JsonToPGSQLConverter) deriveColType(value interface{}) (string, error) {
	var err error
	var colType string
	switch v := value.(type) {
	case int:
		colType = "integer"
	case float32:
		colType = "real"
	case bool:
		colType = "boolean"
	case time.Time:
		colType = "timestamp"
	case string:
		if len(v) <= MAX_CHAR_SIZE {
			colType = "varchar(" + fmt.Sprint(MAX_CHAR_SIZE) + ")"
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

func orderedKeys(m JsonObject) []string {
	var keys []string
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func NVL(val interface{}, defaultVal interface{}) interface{} {
	if val == nil {
		return defaultVal
	}
	return val
}
