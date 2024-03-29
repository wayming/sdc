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

func (d *JsonToPGSQLConverter) GenCreateTableSQLByJson2(jsonText string, tableName string, responseType reflect.Type) (string, error) {
	sliceType := reflect.SliceOf(responseType)
	slicePtr := reflect.New(sliceType)
	err := json.Unmarshal([]byte(jsonText), slicePtr.Interface())
	sliceVal := slicePtr.Elem()
	if err != nil || sliceVal.Len() == 0 {
		err := errors.New("Failed to parse json string " + jsonText + ", error " + err.Error())
		return "", err
	}

	ddl := "CREATE TABLE IF NOT EXISTS " + tableName + " ("
	for _, fieldName := range orderedFields(responseType) {
		if _, ok := d.tableFieldsMap[tableName]; !ok {
			d.tableFieldsMap[tableName] = make([]string, 0)
		}
		d.tableFieldsMap[tableName] = append(d.tableFieldsMap[tableName], strings.ToLower(fieldName))
		fieldValue := sliceVal.Index(0).FieldByName(fieldName)

		// Check if the field value is valid before proceeding
		if !fieldValue.IsValid() {
			log.Println("Field value is invalid for field:", fieldName)
			continue
		}

		fieldType := fieldValue.Type()
		fmt.Println("fieldName", fieldName, "fieldType=", fieldType.Kind(), "fieldValue=", fieldValue.Interface())
		colType, err := d.deriveColType2(fieldType)
		if err != nil {
			err := errors.New("Failed to derive type for " + fieldValue.String() + ", error " + err.Error())
			return "", err
		}
		ddl += fieldName + " " + colType + ", "
	}
	ddl = ddl[:len(ddl)-2] + ");"

	return ddl, nil
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

// // Assume a flat json text string for the same table
// func (d *JsonToPGSQLConverter) GenInsertSQL(jsonText string, tableName string) (string, [][]interface{}) {
// 	var objs []JsonObject
// 	err := json.Unmarshal([]byte(jsonText), &objs)
// 	if err != nil {
// 		log.Fatal("Failed to parse json string ", jsonText, ", error ", err)
// 	}

// 	return d.GenInsertSQLByJsonObjs(objs, tableName)
// }

// func (d *JsonToPGSQLConverter) GenInsertSQLByJsonObjs(jsonObjs []JsonObject, tableName string) (string, [][]interface{}) {
// 	// Generage SQL
// 	fields := d.tableFieldsMap[tableName]
// 	sql := "INSERT INTO " + tableName + " (" + strings.Join(fields, ", ") + ") VALUES ("
// 	for index := range fields {
// 		if index > 0 {
// 			sql = sql + ", "
// 		}
// 		sql = sql + "$" + strconv.Itoa(index+1)
// 	}
// 	sql = sql + ") ON CONFLICT DO NOTHING"

// 	// Generate Bind Variables
// 	var bindVars [][]interface{}
// 	for _, obj := range jsonObjs {
// 		var bindVarsForObj []interface{}
// 		for _, field := range fields {
// 			bindVarsForObj = append(bindVarsForObj, obj[field])
// 		}
// 		bindVars = append(bindVars, bindVarsForObj)
// 	}

// 	return sql, bindVars
// }

func (d *JsonToPGSQLConverter) GenBulkInsertSQLByJsonObjs(jsonObjs []JsonObject, tableName string) string {
	// Generage SQL
	fields := d.tableFieldsMap[tableName]
	cwd, _ := os.Getwd()
	tableFileName := cwd + "/" + tableName + ".csv"
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

func (d *JsonToPGSQLConverter) GenBulkInsertSQLByJsonText(jsonText string, tableName string, responseType reflect.Type) (string, error) {
	var sql string
	sliceType := reflect.SliceOf(responseType)
	slicePtr := reflect.New(sliceType)

	err := json.Unmarshal([]byte(jsonText), slicePtr.Interface())
	sliceVal := slicePtr.Elem()
	if err != nil || sliceVal.Len() == 0 {
		return sql, errors.New("Failed to parse json string " + jsonText + ", error " + err.Error())
	}

	// Generage SQL
	cwd, _ := os.Getwd()
	tableFileName := cwd + "/" + tableName + ".csv"
	sql = "COPY " + tableName + " FROM '" + tableFileName + "' WITH CSV"

	file, err := os.Create(tableFileName)
	if err != nil {
		log.Fatal("Failed to creatge file ", tableFileName, ". Error ", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Generate Bind Variables
	for idx := 0; idx < sliceVal.Len(); idx++ {
		var record []string
		for _, fieldName := range orderedFields((responseType)) {
			fieldValue := sliceVal.Index(idx).FieldByName(fieldName)
			if fieldValue.Type().Kind() == reflect.Struct {
				nestedFieldValue := fieldValue.FieldByName("Name")
				record = append(record, fmt.Sprintf("%v", NVL(nestedFieldValue.Interface(), "")))
			} else {
				record = append(record, fmt.Sprintf("%v", NVL(fieldValue.Interface(), "")))
			}
		}

		if err := writer.Write(record); err != nil {
			return sql, errors.New(
				"Faield to write record " + strings.Join(record, ",") +
					" to file " + tableFileName + ". Error " + err.Error())
		}
	}
	return sql, nil
}

func (d *JsonToPGSQLConverter) GenBulkInsert(jsonText string, tableName string, jsonStructType reflect.Type) ([]string, [][]interface{}, error) {
	var rows [][]interface{}
	fields := d.tableFieldsMap[tableName]

	sliceType := reflect.SliceOf(jsonStructType)
	slicePtr := reflect.New(sliceType)

	err := json.Unmarshal([]byte(jsonText), slicePtr.Interface())
	sliceVal := slicePtr.Elem()
	if err != nil || sliceVal.Len() == 0 {
		return fields, rows, errors.New("Failed to parse json string " + jsonText + ", error " + err.Error())
	}

	// Generate Bind Variables
	for idx := 0; idx < sliceVal.Len(); idx++ {
		var row []interface{}
		for _, fieldName := range orderedFields(jsonStructType) {
			fieldValue := sliceVal.Index(idx).FieldByName(fieldName)
			if fieldValue.Type().Kind() == reflect.Struct {
				nestedFieldValue := fieldValue.FieldByName("Name")
				row = append(row, nestedFieldValue.Interface())
			} else {
				row = append(row, fieldValue.Interface())
			}
		}
		rows = append(rows, row)
	}

	return fields, rows, nil
}

func (d *JsonToPGSQLConverter) GenInsertSQL(jsonText string, tableName string, jsonStructType reflect.Type) (string, [][]interface{}, error) {
	var sql string
	var rows [][]interface{}

	sliceType := reflect.SliceOf(jsonStructType)
	slicePtr := reflect.New(sliceType)

	err := json.Unmarshal([]byte(jsonText), slicePtr.Interface())
	sliceVal := slicePtr.Elem()
	if err != nil || sliceVal.Len() == 0 {
		return sql, rows, errors.New("Failed to parse json string " + jsonText + ", error " + err.Error())
	}

	// Generage SQL
	fields := d.tableFieldsMap[tableName]
	sql = "INSERT INTO " + tableName + " (" + strings.Join(fields, ", ") + ") VALUES ("
	for index := range fields {
		if index > 0 {
			sql = sql + ", "
		}
		sql = sql + "$" + strconv.Itoa(index+1)
	}
	sql = sql + ") ON CONFLICT DO NOTHING"

	// Generate Bind Variables
	for idx := 0; idx < sliceVal.Len(); idx++ {
		var row []interface{}
		for _, fieldName := range orderedFields(jsonStructType) {
			fieldValue := sliceVal.Index(idx).FieldByName(fieldName)
			if fieldValue.Type().Kind() == reflect.Struct {
				nestedFieldValue := fieldValue.FieldByName("Name")
				row = append(row, nestedFieldValue.Interface())
			} else {
				row = append(row, fieldValue.Interface())
			}
		}
		rows = append(rows, row)
	}

	return sql, rows, nil
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

func (d *JsonToPGSQLConverter) deriveColType2(rtype reflect.Type) (string, error) {
	var err error
	var colType string
	switch rtype.Kind() {
	case reflect.Int:
		colType = "integer"
	case reflect.Float32:
		colType = "real"
	case reflect.Bool:
		colType = "boolean"
	case reflect.String:
		colType = "varchar(" + fmt.Sprint(MAX_CHAR_SIZE) + ")"
	case reflect.Struct:
		if rtype == reflect.TypeOf(time.Time{}) {
			colType = "timestamp"
		} else {
			if _, ok := rtype.FieldByName("Name"); ok {
				// Use the "Name" field as the value of the nested struct,
				// thus create the field with the varchar type.
				colType = "varchar(" + fmt.Sprint(MAX_CHAR_SIZE) + ")"
			} else {
				err = errors.New("Unknown struct type for field " + rtype.Name())
			}
		}
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

func orderedFields(rtype reflect.Type) []string {
	var keys []string
	for idx := 0; idx < rtype.NumField(); idx++ {
		keys = append(keys, rtype.Field(idx).Name)
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
