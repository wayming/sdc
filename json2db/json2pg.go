package json2db

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
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

func (d *JsonToPGSQLConverter) GenCreateTable(jsonText string, tableName string, responseType reflect.Type) (string, error) {
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
		fmt.Println("fieldName=", fieldName, "fieldType=", fieldType.Kind(), "fieldValue=", fieldValue.Interface())
		colType, err := d.deriveColType(fieldType)
		if err != nil {
			err := errors.New("Failed to derive type for " + fieldValue.String() + ", error " + err.Error())
			return "", err
		}
		ddl += fieldName + " " + colType + ", "
	}
	ddl = ddl[:len(ddl)-2] + ");"

	return ddl, nil
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

func (d *JsonToPGSQLConverter) GenInsert(jsonText string, tableName string, jsonStructType reflect.Type) (string, [][]interface{}, error) {
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

func (d *JsonToPGSQLConverter) deriveColType(rtype reflect.Type) (string, error) {
	var err error
	var colType string
	switch rtype.Kind() {
	case reflect.Int:
		colType = "integer"
	case reflect.Float32:
		colType = "real"
	case reflect.Float64:
		colType = "double precision"
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
		err = errors.New("unknown type " + rtype.Kind().String())
	}

	return colType, err
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
