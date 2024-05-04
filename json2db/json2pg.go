package json2db

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
)

const MAX_CHAR_SIZE = 1024

type JsonToPGSQLConverter struct {
}

func NewJsonToPGSQLConverter() *JsonToPGSQLConverter {
	return &JsonToPGSQLConverter{}
}

func (d *JsonToPGSQLConverter) GenCreateSchema(schema string) string {
	sql := "CREATE SCHEMA IF NOT EXISTS " + schema
	return sql
}

func (d *JsonToPGSQLConverter) GenDropSchema(schema string) string {
	sql := "DROP SCHEMA IF EXISTS " + schema + " CASCADE"
	return sql
}

// Generate table creation SQL
func (d *JsonToPGSQLConverter) GenCreateTable(tableName string, responseType reflect.Type) (string, error) {
	ddl := "CREATE TABLE IF NOT EXISTS " + tableName + " ("
	for _, fieldName := range orderedFields(responseType) {
		colName := strings.ToLower(fieldName)
		field, ok := responseType.FieldByName(fieldName)
		if !ok {
			return "", errors.New("Failed to get field " + fieldName + " from entity type " + responseType.Name())
		}
		colType, err := d.deriveColType(field.Type)
		if err != nil {
			err := errors.New(
				"Failed to derive type for field " + fieldName +
					", field value type is  " + field.Type.Name() + ". Error: " + err.Error())
			return "", err
		}
		ddl += colName + " " + colType + ", "
	}
	ddl = ddl[:len(ddl)-2] + ");"

	return ddl, nil
}

// Unmarshals the specified JSON text that represents array of entities.
// Returns a slice of column names and a slice of rows. These artifacts can be used as the input for the bulk insert interfaces.
func (d *JsonToPGSQLConverter) GenBulkInsert(jsonText string, tableName string, entityStructType reflect.Type) ([]string, [][]interface{}, error) {
	var rows [][]interface{}

	// Unmarshal the JSON text.
	sliceType := reflect.SliceOf(entityStructType)
	slicePtr := reflect.New(sliceType)
	err := json.Unmarshal([]byte(jsonText), slicePtr.Interface())
	sliceVal := slicePtr.Elem()
	if err != nil || sliceVal.Len() == 0 {
		return nil, nil, errors.New("Failed to parse json string " + jsonText + ", error " + err.Error())
	}

	// Generate Bind Variables
	fields := orderedFields(entityStructType)
	for idx := 0; idx < sliceVal.Len(); idx++ {
		var row []interface{}
		for _, fieldName := range fields {
			fieldValue := sliceVal.Index(idx).FieldByName(fieldName)
			if fieldValue.Type().Kind() == reflect.Struct &&
				fieldValue.Type() != reflect.TypeFor[Date]() &&
				fieldValue.Type() != reflect.TypeFor[time.Time]() {
				nestedFieldValue := fieldValue.FieldByName("Name")
				if !nestedFieldValue.IsValid() {
					return nil, nil, fmt.Errorf("failed to get Name field from value %v, type %v", fieldValue, fieldValue.Type())
				}
				row = append(row, nestedFieldValue.Interface())
			} else {
				row = append(row, fieldValue.Interface())
			}
		}
		rows = append(rows, row)
	}

	// Column names are in lower case
	var cols []string
	for _, field := range fields {
		cols = append(cols, strings.ToLower(field))
	}
	return cols, rows, nil
}

// Unmarshals the specified JSON text that represents array of entities.
// Returns insert SQL with slice of rows. Each row is a slice with each element represents a field value.
func (d *JsonToPGSQLConverter) GenInsert(jsonText string, tableName string, entityStructType reflect.Type) (string, [][]interface{}, error) {
	var sql string
	var rows [][]interface{}

	sliceType := reflect.SliceOf(entityStructType)
	slicePtr := reflect.New(sliceType)
	err := json.Unmarshal([]byte(jsonText), slicePtr.Interface())
	sliceVal := slicePtr.Elem()
	if err != nil || sliceVal.Len() == 0 {
		return sql, nil, errors.New("Failed to parse json string " + jsonText + ", error " + err.Error())
	}

	// Generage SQL
	fields := orderedFields(entityStructType)
	sql = "INSERT INTO " + tableName + " (" + strings.ToLower(strings.Join(fields, ", ")) + ") VALUES ("
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
		for _, fieldName := range fields {
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
		colType = "numeric(12, 2)"
	case reflect.Float64:
		colType = "numeric(24, 2)"
	case reflect.Bool:
		colType = "boolean"
	case reflect.String:
		colType = "varchar(" + fmt.Sprint(MAX_CHAR_SIZE) + ")"
	case reflect.Struct:
		if rtype == reflect.TypeOf(time.Time{}) || rtype == reflect.TypeFor[Date]() {
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
