package json2db

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	. "github.com/wayming/sdc/common"
)

const MAX_CHAR_SIZE = 1024
const NESTED_STRUCT_KEY = "Name"
const TAG_DB = "db"
const TAG_DB_PRIMARYKEY = "PrimaryKey"

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
func (d *JsonToPGSQLConverter) GenCreateTable(tableName string, entityStructType reflect.Type) (string, error) {
	keyFields, nonKeyFields := d.ExtractFieldData(entityStructType)
	ddl := "CREATE TABLE IF NOT EXISTS " + tableName + " ("

	// Columns for key fields
	for _, name := range Keys(keyFields) {
		colType, err := d.deriveColType(keyFields[name])
		if err != nil {
			err := fmt.Errorf(
				"Failed to derive type for field %s, field type is %s. Error: %s"+name, keyFields[name].Name(), err.Error())
			return "", err
		}
		ddl += strings.ToLower(name) + " " + colType + ", "
	}

	// Columns for non-key fields
	for _, name := range Keys(nonKeyFields) {
		colType, err := d.deriveColType(nonKeyFields[name])
		if err != nil {
			err := fmt.Errorf(
				"Failed to derive type for field %s, field type is %s. Error: %s"+name, nonKeyFields[name].Name(), err.Error())
			return "", err
		}
		ddl += strings.ToLower(name) + " " + colType + ", "
	}

	// Primary clause
	if len(Keys(keyFields)) > 0 {
		ddl += "PRIMARY KEY (" + strings.ToLower(strings.Join(Keys(keyFields), ", ")) + "));"
	} else {
		ddl = ddl[:len(ddl)-2] + ");"
	}

	return ddl, nil
}

// Unmarshals the specified JSON text that represents array of entities.
// Returns insert SQL with slice of rows. Each row is a slice with each element represents a field value.
func (d *JsonToPGSQLConverter) GenInsertSQL(jsonText string, tableName string, entityStructType reflect.Type) (string, [][]interface{}, error) {
	var sql string
	var rows [][]interface{}

	keyFields, nonKeyFields := d.ExtractFieldData(entityStructType)
	allFields := append(Keys(keyFields), Keys(nonKeyFields)...)

	// Generage SQL
	colLists := strings.ToLower(strings.Join(allFields, ", "))
	var placeHolderList string
	for index := range allFields {
		if index > 0 {
			placeHolderList += ", "
		}
		placeHolderList += "$" + strconv.Itoa(index+1)
	}
	onConflitsClause := d.OnConflitsSQL(tableName, Keys(keyFields), Keys(nonKeyFields))

	sql = fmt.Sprintf("\nINSERT INTO %s (\n\t%s\n)\nVALUES (\n\t%s\n) %s",
		tableName, colLists, placeHolderList, onConflitsClause)
	rows, err := d.ExtractValues(jsonText, entityStructType)
	if err != nil {
		return sql, rows, err
	}

	return sql, rows, nil
}

func (d *JsonToPGSQLConverter) GenBulkInsertSQL(jsonText string, tableName string, entityStructType reflect.Type) (string, error) {
	var sql string

	keyFields, nonKeyFields := d.ExtractFieldData(entityStructType)
	allFields := append(Keys(keyFields), Keys(nonKeyFields)...)
	rows, err := d.ExtractValues(jsonText, entityStructType)
	if err != nil {
		return sql, err
	}

	// Generage SQL
	var valuesOfRows []string
	for _, row := range rows {
		var values []string
		for idx, v := range row {
			if idx >= len(allFields) {
				return sql, fmt.Errorf("unexpected number of values for a row. %d values, %d colums", idx+1, len(allFields))
			}

			var fieldType reflect.Type
			if Exists(keyFields, allFields[idx]) {
				fieldType = keyFields[allFields[idx]]
			} else if Exists(nonKeyFields, allFields[idx]) {
				fieldType = nonKeyFields[allFields[idx]]
			}

			if fieldType == nil {
				return sql, fmt.Errorf("failed to find data type for column %s", allFields[idx])
			}

			var colValue string
			if len(fmt.Sprintf("%v", v)) == 0 {
				colValue = "NULL"

			} else {
				if fieldType.Kind() == reflect.Struct {
					if fieldType == reflect.TypeFor[Date]() {
						d, _ := v.(Date)
						colValue = fmt.Sprintf("'%v'", d.Format(time.RFC3339))
					} else if fieldType == reflect.TypeFor[time.Time]() {
						t, _ := v.(time.Time)
						colValue = fmt.Sprintf("'%v'", t.Format(time.RFC3339))
					} else { // Name of the nested struct
						colValue = fmt.Sprintf("'%v'", v)
					}
				} else if fieldType.Kind() == reflect.String {
					s, _ := v.(string)
					colValue = fmt.Sprintf("'%v'", strings.ReplaceAll(s, "'", "''"))
				} else {
					colValue = fmt.Sprintf("%v", v)
				}
			}
			values = append(values, colValue)

		}
		valuesOfRows = append(valuesOfRows, fmt.Sprintf("(%s)", strings.Join(values, ", ")))
	}
	colList := strings.ToLower(strings.Join(allFields, ", "))
	valueList := strings.Join(valuesOfRows, ",\n\t")
	onConflictClause := d.OnConflitsSQL(tableName, Keys(keyFields), Keys(nonKeyFields))
	sql = fmt.Sprintf("\nINSERT INTO %s (\n\t%s\n)\nVALUES\n\t%s\n%s",
		tableName, colList, valueList, onConflictClause)

	return sql, nil
}

// Unmarshals the specified JSON text that represents array of entities.
// Returns a slice of column names and a slice of rows. These artifacts can be used as the input for the bulk insert interfaces.
func (d *JsonToPGSQLConverter) ExtractFieldData(entityStructType reflect.Type) (map[string]reflect.Type, map[string]reflect.Type) {
	keyFields := keyFields((entityStructType))
	nonKeyFields := make(map[string]reflect.Type)
	for idx := 0; idx < entityStructType.NumField(); idx++ {
		fieldName := entityStructType.Field(idx).Name
		if !Exists(keyFields, fieldName) {
			nonKeyFields[fieldName] = entityStructType.Field(idx).Type
		}
	}
	return keyFields, nonKeyFields
}

func (d *JsonToPGSQLConverter) ExtractValues(jsonText string, entityStructType reflect.Type) ([][]interface{}, error) {
	var rows [][]interface{}

	// Unmarshal the JSON text.
	sliceType := reflect.SliceOf(entityStructType)
	slicePtr := reflect.New(sliceType)
	err := json.Unmarshal([]byte(jsonText), slicePtr.Interface())
	sliceVal := slicePtr.Elem()
	if err != nil || sliceVal.Len() == 0 {
		return nil, errors.New("Failed to parse json string " + jsonText + ", error " + err.Error())
	}

	// Generate Bind Variables
	keyFields, nonKeyFields := d.ExtractFieldData(entityStructType)

	// Key columms first and then non-key columns.
	fieldNames := append(Keys(keyFields), Keys(nonKeyFields)...)
	for idx := 0; idx < sliceVal.Len(); idx++ {
		var row []interface{}
		for _, fieldName := range fieldNames {
			fieldValue := sliceVal.Index(idx).FieldByName(fieldName)
			if fieldValue.Type().Kind() == reflect.Struct &&
				fieldValue.Type() != reflect.TypeFor[Date]() &&
				fieldValue.Type() != reflect.TypeFor[time.Time]() {
				nestedFieldValue := fieldValue.FieldByName(NESTED_STRUCT_KEY)
				if !nestedFieldValue.IsValid() {
					return nil, fmt.Errorf("failed to find [%s] field from nested struct %s", NESTED_STRUCT_KEY, fieldName)
				}
				row = append(row, nestedFieldValue.Interface())
			} else {
				row = append(row, fieldValue.Interface())
			}
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func (d *JsonToPGSQLConverter) OnConflitsSQL(table string, keyFields []string, nonKeyFields []string) string {
	var sql string
	// Handle conflicts if has primary keys
	if len(keyFields) > 0 {
		keyColList := strings.ToLower(strings.Join(keyFields, ", "))
		var setConditions []string
		// var whereConditions []string
		for _, field := range append(keyFields, nonKeyFields...) {
			colName := strings.ToLower(field)
			setConditions = append(setConditions, colName+" = EXCLUDED."+colName)
			// whereConditions = append(whereConditions, table+"."+colName+" <> EXCLUDED."+colName)
		}

		// sql = fmt.Sprintf("ON CONFLICT (\n\t%s\n) DO UPDATE SET\n\t%s\nWHERE\n\t%s\n",
		// 	keyColList, strings.Join(setConditions, ",\n\t"), strings.Join(whereConditions, " OR\n\t"))
		sql = fmt.Sprintf("ON CONFLICT (\n\t%s\n) DO UPDATE SET\n\t%s\n",
			keyColList, strings.Join(setConditions, ",\n\t"))
	}
	return sql
}

func (d *JsonToPGSQLConverter) deriveColType(rtype reflect.Type) (string, error) {
	var err error
	var colType string
	switch rtype.Kind() {
	case reflect.Int, reflect.Int64:
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

func keyFields(rtype reflect.Type) map[string]reflect.Type {
	keyFields := make(map[string]reflect.Type)
	for idx := 0; idx < rtype.NumField(); idx++ {
		tags := rtype.Field(idx).Tag
		dbTag, ok := tags.Lookup(TAG_DB)
		if ok && dbTag == TAG_DB_PRIMARYKEY {
			keyFields[rtype.Field(idx).Name] = rtype.Field(idx).Type
		}
	}
	return keyFields
}

func NVL(val interface{}, defaultVal interface{}) interface{} {
	if val == nil {
		return defaultVal
	}
	return val
}
