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
	keyCols, nonKeyCols := d.ExtractColData(entityStructType)
	ddl := "CREATE TABLE IF NOT EXISTS " + tableName + " ("

	// Columns for key fields
	for _, name := range Keys(keyCols) {
		colType, err := d.deriveColType(keyCols[name])
		if err != nil {
			err := fmt.Errorf(
				"Failed to derive type for field %s, field type is %s. Error: %s"+name, keyCols[name].Name(), err.Error())
			return "", err
		}
		ddl += name + " " + colType + ", "
	}

	// Columns for non-key fields
	for _, name := range Keys(nonKeyCols) {
		colType, err := d.deriveColType(keyCols[name])
		if err != nil {
			err := fmt.Errorf(
				"Failed to derive type for field %s, field type is %s. Error: %s"+name, keyCols[name].Name(), err.Error())
			return "", err
		}
		ddl += name + " " + colType + ", "
	}

	// Primary clause
	if len(Keys(keyCols)) > 0 {
		ddl += "PRIMARY KEY (" + strings.Join(Keys(keyCols), ", ") + "));"
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

	allCols, keyCols := d.ExtractColData(entityStructType)

	// Generage SQL
	sql = "INSERT INTO " + tableName + " (" + strings.Join(Keys(allCols), ", ") + ") "
	sql += "VALUES ("
	for index := range Keys(allCols) {
		if index > 0 {
			sql += ", "
		}
		sql += "$" + strconv.Itoa(index+1)
	}
	sql += ") "

	sql += d.OnConflitsSQL(tableName, Keys(allCols), Keys(keyCols))
	return sql, rows, nil
}

func (d *JsonToPGSQLConverter) GenBulkInsertSQL(jsonText string, tableName string, entityStructType reflect.Type) (string, error) {
	var sql string

	keyCols, nonKeyCols := d.ExtractColData(entityStructType)
	allColNames := append(Keys(keyCols), Keys(nonKeyCols)...)
	rows, err := d.ExtractValues(jsonText, entityStructType)
	if err != nil {
		return sql, err
	}

	// Generage SQL
	sql = "INSERT INTO " + tableName + " (" + strings.ToLower(strings.Join(allColNames, ", ")) + ") "
	sql += "VALUES "

	for idx, row := range rows {
		sql += "("
		for idx2, v := range row {
			if idx2 >= len(allColNames) {
				return sql, fmt.Errorf("Unexpected number of values for a row. %d values, %d colums.", idx2+1, len(allColNames))
			}

			var colType reflect.Type
			if Exists(keyCols, allColNames[idx2]) {
				colType = keyCols[allColNames[idx2]]
			} else if Exists(nonKeyCols, allColNames[idx2]) {
				colType = nonKeyCols[allColNames[idx2]]
			}

			if colType == nil {
				return sql, fmt.Errorf("Failed to find data type for column %s", &allColNames[idx2])
			}

			if colType == reflect.TypeFor[string]() ||
				colType == reflect.TypeFor[Date]() ||
				colType == reflect.TypeFor[time.Time]() {
				sql += fmt.Sprintf("'%s',", v)
			} else {
				sql += fmt.Sprintf("%s,", v)
			}

			if idx2 < len(row)-1 {
				sql += ","
			}
		}
		sql += ")"
		if idx < len(rows)-1 {
			sql += ","
		}
	}

	sql += d.OnConflitsSQL(tableName, Keys(keyCols), Keys(nonKeyCols))
	return sql, nil
}

// Unmarshals the specified JSON text that represents array of entities.
// Returns a slice of column names and a slice of rows. These artifacts can be used as the input for the bulk insert interfaces.
func (d *JsonToPGSQLConverter) ExtractColData(entityStructType reflect.Type) (map[string]reflect.Type, map[string]reflect.Type) {
	keyFields := keyFields((entityStructType))
	nonKeyFields := make(map[string]reflect.Type)
	for idx := 0; idx < entityStructType.NumField(); idx++ {
		fieldName := strings.ToLower(entityStructType.Field(idx).Name)
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
	keyCols, nonKeyCols := d.ExtractColData(entityStructType)

	// Key columms first and then non-key columns.
	fieldNames := append(Keys(keyCols), Keys(nonKeyCols)...)
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

func (d *JsonToPGSQLConverter) OnConflitsSQL(table string, keyCols []string, nonKeyCols []string) string {
	var sql string
	// Handle conflicts if has primary keys
	if len(keyCols) > 0 {
		firstStatement := true
		var setClause string
		var whereClause string
		sql = "ON CONFLICT (" + strings.ToLower(strings.Join(keyCols, ", ")) + ") DO UPDATE "
		for _, col := range append(keyCols, nonKeyCols...) {
			if firstStatement {
				firstStatement = false
				setClause = "SET "
				whereClause = "WHERE "
			} else {
				setClause += ", "
				whereClause += "OR "
			}

			setClause += col + " = EXCLUDED." + col
			whereClause += table + "." + col + " <> EXCLUDED." + col + " "
		}
		sql += setClause + " " + whereClause
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
			keyFields[strings.ToLower(rtype.Field(idx).Name)] = rtype.Field(idx).Type
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
