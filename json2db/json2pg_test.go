package json2db

import (
	"reflect"
	"testing"
	"time"
)

const JSON_TEXT = `[
    {
        "field1": "strVal",
        "field2": 10,
        "field3": 1,
        "field4": false,
        "field5": {
            "name1": "strVal2",
            "nestedField2": 100
        },
		"field6": "2015-10-31"
    },
    {
        "field1": "strVal3",
        "field2": 20,
        "field3": 2,
        "field4": true,
        "field5": {
            "name1": "strVal2",
            "nestedField2": 100
        },
		"field6": "2015-07-31"
    }
]`

type NestedJsonEntityStruct struct {
	Name         string `json:"name"`
	NestedField2 int    `json:"nestedField2"`
}
type JsonEntityStruct struct {
	Field1 string                 `json:"field1" db:"PrimaryKey"`
	Field2 int                    `json:"field2" db:"PrimaryKey"`
	Field3 float64                `json:"field3"`
	Field4 bool                   `json:"field4"`
	Field5 NestedJsonEntityStruct `json:"field5"`
	Field6 Date                   `json:"field6"`
}

const TEST_TABLE = "json2pg_test"

func TestJsonToPGSQLConverter_CreateTableSQL(t *testing.T) {
	wantSql :=
		`CREATE TABLE IF NOT EXISTS json2pg_test ` +
			"(field1 varchar(1024), " +
			"field2 integer, " +
			"field3 numeric(24, 2), " +
			"field4 boolean, " +
			"field5 varchar(1024), " +
			"field6 timestamp, " +
			"PRIMARY KEY (field1, field2));"

	t.Run("CreateTableSQL", func(t *testing.T) {
		got, err := NewJsonToPGSQLConverter().GenCreateTable(TEST_TABLE, reflect.TypeFor[JsonEntityStruct]())
		if err != nil {
			t.Errorf("GenCreateTable returns error %s", err.Error())
		}
		if got != wantSql {
			t.Errorf("JsonToPGSQLConverter.GenCreateTable() = %v, wantSql %v", got, wantSql)
		}
	})
}

func TestJsonToPGSQLConverter_SQLData(t *testing.T) {
	time1, _ := time.Parse("2006-01-02 15:04:05 -0700 MST", "2015-10-31 00:00:00 +0000 UTC")
	time2, _ := time.Parse("2006-01-02 15:04:05 -0700 MST", "2015-07-31 00:00:00 +0000 UTC")
	date1 := Date{time1}
	date2 := Date{time2}

	wantFields := []string{"field1", "field2", "field3", "field4", "field5", "field6"}
	wantValues := [][]interface{}{
		{"strVal", 10, 1.0, false, "strVal2", date1},
		{"strVal3", 20, 2.0, true, "strVal2", date2},
	}
	t.Run("SQLData", func(t *testing.T) {
		gotFields, gotValues, err := NewJsonToPGSQLConverter().SQLData(JSON_TEXT, TEST_TABLE, reflect.TypeFor[JsonEntityStruct]())
		if err != nil {
			t.Errorf("JsonToPGSQLConverter.SQLData() error = %v", err)
			return
		}
		if !reflect.DeepEqual(gotFields, wantFields) {
			t.Errorf("JsonToPGSQLConverter.SQLData() got = %v, want %v", gotFields, wantFields)
		}
		if !reflect.DeepEqual(gotValues, wantValues) {
			t.Errorf("JsonToPGSQLConverter.SQLData() got = %v, want %v", gotValues, wantValues)
		}
	})
}

func TestJsonToPGSQLConverter_GenInsertSQL(t *testing.T) {

	wantSQL := "INSERT INTO json2pg_test (field1, field2, field3, field4, field5) " +
		"VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING"
	wantFieldValues := [][]interface{}{
		{"strVal", 10, 1.0, false, "strVal2"},
		{"strVal3", 20, 2.0, true, "strVal2"},
	}
	t.Run("GenInsertSQL", func(t *testing.T) {
		gotSQL, gotFieldValues, err := NewJsonToPGSQLConverter().GenInsertSQL(JSON_TEXT, TEST_TABLE, reflect.TypeFor[JsonEntityStruct]())
		if err != nil {
			t.Errorf("JsonToPGSQLConverter.GenInsertSQL() error = %v", err)
			return
		}
		if gotSQL != wantSQL {
			t.Errorf("JsonToPGSQLConverter.GenInsertSQL() gotSQL = %v, wantSQL %v", gotSQL, wantSQL)
		}

		if !reflect.DeepEqual(gotFieldValues, wantFieldValues) {
			t.Errorf("JsonToPGSQLConverter.GenInsertSQL() gotFieldValues = %v, wantFieldValues %v", gotFieldValues, wantFieldValues)
		}
	})
}
