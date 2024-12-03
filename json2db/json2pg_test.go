package json2db

import (
	"reflect"
	"strings"
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
            "name": "strVal2",
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
            "name": "strVal2",
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

func TestJsonToPGSQLConverter_ExtractFieldData(t *testing.T) {
	wantKeyCols := map[string]reflect.Type{
		"Field1": reflect.TypeFor[string](),
		"Field2": reflect.TypeFor[int](),
	}
	wantNonKeyCols := map[string]reflect.Type{
		"Field3": reflect.TypeFor[float64](),
		"Field4": reflect.TypeFor[bool](),
		"Field5": reflect.TypeFor[NestedJsonEntityStruct](),
		"Field6": reflect.TypeFor[Date](),
	}
	t.Run("ExtractFieldData", func(t *testing.T) {
		gotKeyCols, gotNonKeyCols := NewJsonToPGSQLConverter().ExtractFieldData(reflect.TypeFor[JsonEntityStruct]())
		if !reflect.DeepEqual(gotKeyCols, wantKeyCols) {
			t.Errorf("JsonToPGSQLConverter.ExtractFieldData() got key colmns = %v, want %v", gotKeyCols, wantKeyCols)
		}
		if !reflect.DeepEqual(gotNonKeyCols, wantNonKeyCols) {
			t.Errorf("JsonToPGSQLConverter.ExtractFieldData() got key colmns = %v, want %v", gotNonKeyCols, wantNonKeyCols)
		}
	})
}

func TestJsonToPGSQLConverter_ExtractValues(t *testing.T) {
	time1, _ := time.Parse("2006-01-02 15:04:05 -0700 MST", "2015-10-31 00:00:00 +0000 UTC")
	time2, _ := time.Parse("2006-01-02 15:04:05 -0700 MST", "2015-07-31 00:00:00 +0000 UTC")
	date1 := Date{time1}
	date2 := Date{time2}
	wantRows := [][]interface{}{
		{"strVal", 10, 1.0, false, "strVal2", date1},
		{"strVal3", 20, 2.0, true, "strVal2", date2},
	}
	t.Run("ExtractValues", func(t *testing.T) {
		gotRows, err := NewJsonToPGSQLConverter().ExtractValues(JSON_TEXT, reflect.TypeFor[JsonEntityStruct]())
		if err != nil {
			t.Errorf("JsonToPGSQLConverter.ExtractValues() error = %v", err)
			return
		}
		if !reflect.DeepEqual(gotRows, wantRows) {
			t.Errorf("JsonToPGSQLConverter.ExtractValues() all rows got = %v, want %v", gotRows, wantRows)
		}
	})
}

func TestJsonToPGSQLConverter_GenInsertSQL(t *testing.T) {

	time1, _ := time.Parse("2006-01-02 15:04:05 -0700 MST", "2015-10-31 00:00:00 +0000 UTC")
	time2, _ := time.Parse("2006-01-02 15:04:05 -0700 MST", "2015-07-31 00:00:00 +0000 UTC")
	date1 := Date{time1}
	date2 := Date{time2}
	wantSQL := `
INSERT INTO json2pg_test (
	field1, field2, field3, field4, field5, field6
)
VALUES (
	$1, $2, $3, $4, $5, $6
) ON CONFLICT (
	field1, field2
) DO UPDATE SET
	field1 = EXCLUDED.field1,
	field2 = EXCLUDED.field2,
	field3 = EXCLUDED.field3,
	field4 = EXCLUDED.field4,
	field5 = EXCLUDED.field5,
	field6 = EXCLUDED.field6
`
	wantRows := [][]interface{}{
		{"strVal", 10, 1.0, false, "strVal2", date1},
		{"strVal3", 20, 2.0, true, "strVal2", date2},
	}
	t.Run("GenInsertSQL", func(t *testing.T) {
		gotSQL, gotRows, err := NewJsonToPGSQLConverter().GenInsertSQL(JSON_TEXT, TEST_TABLE, reflect.TypeFor[JsonEntityStruct]())
		if err != nil {
			t.Errorf("JsonToPGSQLConverter.GenInsertSQL() error = %v", err)
			return
		}
		if strings.TrimSpace(gotSQL) != strings.TrimSpace(wantSQL) {
			t.Errorf("JsonToPGSQLConverter.GenInsertSQL() gotSQL = %v, wantSQL %v", gotSQL, wantSQL)
		}

		if !reflect.DeepEqual(gotRows, wantRows) {
			t.Errorf("JsonToPGSQLConverter.GenInsertSQL() gotRows = %v, wantRows %v", gotRows, wantRows)
		}
	})
}

func TestJsonToPGSQLConverter_GenBulkInsertSQL(t *testing.T) {
	wantSQL := `
INSERT INTO json2pg_test (
	field1, field2, field3, field4, field5, field6
)
VALUES
	('strVal', 10, 1, false, 'strVal2', '2015-10-31 00:00:00 +0000 UTC'),
	('strVal3', 20, 2, true, 'strVal2', '2015-07-31 00:00:00 +0000 UTC')
ON CONFLICT (
	field1, field2
) DO UPDATE SET
	field1 = EXCLUDED.field1,
	field2 = EXCLUDED.field2,
	field3 = EXCLUDED.field3,
	field4 = EXCLUDED.field4,
	field5 = EXCLUDED.field5,
	field6 = EXCLUDED.field6
`
	t.Run("GenBulkInsertSQL", func(t *testing.T) {
		gotSQL, err := NewJsonToPGSQLConverter().GenBulkInsertSQL(JSON_TEXT, TEST_TABLE, reflect.TypeFor[JsonEntityStruct]())
		if err != nil {
			t.Errorf("JsonToPGSQLConverter.GenBulkInsertSQL() error = %v", err)
			return
		}
		if strings.TrimSpace(gotSQL) != strings.TrimSpace(wantSQL) {
			t.Errorf("JsonToPGSQLConverter.GenBulkInsertSQL() gotSQL = %v, wantSQL %v", gotSQL, wantSQL)
		}
	})
}
