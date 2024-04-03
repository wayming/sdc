package json2db

import (
	"reflect"
	"testing"
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
        }
    },
    {
        "field1": "strVal3",
        "field2": 20,
        "field3": 2,
        "field4": true,
        "field5": {
            "name": "strVal2",
            "nestedField2": 100
        }
    }
]`

type NestedJsonEntityStruct struct {
	Name         string `json:"name"`
	NestedField2 int    `json:"nestedField2"`
}
type JsonEntityStruct struct {
	Field1 string                 `json:"field1"`
	Field2 int                    `json:"field2"`
	Field3 float64                `json:"field3"`
	Field4 bool                   `json:"field4"`
	Field5 NestedJsonEntityStruct `json:"field5"`
}

const TEST_SCHEMA = "sdc_test"
const TEST_TABLE = "json2pg_test"

func TestJsonToPGSQLConverter_CreateTableSQL(t *testing.T) {
	type args struct {
		tableName  string
		entityType reflect.Type
	}
	tests := []struct {
		name    string
		args    args
		wantSql string
	}{
		{
			name: "CreateTableSQL",
			args: args{tableName: TEST_TABLE, entityType: reflect.TypeFor[JsonEntityStruct]()},
			wantSql: `CREATE TABLE IF NOT EXISTS json2pg_test ` +
				`(Field1 varchar(1024), Field2 integer, Field3 double precision, Field4 boolean, Field5 varchar(1024));`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &JsonToPGSQLConverter{}
			got, err := d.GenCreateTable(tt.args.tableName, tt.args.entityType)
			if err != nil {
				t.Errorf("GenCreateTable returns error %s", err.Error())
			}
			if got != tt.wantSql {
				t.Errorf("JsonToPGSQLConverter.GenCreateTable() = %v, wantSql %v", got, tt.wantSql)
			}
		})
	}
}

func TestJsonToPGSQLConverter_GenBulkInsert(t *testing.T) {
	type args struct {
		jsonText         string
		tableName        string
		entityStructType reflect.Type
	}
	tests := []struct {
		name       string
		d          *JsonToPGSQLConverter
		args       args
		wantFields []string
		wantValues [][]interface{}
		wantErr    bool
	}{
		{
			name: "GenBulkInsert",
			d:    NewJsonToPGSQLConverter(),
			args: args{
				jsonText:         JSON_TEXT,
				tableName:        TEST_TABLE,
				entityStructType: reflect.TypeFor[JsonEntityStruct](),
			},
			wantFields: []string{"Field1", "Field2", "Field3", "Field4", "Field5"},
			wantValues: [][]interface{}{
				{"strVal", 10, 1.0, false, "strVal2"},
				{"strVal3", 20, 2.0, true, "strVal2"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &JsonToPGSQLConverter{}
			gotFields, gotValues, err := d.GenBulkInsert(tt.args.jsonText, tt.args.tableName, tt.args.entityStructType)
			if (err != nil) != tt.wantErr {
				t.Errorf("JsonToPGSQLConverter.GenBulkInsert() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotFields, tt.wantFields) {
				t.Errorf("JsonToPGSQLConverter.GenBulkInsert() got = %v, want %v", gotFields, tt.wantFields)
			}
			if !reflect.DeepEqual(gotValues, tt.wantValues) {
				t.Errorf("JsonToPGSQLConverter.GenBulkInsert() got = %v, want %v", gotValues, tt.wantValues)
			}
		})
	}
}

func TestJsonToPGSQLConverter_GenInsert(t *testing.T) {
	type args struct {
		jsonText         string
		tableName        string
		entityStructType reflect.Type
	}
	tests := []struct {
		name            string
		d               *JsonToPGSQLConverter
		args            args
		wantSQL         string
		wantFieldValues [][]interface{}
		wantErr         bool
	}{
		{
			name: "GenInsert",
			d:    NewJsonToPGSQLConverter(),
			args: args{
				jsonText:         JSON_TEXT,
				tableName:        TEST_TABLE,
				entityStructType: reflect.TypeFor[JsonEntityStruct](),
			},
			wantSQL: "INSERT INTO json2pg_test (Field1, Field2, Field3, Field4, Field5) " +
				"VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING",
			wantFieldValues: [][]interface{}{
				{"strVal", 10, 1.0, false, "strVal2"},
				{"strVal3", 20, 2.0, true, "strVal2"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &JsonToPGSQLConverter{}
			got, got1, err := d.GenInsert(tt.args.jsonText, tt.args.tableName, tt.args.entityStructType)
			if (err != nil) != tt.wantErr {
				t.Errorf("JsonToPGSQLConverter.GenInsert() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.wantSQL {
				t.Errorf("JsonToPGSQLConverter.GenInsert() got = %v, wantSQL %v", got, tt.wantSQL)
			}

			if !reflect.DeepEqual(got1, tt.wantFieldValues) {
				t.Errorf("JsonToPGSQLConverter.GenInsert() got1 = %v, wantFieldValues %v", got1, tt.wantFieldValues)
			}
		})
	}
}
