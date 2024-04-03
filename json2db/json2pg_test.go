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

// func TestJsonToPGSQLConverter_GenInsert(t *testing.T) {
// 	type fields struct {
// 		tableFieldsMap map[string][]string
// 	}
// 	type args struct {
// 		jsonText  string
// 		tableName string
// 	}
// 	jsonObjs := []map[string]interface{}{JSON_OBJS[0], JSON_OBJS[0]}
// 	testJsonText, _ := json.Marshal(jsonObjs)
// 	tests := []struct {
// 		name              string
// 		fields            fields
// 		args              args
// 		wantSql           string
// 		wantBindVariables [][]interface{}
// 	}{
// 		{
// 			name: "InsertRowsSQL",
// 			fields: fields{
// 				map[string][]string{
// 					"sdc_tickers": {"country", "has_eod", "has_intraday", "name", "stock_exchange", "symbol"},
// 				},
// 			},
// 			args:    args{jsonText: string(testJsonText), tableName: "sdc_tickers"},
// 			wantSql: `INSERT INTO sdc_tickers (country, has_eod, has_intraday, name, stock_exchange, symbol) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT DO NOTHING`,
// 			wantBindVariables: [][]interface{}{
// 				{
// 					nil, true, false, "Microsoft Corporation", "NASDAQ Stock Exchange", "MSFT",
// 				},
// 				{
// 					nil, true, false, "Microsoft Corporation", "NASDAQ Stock Exchange", "MSFT",
// 				},
// 			},
// 		},
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			d := &JsonToPGSQLConverter{
// 				tableFieldsMap: tt.fields.tableFieldsMap,
// 			}
// 			gotSql, gotBindVariables := d.GenInsert(tt.args.jsonText, tt.args.tableName)
// 			if gotSql != tt.wantSql {
// 				t.Errorf("JsonToPGSQLConverter.GenInsert() gotSql = %v, wantSql %v", gotSql, tt.wantSql)
// 			}
// 			if !reflect.DeepEqual(gotBindVariables, tt.wantBindVariables) {

// 				t.Errorf("JsonToPGSQLConverter.GenInsert() gotBindVariables = %v, wantBindVariables %v", gotBindVariables, tt.wantBindVariables)
// 			}
// 		})
// 	}
// }

// func TestJsonToPGSQLConverter_GenCreateTable(t *testing.T) {
// 	type fields struct {
// 		schema         string
// 		tableFieldsMap map[string][]string
// 	}
// 	type args struct {
// 		jsonText     string
// 		tableName    string
// 		responseType reflect.Type
// 	}
// 	var tickersObj Tickers
// 	tests := []struct {
// 		name   string
// 		fields fields
// 		args   args
// 		want   string
// 	}{
// 		{
// 			name:   "GenCreateTable",
// 			fields: fields{TEST_SCHEMA, JSON_FIELDS_MAP},
// 			args: args{
// 				jsonText:     JSON_TEXT,
// 				tableName:    "sdc_tickers",
// 				responseType: reflect.TypeOf(tickersObj),
// 			},
// 			want: "CREATE TABLE IF NOT EXISTS sdc_tickers (Country varchar(1024), HasEod boolean, HasIntraday boolean, Name varchar(1024), StockExchange varchar(1024), Symbol varchar(1024));",
// 		},
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			d := &JsonToPGSQLConverter{
// 				schema:         tt.fields.schema,
// 				tableFieldsMap: tt.fields.tableFieldsMap,
// 			}
// 			if got, _ := d.GenCreateTable(tt.args.jsonText, tt.args.tableName, tt.args.responseType); got != tt.want {
// 				t.Errorf("JsonToPGSQLConverter.GenCreateTable() = %v, want %v", got, tt.want)
// 			}
// 		})
// 	}
// }

// func TestJsonToPGSQLConverter_GenBulkInsertSQLByJsonText(t *testing.T) {
// 	type fields struct {
// 		schema         string
// 		tableFieldsMap map[string][]string
// 	}
// 	type args struct {
// 		jsonText     string
// 		tableName    string
// 		responseType reflect.Type
// 	}
// 	var responseObj Tickers
// 	tests := []struct {
// 		name    string
// 		fields  fields
// 		args    args
// 		want    string
// 		wantErr bool
// 	}{
// 		{
// 			name:    "GenBulkInsertSQLByJsonText",
// 			fields:  fields{schema: "sdc_test", tableFieldsMap: make(map[string][]string)},
// 			args:    args{jsonText: JSON_TEXT2, tableName: "sdc_tickers", responseType: reflect.TypeOf(responseObj)},
// 			want:    "COPY sdc_tickers FROM 'sdc_tickers.csv' DELIMITER ',' CSV",
// 			wantErr: false,
// 		},
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			d := &JsonToPGSQLConverter{
// 				schema:         tt.fields.schema,
// 				tableFieldsMap: tt.fields.tableFieldsMap,
// 			}
// 			got, err := d.GenBulkInsertSQLByJsonText(tt.args.jsonText, tt.args.tableName, tt.args.responseType)
// 			if (err != nil) != tt.wantErr {
// 				t.Errorf("JsonToPGSQLConverter.GenBulkInsertSQLByJsonText() error = %v, wantErr %v", err, tt.wantErr)
// 				return
// 			}
// 			if got != tt.want {
// 				t.Errorf("JsonToPGSQLConverter.GenBulkInsertSQLByJsonText() = %v, want %v", got, tt.want)
// 			}
// 		})
// 	}
// }
