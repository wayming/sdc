package json2db

import (
	"encoding/json"
	"reflect"
	"testing"
)

const JSON_TEXT = `[{
	"name": "Microsoft Corporation",
	"symbol": "MSFT",
	"has_intraday": false,
	"has_eod": true,
	"country": null,
	"stock_exchange": {
		"name": "NASDAQ Stock Exchange",
		"acronym": "NASDAQ",
		"mic": "XNAS",
		"country": "USA",
		"country_code": "US",
		"city": "New York",
		"website": "www.nasdaq.com"
	}}]`

var JSON_OBJS = [2]map[string]interface{}{
	{
		"country":        nil,
		"has_eod":        true,
		"has_intraday":   false,
		"name":           "Microsoft Corporation",
		"stock_exchange": "NASDAQ Stock Exchange",
		"symbol":         "MSFT",
	},
	{"acronym": "NASDAQ",
		"city":         "New York",
		"country":      "USA",
		"country_code": "US",
		"mic":          "XNAS",
		"name":         "NASDAQ Stock Exchange",
		"website":      "www.nasdaq.com",
	},
}

func TestJsonToPGSQLConverter_GenCreateTableSQLByJson(t *testing.T) {
	type fields struct {
		tableFieldsMap map[string][]string
	}
	type args struct {
		jsonText  string
		tableName string
	}
	testJsonText, _ := json.Marshal(JSON_OBJS[0])
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantSql string
	}{
		{
			name:    "CreateTableSQL",
			fields:  fields{},
			args:    args{jsonText: string(testJsonText), tableName: "sdc_tickers"},
			wantSql: `CREATE TABLE sdc_tickers (country text, has_eod boolean, has_intraday boolean, name vchar(1024), stock_exchange vchar(1024), symbol vchar(1024));`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &JsonToPGSQLConverter{
				tableFieldsMap: tt.fields.tableFieldsMap,
			}
			if got := d.GenCreateTableSQLByJson(tt.args.jsonText, tt.args.tableName); got != tt.wantSql {
				t.Errorf("JsonToPGSQLConverter.GenCreateTableSQLByJson() = %v, wantSql %v", got, tt.wantSql)
			}
		})
	}
}

func TestJsonToPGSQLConverter_GenBulkInsertRowsSQLByJson(t *testing.T) {
	type fields struct {
		tableFieldsMap map[string][]string
	}
	type args struct {
		jsonText  string
		tableName string
	}
	jsonObjs := []map[string]interface{}{JSON_OBJS[0], JSON_OBJS[0]}
	testJsonText, _ := json.Marshal(jsonObjs)
	tests := []struct {
		name              string
		fields            fields
		args              args
		wantSql           string
		wantBindVariables [][]interface{}
	}{
		{
			name: "InsertRowsSQL",
			fields: fields{
				map[string][]string{
					"sdc_tickers": {"country", "has_eod", "has_intraday", "name", "stock_exchange", "symbol"},
				},
			},
			args:    args{jsonText: string(testJsonText), tableName: "sdc_tickers"},
			wantSql: `INSERT INTO sdc_tickers (country, has_eod, has_intraday, name, stock_exchange, symbol) VALUES ($1, $2, $3, $4, $5, $6)`,
			wantBindVariables: [][]interface{}{
				{
					nil, true, false, "Microsoft Corporation", "NASDAQ Stock Exchange", "MSFT",
				},
				{
					nil, true, false, "Microsoft Corporation", "NASDAQ Stock Exchange", "MSFT",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &JsonToPGSQLConverter{
				tableFieldsMap: tt.fields.tableFieldsMap,
			}
			gotSql, gotBindVariables := d.GenBulkInsertRowsSQLByJson(tt.args.jsonText, tt.args.tableName)
			if gotSql != tt.wantSql {
				t.Errorf("JsonToPGSQLConverter.GenBulkInsertRowsSQLByJson() gotSql = %v, wantSql %v", gotSql, tt.wantSql)
			}
			if !reflect.DeepEqual(gotBindVariables, tt.wantBindVariables) {

				t.Errorf("JsonToPGSQLConverter.GenBulkInsertRowsSQLByJson() gotBindVariables = %v, wantBindVariables %v", gotBindVariables, tt.wantBindVariables)
			}
		})
	}
}

func TestJsonToPGSQLConverter_FlattenJsonArray(t *testing.T) {
	type fields struct {
		tableFieldsMap map[string][]string
	}
	type args struct {
		jsonText  string
		rootTable string
	}
	tests := []struct {
		name           string
		fields         fields
		args           args
		wantJsonTexts  []map[string]interface{}
		wantTableNames []string
	}{
		{
			name:           "FlattenJsonArray",
			fields:         fields{},
			args:           args{jsonText: JSON_TEXT, rootTable: "sdc_tickers"},
			wantJsonTexts:  JSON_OBJS[:],
			wantTableNames: []string{"sdc_tickers", "sdc_stock_exchange"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &JsonToPGSQLConverter{
				tableFieldsMap: tt.fields.tableFieldsMap,
			}
			got, got1 := d.FlattenJsonArray(tt.args.jsonText, tt.args.rootTable)
			if !reflect.DeepEqual(got, tt.wantJsonTexts) {
				t.Errorf("JsonToPGSQLConverter.FlattenJsonArray() got = %v, want %v", got, tt.wantJsonTexts)
			}
			if !reflect.DeepEqual(got1, tt.wantTableNames) {
				t.Errorf("JsonToPGSQLConverter.FlattenJsonArray() got1 = %v, want %v", got1, tt.wantTableNames)
			}
		})
	}
}
