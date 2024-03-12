package json2db

import (
	"reflect"
	"testing"
)

func TestJsonToPGSQLConverter_CreateTableSQL(t *testing.T) {
	type args struct {
		jsonText string
	}
	tests := []struct {
		name string
		d    *JsonToPGSQLConverter
		args args
		want []string
	}{
		{
			name: "CreateTableSQL",
			d:    NewJsonToPGSQLConverter(),
			args: args{jsonText: `{
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
				}
			}`},
			want: []string{
				`CREATE TABLE sdc_stock_exchange (acronym vchar(1024), city vchar(1024), country vchar(1024), country_code vchar(1024), mic vchar(1024), name vchar(1024), website vchar(1024));`,
				`CREATE TABLE sdc_tickers (country text, has_eod boolean, has_intraday boolean, name vchar(1024), stock_exchange_name string, symbol vchar(1024));`},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &JsonToPGSQLConverter{}
			if got := d.CreateTableSQL(tt.args.jsonText, "sdc_tickers"); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("JsonToPGSQLConverter.Gen() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestJsonToPGSQLConverter_InsertRowsSQL(t *testing.T) {
	type fields struct {
		tableFieldsMap map[string][]string
	}
	type args struct {
		jsonText  string
		tableName string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []string
	}{
		{
			name: "InsertRowsSQL",
			fields: fields{
				map[string][]string{
					"sdc_tickers": {"country", "has_eod", "has_intraday", "name", "stock_exchange", "symbol"},
				},
			},
			args: args{
				jsonText: `[{
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
				}}]`,
				tableName: "sdc_tickers"},
			want: []string{
				`INSERT INTO sdc_tickers (country, has_eod, has_intraday, name, stock_exchange, symbol) VALUES（$1, $2, $3, $4, $5, $6)`},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &JsonToPGSQLConverter{
				tableFieldsMap: tt.fields.tableFieldsMap,
			}
			sqls, bindVars := d.InsertRowsSQL(tt.args.jsonText, tt.args.tableName)
			if !reflect.DeepEqual(sqls, tt.want) {
				t.Errorf("JsonToPGSQLConverter.InsertRowsSQL() = %v, %v, want %v", sqls, bindVars, tt.want)
			}
			if len(bindVars) == 1 && len(bindVars[0]) == 1 && len(bindVars[0][0]) == len(tt.fields.tableFieldsMap["sdc_tickers"]) {
				t.Log("Got expected bind variables")
			} else {
				t.Errorf("bind variables %v do not match the insert fields %v", bindVars, tt.fields.tableFieldsMap["sdc_tickers"])
			}
		})
	}
}
