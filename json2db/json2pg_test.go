package json2db

import (
	"reflect"
	"testing"
)

func TestJsonToPGSQLConverter_CreateTable(t *testing.T) {
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
			name: "Sanity",
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
			if got := d.CreateTable(tt.args.jsonText, "sdc_tickers"); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("JsonToPGSQLConverter.Gen() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestJsonToPGSQLConverter_InsertRows(t *testing.T) {
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
			name: "Sanity",
			fields: fields{
				map[string][]string{
					"sdc_tickers": {"name", "symbol", "has_intraday", "country", "stock_exchange_name"},
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
				`CREATE TABLE sdc_stock_exchange (acronym vchar(1024), city vchar(1024), country vchar(1024), country_code vchar(1024), mic vchar(1024), name vchar(1024), website vchar(1024));`,
				`CREATE TABLE sdc_tickers (country text, has_eod boolean, has_intraday boolean, name vchar(1024), stock_exchange_name string, symbol vchar(1024));`},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &JsonToPGSQLConverter{
				tableFieldsMap: tt.fields.tableFieldsMap,
			}
			if got := d.InsertRows(tt.args.jsonText, tt.args.tableName); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("JsonToPGSQLConverter.InsertRows() = %v, want %v", got, tt.want)
			}
		})
	}
}
