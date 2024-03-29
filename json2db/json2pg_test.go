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
		"country": "US",
		"stock_exchange": {
			"name": "NASDAQ Stock Exchange",
			"acronym": "NASDAQ",
			"mic": "XNAS",
			"country": "USA",
			"country_code": "US",
			"city": "New York",
			"website": "www.nasdaq.com"
		}}]`
const JSON_TEXT2 = `[
	{
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
	},
	{
	  "name": "Apple Inc",
	  "symbol": "AAPL",
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
	}
  ]`

const TEST_SCHEMA = "sdc_test"

var JSON_OBJS = [2]JsonObject{
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

var JSON_FIELDS_MAP = map[string][]string{
	"sdc_tickers": {"country", "has_eod", "has_intraday", "name", "stock_exchange", "symbol"},
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
			fields:  fields{tableFieldsMap: make(map[string][]string)},
			args:    args{jsonText: string(testJsonText), tableName: "sdc_tickers"},
			wantSql: `CREATE TABLE IF NOT EXISTS sdc_tickers (country text, has_eod boolean, has_intraday boolean, name varchar(1024), stock_exchange varchar(1024), symbol varchar(1024));`,
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

func TestJsonToPGSQLConverter_GenInsertSQL(t *testing.T) {
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
			wantSql: `INSERT INTO sdc_tickers (country, has_eod, has_intraday, name, stock_exchange, symbol) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT DO NOTHING`,
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
			gotSql, gotBindVariables := d.GenInsertSQL(tt.args.jsonText, tt.args.tableName)
			if gotSql != tt.wantSql {
				t.Errorf("JsonToPGSQLConverter.GenInsertSQL() gotSql = %v, wantSql %v", gotSql, tt.wantSql)
			}
			if !reflect.DeepEqual(gotBindVariables, tt.wantBindVariables) {

				t.Errorf("JsonToPGSQLConverter.GenInsertSQL() gotBindVariables = %v, wantBindVariables %v", gotBindVariables, tt.wantBindVariables)
			}
		})
	}
}

func TestJsonToPGSQLConverter_FlattenJsonArrayText(t *testing.T) {
	type fields struct {
		tableFieldsMap map[string][]string
	}
	type args struct {
		jsonText  string
		rootTable string
	}
	tests := []struct {
		name         string
		fields       fields
		args         args
		wantJsonObjs map[string][]JsonObject
	}{
		{
			name:   "FlattenJsonArrayText",
			fields: fields{},
			args:   args{jsonText: JSON_TEXT, rootTable: "sdc_tickers"},
			wantJsonObjs: map[string][]JsonObject{
				"sdc_tickers":        JSON_OBJS[:1],
				"sdc_stock_exchange": JSON_OBJS[1:],
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &JsonToPGSQLConverter{
				tableFieldsMap: tt.fields.tableFieldsMap,
			}
			got := d.FlattenJsonArrayText(tt.args.jsonText, tt.args.rootTable)
			if !reflect.DeepEqual(got, tt.wantJsonObjs) {
				t.Errorf("JsonToPGSQLConverter.FlattenJsonArray() got = %v, want %v", got, tt.wantJsonObjs)
			}
		})
	}
}

func TestJsonToPGSQLConverter_GenBulkInsertSQLByJsonObjs(t *testing.T) {
	type fields struct {
		tableFieldsMap map[string][]string
	}
	type args struct {
		jsonObjs  []JsonObject
		tableName string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
	}{
		{
			name:   "GenBulkInsertSQLByJsonObjs",
			fields: fields{JSON_FIELDS_MAP},
			args: args{
				jsonObjs:  JSON_OBJS[:],
				tableName: "sdc_tickers",
			},
			want: "COPY sdc_tickers FROM 'sdc_tickers.csv' DELIMITER ',' CSV",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &JsonToPGSQLConverter{
				tableFieldsMap: tt.fields.tableFieldsMap,
			}
			if got := d.GenBulkInsertSQLByJsonObjs(tt.args.jsonObjs, tt.args.tableName); got != tt.want {
				t.Errorf("JsonToPGSQLConverter.GenBulkInsertSQLByJsonObjs() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestJsonToPGSQLConverter_GenCreateTableSQLByJson2(t *testing.T) {
	type fields struct {
		schema         string
		tableFieldsMap map[string][]string
	}
	type args struct {
		jsonText     string
		tableName    string
		responseType reflect.Type
	}
	var tickersObj Tickers
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
	}{
		{
			name:   "GenCreateTableSQLByJson2",
			fields: fields{TEST_SCHEMA, JSON_FIELDS_MAP},
			args: args{
				jsonText:     JSON_TEXT,
				tableName:    "sdc_tickers",
				responseType: reflect.TypeOf(tickersObj),
			},
			want: "CREATE TABLE IF NOT EXISTS sdc_tickers (Country varchar(1024), HasEod boolean, HasIntraday boolean, Name varchar(1024), StockExchange varchar(1024), Symbol varchar(1024));",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &JsonToPGSQLConverter{
				schema:         tt.fields.schema,
				tableFieldsMap: tt.fields.tableFieldsMap,
			}
			if got, _ := d.GenCreateTableSQLByJson2(tt.args.jsonText, tt.args.tableName, tt.args.responseType); got != tt.want {
				t.Errorf("JsonToPGSQLConverter.GenCreateTableSQLByJson2() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestJsonToPGSQLConverter_GenBulkInsertSQLByJsonText(t *testing.T) {
	type fields struct {
		schema         string
		tableFieldsMap map[string][]string
	}
	type args struct {
		jsonText     string
		tableName    string
		responseType reflect.Type
	}
	var responseObj Tickers
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    string
		wantErr bool
	}{
		{
			name:    "GenBulkInsertSQLByJsonText",
			fields:  fields{schema: "sdc_test", tableFieldsMap: make(map[string][]string)},
			args:    args{jsonText: JSON_TEXT2, tableName: "sdc_tickers", responseType: reflect.TypeOf(responseObj)},
			want:    "COPY sdc_tickers FROM 'sdc_tickers.csv' DELIMITER ',' CSV",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &JsonToPGSQLConverter{
				schema:         tt.fields.schema,
				tableFieldsMap: tt.fields.tableFieldsMap,
			}
			got, err := d.GenBulkInsertSQLByJsonText(tt.args.jsonText, tt.args.tableName, tt.args.responseType)
			if (err != nil) != tt.wantErr {
				t.Errorf("JsonToPGSQLConverter.GenBulkInsertSQLByJsonText() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("JsonToPGSQLConverter.GenBulkInsertSQLByJsonText() = %v, want %v", got, tt.want)
			}
		})
	}
}
