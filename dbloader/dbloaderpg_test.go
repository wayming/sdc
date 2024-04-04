package dbloader

import (
	"fmt"
	"log"
	"os"
	"reflect"
	"sort"
	"testing"

	_ "github.com/lib/pq"
)

const JSON_TEXT = `{
    "pagination": {
        "limit": 10000,
        "offset": 0,
        "count": 250,
        "total": 250
    },
    "data": [
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
	]
}`

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

type Tickers struct {
	Name          string `json:"name"`
	Symbol        string `json:"symbol"`
	HasIntraday   bool   `json:"has_intraday"`
	HasEod        bool   `json:"has_eod"`
	Country       string `json:"country"`
	StockExchange struct {
		Name string `json:"name"`
	} `json:"stock_exchange"`
}

const SCHEMA_NAME = "sdc_test"
const LOG_FILE = "logs/dbloaderpg_test.log"

var logger *log.Logger
var loader *PGLoader

func setup() {

	file, err := os.OpenFile(LOG_FILE, os.O_CREATE|os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		log.Fatal("Failed to open log file ", LOG_FILE, ". Error: ", err)
	}
	logger = log.New(file, "sdctest: ", log.Ldate|log.Ltime)
	logger.Println("Recreate test schema", SCHEMA_NAME)

	loader = NewPGLoader(SCHEMA_NAME, logger)
	loader.Connect(os.Getenv("PGHOST"),
		os.Getenv("PGPORT"),
		os.Getenv("PGUSER"),
		os.Getenv("PGPASSWORD"),
		os.Getenv("PGDATABASE"))
	loader.DropSchema(SCHEMA_NAME)
	loader.CreateSchema(SCHEMA_NAME)
}

func teardown() {
	defer loader.Disconnect()
	logger.Println("Drop schema", SCHEMA_NAME, "if exists")
	loader.DropSchema(SCHEMA_NAME)
}

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	teardown()
	os.Exit(code)
}

func TestPGLoader_LoadByJsonText(t *testing.T) {
	type args struct {
		jsonText       string
		tableName      string
		jsonStructType reflect.Type
	}
	tests := []struct {
		name        string
		args        args
		want        int64
		wantErr     bool
		wantSymbols []string
	}{
		{
			name: "LoadByJsonText",
			args: args{
				jsonText:       JSON_TEXT2,
				tableName:      "sdc_tickers",
				jsonStructType: reflect.TypeFor[Tickers](),
			},
			want:        2,
			wantErr:     false,
			wantSymbols: []string{"MSFT", "AAPL1"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := loader.LoadByJsonText(tt.args.jsonText, tt.args.tableName, tt.args.jsonStructType)
			if (err != nil) != tt.wantErr {
				t.Errorf("PGLoader.LoadByJsonText() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("PGLoader.LoadByJsonText() = %v, want %v", got, tt.want)
			}
			results, err := loader.RunQuery("SELECT symbol FROM sdc_tickers", tt.args.jsonStructType)
			if err != nil {
				t.Errorf("PGLoader.RunQuery() error = %v", err)
				return
			}

			tickers, ok := results.([]Tickers)
			if !ok {
				t.Errorf("PGLoader.RunQuery() does not return slice of Tickers")
				return
			}

			var symbols []string
			for _, ticker := range tickers {
				symbols = append(symbols, ticker.Symbol)
			}
			if !reflect.DeepEqual(sort.StringSlice(symbols), sort.StringSlice(tt.wantSymbols)) {
				t.Errorf("PGLoader.RunQuery() = %v, want %v", sort.StringSlice(symbols), sort.StringSlice(tt.wantSymbols))
			}
			fmt.Println(tickers)
		})
	}
}
