package dbloader

import (
	"log"
	"os"
	"testing"
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
        }
    ]
}`

const SCHEMA_NAME = "sdc-test"
const LOG_FILE = "logs/dbloaderpg_test.log"

var logger *log.Logger
var loader *PGLoader

func setup() {

	file, err := os.OpenFile(LOG_FILE, os.O_CREATE|os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		log.Fatal("Failed to open log file ", LOG_FILE, ". Error: ", err)
	}
	logger = log.New(file, "sdc-test: ", log.Ldate|log.Ltime)
	logger.Println("Recreate test schema", SCHEMA_NAME)

	loader = NewPGLoader(logger)
	loader.Connect(os.Getenv("PGHOST"),
		os.Getenv("PGPORT"),
		os.Getenv("PGUSER"),
		os.Getenv("PGPASSWORD"),
		os.Getenv("PGDATABASE"))
	loader.DropSchema("sdc_test")
	loader.CreateSchema("sdc_test")
}

func teardown() {
	defer loader.Disconnect()
	logger.Println("Drop schema", SCHEMA_NAME, "if exists")
	loader.DropSchema("sdc_test")
}

func TestPGLoader_LoadByJsonResponse(t *testing.T) {
	type args struct {
		JsonResponse string
		tableName    string
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "PGLoader_Load",
			args: args{
				JsonResponse: JSON_TEXT,
				tableName:    "sdc_tickers",
			},
			want: 2,
		},
	}
	if loader == nil {
		logger.Fatal("nil poiner")
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := loader.LoadByJsonResponse(tt.args.JsonResponse, tt.args.tableName); got != tt.want {
				t.Errorf("PGLoader.Load() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	teardown()
	os.Exit(code)
}
