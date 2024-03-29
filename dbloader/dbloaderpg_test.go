package dbloader

import (
	"fmt"
	"log"
	"os"
	"reflect"
	"testing"

	_ "github.com/lib/pq"
	"github.com/wayming/sdc/json2db"
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
	// loader.DropSchema("sdc_test")
}

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	teardown()
	os.Exit(code)
}

func readFromCSV(csvData string) *os.File {
	f, err := os.CreateTemp("", "csvdata*.csv")
	if err != nil {
		fmt.Println("Error creating temporary file:", err)
		return nil
	}
	defer f.Close()

	_, err = f.WriteString(csvData)
	if err != nil {
		fmt.Println("Error writing to temporary file:", err)
		return nil
	}

	_, err = f.Seek(0, 0)
	if err != nil {
		fmt.Println("Error seeking to the beginning of the file:", err)
		return nil
	}

	return f
}

func TestPGLoader_LoadByJsonText2(t *testing.T) {
	db := loader.db

	copyData := [][]string{
		{"1", "John"},
		{"2", "Jane"},
		{"3", "Alice"},
	}

	// Prepare the COPY data as CSV format
	copyDataCSV := ""
	for _, row := range copyData {
		copyDataCSV += fmt.Sprintf("%s,%s\n", row[0], row[1])
	}

	copyDataCSV = `
	1,John
	2,Jane
	3,Alice
	`
	_, err := db.Exec("COPY sdc_test.target_table(id, name) FROM STDIN WITH CSV", readFromCSV(copyDataCSV))
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Bulk insert using COPY command executed successfully!")

	fmt.Println("COPY command executed successfully!")
}

func TestPGLoader_LoadByJsonText(t *testing.T) {
	type args struct {
		jsonText       string
		tableName      string
		jsonStructType reflect.Type
	}
	var jsonStruct json2db.Tickers
	tests := []struct {
		name    string
		args    args
		want    int64
		wantErr bool
	}{
		{
			name: "LoadByJsonText",
			args: args{
				jsonText:       JSON_TEXT2,
				tableName:      "sdc_tickers",
				jsonStructType: reflect.TypeOf(jsonStruct),
			},
			want:    2,
			wantErr: false,
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
		})
	}
}
