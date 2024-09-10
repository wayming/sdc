package dbloader_test

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"

	testcommon "github.com/wayming/sdc/testcommon"

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

const TEST_TABLE = "test_tickers"

func TestPGLoader_LoadByJsonText(t *testing.T) {

	// Test schema are dropped and recreated for every test case
	testFixture := testcommon.NewPGTestFixture(t)
	defer testFixture.Teardown(t)

	if err := testFixture.Loader().CreateTableByJsonStruct(TEST_TABLE, reflect.TypeFor[Tickers]()); err != nil {
		t.Errorf("Failed to create table %s. Error: %v", TEST_TABLE, err)
	}

	wantInserts := int64(2)
	wantSymbols := []string{"MSFT", "AAPL"}
	t.Run("LoadByJsonText", func(t *testing.T) {
		got, err := testFixture.Loader().LoadByJsonText(JSON_TEXT2, TEST_TABLE, reflect.TypeFor[Tickers]())
		if err != nil {
			t.Errorf("PGLoader.LoadByJsonText() error = %v", err)
			return
		}
		if got != wantInserts {
			t.Errorf("PGLoader.LoadByJsonText() = %v, want %v", got, wantInserts)
		}
		results, err := testFixture.Loader().RunQuery("SELECT symbol FROM test_tickers", reflect.TypeFor[Tickers]())
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
		if !reflect.DeepEqual(sort.StringSlice(symbols), sort.StringSlice(wantSymbols)) {
			t.Errorf("PGLoader.RunQuery() = %v, want %v", sort.StringSlice(symbols), sort.StringSlice(wantSymbols))
		}
		fmt.Println(tickers)
	})
}

func TestPGLoader_BulkInser(t *testing.T) {
	// Test schema are dropped and recreated for every test case
	testFixture := testcommon.NewPGTestFixture(t)
	defer testFixture.Teardown(t)

	// Connection string
	connectionString := "host=" + os.Getenv("PGHOST")
	connectionString += " port=" + os.Getenv("PGPORT")
	connectionString += " user=" + os.Getenv("PGUSER")
	connectionString += " password=" + os.Getenv("PGPASSWORD")
	connectionString += " dbname=" + os.Getenv("PGDATABASE")
	connectionString += " sslmode=disable"

	// Open the database connection
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create a buffer with your CSV data
	csvData := `employee_id,name,position,salary
	1,John Doe,Software Engineer,70000
	2,Jane Smith,Data Scientist,75000
	3,Emily Davis,Product Manager,80000`
	reader := strings.NewReader(csvData) // You could use any io.Reader here, such as an os.File or network stream

	// Start a transaction
	tx, err := db.Begin()
	if err != nil {
		log.Fatal("Failed to begin transaction:", err)
	}

	// Create temporary table for bulk insert
	_, err = tx.Exec(`
			CREATE TEMP TABLE temp_employees (
				employee_id INT PRIMARY KEY,
				name TEXT,
				position TEXT,
				salary INT
			)
		`)
	if err != nil {
		tx.Rollback()
		log.Fatal("Failed to create temporary table:", err)
	}

	// Perform the COPY operation with io.Reader
	_, err = tx.CopyFrom(
		reader,
		"temp_employees",
		[]string{"employee_id", "name", "position", "salary"},
		"CSV",
		"DELIMITER ','",
		"HEADER",
	)
	if err != nil {
		tx.Rollback()
		log.Fatal("Failed to execute COPY command:", err)
	}

	// Upsert from temporary table into the main table
	_, err = tx.Exec(`
			INSERT INTO employees (employee_id, name, position, salary)
			SELECT employee_id, name, position, salary
			FROM temp_employees
			ON CONFLICT (employee_id)
			DO UPDATE SET
				name = EXCLUDED.name,
				position = EXCLUDED.position,
				salary = EXCLUDED.salary
		`)
	if err != nil {
		tx.Rollback()
		log.Fatal("Failed to upsert data:", err)
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		log.Fatal("Failed to commit transaction:", err)
	}

	log.Println("Data successfully loaded and transaction committed.")
}
