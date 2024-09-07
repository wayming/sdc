package dbloader_test

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"reflect"
	"sort"
	"testing"

	testcommon "github.com/wayming/sdc/testcommon"

	"github.com/lib/pq"
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

	connectonString := "host=" + os.Getenv("PGHOST")
	connectonString += " port=" + os.Getenv("PGPORT")
	connectonString += " user=" + os.Getenv("PGUSER")
	connectonString += " password=" + os.Getenv("PGPASSWORD")
	connectonString += " dbname=" + os.Getenv("PGDATABASE")
	connectonString += " sslmode=disable"
	db, err := sql.Open("postgres", connectonString)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create temporary table for bulk insert
	_, err = db.Exec(`
        CREATE TEMP TABLE temp_employees (
            employee_id INT PRIMARY KEY,
            name TEXT,
            position TEXT,
            salary INT
        )
    `)
	if err != nil {
		log.Fatal(err)
	}

	// Start a transaction
	tx, err := db.Begin()
	if err != nil {
		tx.Rollback()
		log.Fatal(err)
	}

	// Prepare COPY command
	stmt, err := tx.Prepare(pq.CopyIn("temp_employees", "employee_id", "name", "position", "salary"))
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()

	// Bulk insert data into the temporary table
	_, err = stmt.Exec(
		1, "John Doe", "Software Engineer", 70000,
		2, "Jane Smith", "Data Scientist", 75000,
		3, "Emily Davis", "Product Manager", 80000,
	)
	if err != nil {
		log.Fatal(err)
	}

	// End COPY operation
	_, err = stmt.Exec()
	if err != nil {
		log.Fatal(err)
	}

	// Upsert from temporary table into the main table
	_, err = db.Exec(`
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
		log.Fatal(err)
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Bulk insert and upsert operation completed successfully")
}
