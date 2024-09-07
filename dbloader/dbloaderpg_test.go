package dbloader_test

import (
	"fmt"
	"reflect"
	"sort"
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
