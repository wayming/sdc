package collector_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"
	. "github.com/wayming/sdc/collector"
	"github.com/wayming/sdc/testcommon"
)

func TestYFCollector_Tickers(t *testing.T) {
	fixture := testcommon.NewMockTestFixture(t)
	defer fixture.Teardown(t)

	fixture.DBExpect().CreateTableByJsonStruct(testcommon.NewStringPatternMatcher(YFDataTables[YF_TICKERS]+".*"), YFDataTypes[YF_TICKERS])
	fixture.DBExpect().LoadByJsonText(gomock.Any(), testcommon.NewStringPatternMatcher(YFDataTables[YF_TICKERS]+".*"), YFDataTypes[YF_TICKERS])
	t.Run("TestYFCollector_Tickers", func(t *testing.T) {
		c := NewYFCollector(fixture.Reader(), fixture.Exporter(), fixture.DBMock(), fixture.Logger())
		if err := c.Tickers(); err != nil {
			t.Errorf("YFTickers() error = %v", err)
		}
	})
}

func TestYFCollector_EOD(t *testing.T) {
	fixture := testcommon.NewMockTestFixture(t)
	defer fixture.Teardown(t)

	fixture.DBExpect().RunQuery(testcommon.NewStringPatternMatcher("select symbol from fy_tickers.*"), gomock.Any()).
		DoAndReturn(func(sql string, resultType reflect.Type, args ...any) (interface{}, error) {
			// Validate the struct type
			if resultType.NumField() != 1 {
				t.Errorf("Expecting one field for the result struct, however, got %d", resultType.NumField())
			}
			if resultType.Field(0).Type.Kind() != reflect.String {
				t.Errorf("Expecting a string field for the result struct, however, got %v", resultType.Field(0).Type.Kind())
			}

			// Create a slice of the result type
			sliceType := reflect.SliceOf(resultType)
			result := reflect.MakeSlice(sliceType, 0, 0)

			// Create a new instance of result type
			row := reflect.New(resultType).Elem()
			row.Field(0).SetString("MSFT")
			result = reflect.Append(result, row)
			return result.Interface(), nil
		})
	fixture.DBExpect().CreateTableByJsonStruct(testcommon.NewStringPatternMatcher(YFDataTables[YF_EOD]+".*"), YFDataTypes[YF_EOD])
	fixture.DBExpect().LoadByJsonText(gomock.Any(), testcommon.NewStringPatternMatcher(YFDataTables[YF_EOD]+".*"), YFDataTypes[YF_EOD]).
		DoAndReturn(func(text string, tableName string, structType reflect.Type) (int64, error) {
			countOfFirstField := 0
			var err error
			if countOfFirstField, err = CountMatches(text, `"`+structType.Field(0).Tag.Get("json")+`"`); err != nil {
				t.Errorf("Failed to get the count of entries for field %s. Error: %v", structType.Field(0).Name, err)
			}
			if countOfFirstField == 0 {
				t.Errorf("Got 0 entries for field %s", structType.Field(0).Name)

			}
			for i := 0; i < structType.NumField(); i++ {
				countOfCurrentField, err := CountMatches(text, `"`+structType.Field(i).Tag.Get("json")+`"`)
				if err != nil {
					t.Errorf("Failed to get the count of entries for field %s. Error: %v",
						structType.Field(i).Name, err)
				}
				if countOfCurrentField != countOfFirstField {
					t.Errorf("Field %s and field %s has different occurences. %d vs %d",
						structType.Field(i).Name, structType.Field(0).Name, countOfCurrentField, countOfFirstField)
				}
			}
			return int64(countOfFirstField), nil
		})

	t.Run("TestYFCollector_EOD", func(t *testing.T) {
		c := NewYFCollector(fixture.Reader(), fixture.Exporter(), fixture.DBMock(), fixture.Logger())
		if err := c.EOD(); err != nil {
			t.Errorf("YFCollector::EOD error=%v", err)
		}
	})

	// teardownYFTest()
}

func TestExtractData(t *testing.T) {
	inpuJSONText := `[
		{
			"symbol": "A",
			"name": "Agilent Technologies, In Common Stock",
			"nasdaq_traded": "Y",
			"exchange": "N",
			"market_category": "",
			"etf": "N",
			"round_lot_size": 100,
			"test_issue": "N",
			"financial_status": "",
			"cqs_symbol": "A",
			"nasdaq_symbol": "A",
			"next_shares": "N"
		}
	]`
	textJSON :=
		`{
		"results": ` + inpuJSONText + `,
		"provider": "nasdaq",
		"warnings": [
			{
				"category": "OpenBBWarning",
				"message": "Parameter 'limit' is not supported by nasdaq. Available for: intrinio."
			},
			{
				"category": "FutureWarning",
				"message": "Downcasting object dtype arrays on .fillna, .ffill, .bfill is deprecated and will change in a future version. Call result.infer_objects(copy=False) instead. To opt-in to the future behavior, set "
			}
		],
		"chart": null,
		"extra": {
			"metadata": {
				"arguments": {
					"provider_choices": {
						"provider": "nasdaq"
					},
					"standard_params": {
						"query": "",
						"is_symbol": false,
						"use_cache": true
					},
					"extra_params": {
						"active": true,
						"limit": 100000,
						"is_etf": null,
						"is_fund": false
					}
				},
				"duration": 4196819148,
				"route": "/equity/search",
				"timestamp": "2024-07-30T12:56:09.154604"
			}
		}
	}`

	var tickers []YFTickers
	json.Unmarshal([]byte(inpuJSONText), &tickers)
	expectedJSONText, _ := (json.Marshal(tickers))

	t.Run("TestExtractData", func(t *testing.T) {
		got, err := ExtractData(textJSON, reflect.TypeFor[FYTickersResponse]())
		if err != nil {
			t.Errorf("ExtractData() error = %v", err)
		}
		if got != string(expectedJSONText) {
			t.Errorf("ExtractData() = %v, want %v", got, expectedJSONText)
		}
	})
}
