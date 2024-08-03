package collector_test

import (
	"encoding/json"
	"log"
	"os"
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"
	. "github.com/wayming/sdc/collector"
	"github.com/wayming/sdc/dbloader"
	testcommon "github.com/wayming/sdc/utils"
)

const YF_TEST_SCHEMA_NAME = "yf_test"

var yfDB *dbloader.PGLoader
var yfDBMock *dbloader.MockDBLoader
var yfTestLogger *log.Logger
var yfReader IHttpReader
var yfExporter YFDataExporter
var yfExporterMock YFDataExporter

func setupYFTest(testName string) {
	testcommon.SetupTest(testName)
	yfTestLogger = testcommon.TestLogger(testName)
	yfDB = dbloader.NewPGLoader(YF_TEST_SCHEMA_NAME, yfTestLogger)
	yfDB.Connect(os.Getenv("PGHOST"),
		os.Getenv("PGPORT"),
		os.Getenv("PGUSER"),
		os.Getenv("PGPASSWORD"),
		os.Getenv("PGDATABASE"))

	yfDB.DropSchema(YF_TEST_SCHEMA_NAME)
	yfDB.CreateSchema(YF_TEST_SCHEMA_NAME)

	yfReader = NewHttpReader(NewLocalClient())
	yfExporter.AddExporter(NewYFFileExporter())
	yfExporter.AddExporter(NewYFDBExporter(yfDB, YF_TEST_SCHEMA_NAME))

}

func teardownYFTest() {
	defer yfDB.Disconnect()
	yfDB.DropSchema(YF_TEST_SCHEMA_NAME)
	testcommon.TeardownTest()
}

func TestYFCollector_Tickers(t *testing.T) {
	type fields struct {
		reader    IHttpReader
		exporters IDataExporter
		db        dbloader.DBLoader
	}

	setupYFTest(t.Name())

	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "TestYFCollector_Tickers",
			fields: fields{
				reader:    yfReader,
				exporters: &yfExporter,
				db:        yfDB,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewYFCollector(tt.fields.reader, tt.fields.exporters, tt.fields.db)
			if err := c.Tickers(); (err != nil) != tt.wantErr {
				t.Errorf("YFTickers() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}

	// teardownYFTest()
}

func TestYFCollector_EOD(t *testing.T) {
	setupYFTest(t.Name())

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	yfDBMock = dbloader.NewMockDBLoader(mockCtrl)
	yfDBMock.EXPECT().CreateSchema(YF_TEST_SCHEMA_NAME)
	yfDBMock.EXPECT().Exec("SET search_path TO yf_test")

	yfDBMock.EXPECT().RunQuery(testcommon.NewStringPatternMatcher("select symbol from fy_tickers.*"), gomock.Any()).
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
	yfDBMock.EXPECT().CreateTableByJsonStruct(testcommon.NewStringPatternMatcher(FYDataTables[FY_EOD]+".*"), FYDataTypes[FY_EOD])
	yfDBMock.EXPECT().LoadByJsonText(gomock.Any(), testcommon.NewStringPatternMatcher(FYDataTables[FY_EOD]+".*"), FYDataTypes[FY_EOD]).
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

	yfExporterMock.AddExporter(NewYFFileExporter())
	yfExporterMock.AddExporter(NewYFDBExporter(yfDBMock, YF_TEST_SCHEMA_NAME))

	t.Run("TestYFCollector_EOD", func(t *testing.T) {
		c := NewYFCollector(yfReader, &yfExporterMock, yfDBMock)
		if err := c.EOD(); err != nil {
			t.Errorf("YFCollector::EOD error=%v", err)
		}
	})

	// teardownYFTest()
}

func TestYFCollect(t *testing.T) {
	type args struct {
		schemaName string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "TestYFCollect",
			args: args{
				schemaName: YF_TEST_SCHEMA_NAME,
			},
		},
	}

	setupYFTest(t.Name())

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := YFCollect(tt.args.schemaName, ""); (err != nil) != tt.wantErr {
				t.Errorf("YFCollect() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}

	// teardownYFTest()
}

func TestExtractData(t *testing.T) {
	type args struct {
		textJSON string
		t        reflect.Type
	}
	bodyType := reflect.TypeFor[FYTickersResponse]()
	inpuJSONText := `[
		{
			"symbol": "A",
			"name": "Agilent Technologies, Inc. Common Stock",
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
	var tickers []FYTickers
	json.Unmarshal([]byte(inpuJSONText), &tickers)
	expectedJSONText, _ := (json.Marshal(tickers))
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "TestExtractData",
			args: args{
				textJSON: `{
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
				}`,
				t: bodyType,
			},
			want:    string(expectedJSONText),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ExtractData(tt.args.textJSON, tt.args.t)
			if (err != nil) != tt.wantErr {
				t.Errorf("ExtractData() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ExtractData() = %v, want %v", got, tt.want)
			}
		})
	}
}
