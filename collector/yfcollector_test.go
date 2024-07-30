package collector_test

import (
	"log"
	"os"
	"reflect"
	"testing"

	"github.com/wayming/sdc/collector"
	"github.com/wayming/sdc/dbloader"
	testcommon "github.com/wayming/sdc/utils"
)

const YF_TEST_SCHEMA_NAME = "yf_test"

var yfTestDBLoader *dbloader.PGLoader
var yfTestLogger *log.Logger

func setupYFTest(testName string) {
	testcommon.SetupTest(testName)
	yfTestLogger = testcommon.TestLogger(testName)
	collector.CacheCleanup()
	yfTestDBLoader = dbloader.NewPGLoader(YF_TEST_SCHEMA_NAME, yfTestLogger)
	yfTestDBLoader.Connect(os.Getenv("PGHOST"),
		os.Getenv("PGPORT"),
		os.Getenv("PGUSER"),
		os.Getenv("PGPASSWORD"),
		os.Getenv("PGDATABASE"))

	yfTestDBLoader.DropSchema(YF_TEST_SCHEMA_NAME)
	yfTestDBLoader.CreateSchema(YF_TEST_SCHEMA_NAME)

	// Load tickes from csv file for testing
	if testName == "TestCollectFinancialsNotFound" {
		collector.CollectTickers(YF_TEST_SCHEMA_NAME, os.Getenv("SDC_HOME")+"/data/tickersNotFound.json")
	} else {
		collector.CollectTickers(YF_TEST_SCHEMA_NAME, os.Getenv("SDC_HOME")+"/data/tickers5.json")
	}
}

func teardownYFTest() {
	defer yfTestDBLoader.Disconnect()
	yfTestDBLoader.DropSchema(YF_TEST_SCHEMA_NAME)
	collector.CacheCleanup()

	testcommon.TeardownTest()
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
			if err := collector.YFCollect(tt.args.schemaName, ""); (err != nil) != tt.wantErr {
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
	bodyType := reflect.TypeFor[collector.FYTickersResponse]()
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
					"results": [
						{
							"symbol": "A",
							"name": "Agilent Technologies, Inc. Common Stock",
							"nasdaq_traded": "Y",
							"exchange": "N",
							"market_category": null,
							"etf": "N",
							"round_lot_size": 100,
							"test_issue": "N",
							"financial_status": null,
							"cqs_symbol": "A",
							"nasdaq_symbol": "A",
							"next_shares": "N"
						}
					],
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
			want:    `[{"symbol":"A","name":"Agilent Technologies, Inc. Common Stock","price":0,"exchange":"N","exchangeShortName":"","type":""}]`,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := collector.ExtractData(tt.args.textJSON, tt.args.t)
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
