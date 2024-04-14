package collector_test

import (
	"log"
	"os"
	"reflect"
	"testing"

	"github.com/wayming/sdc/collector"
	"github.com/wayming/sdc/dbloader"
)

const SA_TEST_SCHEMA_NAME = "sdc_test"
const SA_TEST_LOG_FILE_BASE = "logs/collector_testlog"

var saTestDBLoader *dbloader.PGLoader
var saTestLogger *log.Logger

func setupSATest(testName string) {

	logName := SA_TEST_LOG_FILE_BASE + "_" + testName + ".log"
	os.Remove(logName)
	file, _ := os.OpenFile(logName, os.O_CREATE|os.O_WRONLY|os.O_CREATE, 0666)
	saTestLogger = log.New(file, "mscollectortest: ", log.Ldate|log.Ltime)

	saTestDBLoader = dbloader.NewPGLoader(SA_TEST_SCHEMA_NAME, saTestLogger)
	saTestDBLoader.Connect(os.Getenv("PGHOST"),
		os.Getenv("PGPORT"),
		os.Getenv("PGUSER"),
		os.Getenv("PGPASSWORD"),
		os.Getenv("PGDATABASE"))

	saTestDBLoader.DropSchema(SA_TEST_SCHEMA_NAME)
	saTestDBLoader.CreateSchema(SA_TEST_SCHEMA_NAME)
}

func teardownSATest() {
	defer saTestDBLoader.Disconnect()
	// saTestLogger.Println("Drop schema", SA_TEST_SCHEMA_NAME, "if exists")
	// saTestDBLoader.DropSchema(SA_TEST_SCHEMA_NAME)
}

func TestMSCollector_ReadOverallPage(t *testing.T) {
	type fields struct {
		dbLoader   dbloader.DBLoader
		logger     *log.Logger
		dbSchema   string
		thisSymbol string
	}
	type args struct {
		url                string
		params             map[string]string
		dataStructTypeName string
	}

	setupSATest(t.Name())

	commonTestConfig := struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		name: "ReadOverallPage",
		fields: fields{
			dbLoader:   saTestDBLoader,
			logger:     saTestLogger,
			dbSchema:   MS_TEST_SCHEMA_NAME,
			thisSymbol: "msft",
		},
		args: args{
			url:                "",
			params:             make(map[string]string, 0),
			dataStructTypeName: reflect.TypeFor[collector.StockOverview]().Name(),
		},
		wantErr: false,
	}
	stockOverview := commonTestConfig
	stockOverview.args.url = "https://stockanalysis.com/stocks/msft/"
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{stockOverview}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			collector := collector.NewSACollector(
				tt.fields.dbLoader,
				tt.fields.logger,
				tt.fields.dbSchema,
			)
			collector.SetSymbol(tt.fields.thisSymbol)
			got, err := collector.ReadOverallPage(tt.args.url, tt.args.params, tt.args.dataStructTypeName)
			if (err != nil) != tt.wantErr {
				t.Errorf("MSCollector.ReadPage() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if len(got) == 0 {
				t.Errorf("MSCollector.ReadPage() fails to parse %s", tt.args.url)
			}
		})
	}
	teardownSATest()
}

func TestMSCollector_ReadPageTimeSeries(t *testing.T) {
	type fields struct {
		dbLoader   dbloader.DBLoader
		logger     *log.Logger
		dbSchema   string
		thisSymbol string
	}
	type args struct {
		url                string
		params             map[string]string
		dataStructTypeName string
	}

	setupSATest(t.Name())

	commonTestConfig := struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		name: "ReadPageTimeSeries",
		fields: fields{
			dbLoader:   saTestDBLoader,
			logger:     saTestLogger,
			dbSchema:   MS_TEST_SCHEMA_NAME,
			thisSymbol: "msft",
		},
		args: args{
			url:    "",
			params: make(map[string]string, 0),
		},
		wantErr: false,
	}
	financialsIncome := commonTestConfig
	financialsIncome.args.url = "https://stockanalysis.com/stocks/msft/financials/?p=quarterly"
	financialsIncome.args.dataStructTypeName = reflect.TypeFor[collector.FinancialsIncome]().Name()
	financialsBalanceShet := commonTestConfig
	financialsBalanceShet.args.url = "https://stockanalysis.com/stocks/msft/financials/balance-sheet/?p=quarterly"
	financialsBalanceShet.args.dataStructTypeName = reflect.TypeFor[collector.FinancialsBalanceShet]().Name()
	financialsCashFlow := commonTestConfig
	financialsCashFlow.args.url = "https://stockanalysis.com/stocks/msft/financials/cash-flow-statement/?p=quarterly"
	financialsCashFlow.args.dataStructTypeName = reflect.TypeFor[collector.FinancialsCashFlow]().Name()
	financialsRatios := commonTestConfig
	financialsRatios.args.url = "https://stockanalysis.com/stocks/msft/financials/ratios/?p=quarterly"
	financialsRatios.args.dataStructTypeName = reflect.TypeFor[collector.FinancialRatios]().Name()

	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{financialsIncome, financialsBalanceShet, financialsCashFlow, financialsRatios}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			collector := collector.NewSACollector(
				tt.fields.dbLoader,
				tt.fields.logger,
				tt.fields.dbSchema,
			)
			collector.SetSymbol(tt.fields.thisSymbol)

			got, err := collector.ReadTimeSeriesPage(tt.args.url, tt.args.params, tt.args.dataStructTypeName)
			if (err != nil) != tt.wantErr {
				t.Errorf("MSCollector.ReadTimeSeriesPage() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if len(got) == 0 {
				t.Errorf("MSCollector.ReadTimeSeriesPage() fails to parse %s", tt.args.url)
			}
		})
	}
	teardownSATest()
}

func TestSACollector_CollectOverallMetrics(t *testing.T) {
	type fields struct {
		dbLoader   dbloader.DBLoader
		logger     *log.Logger
		dbSchema   string
		thisSymbol string
	}
	type args struct {
		symbol         string
		dataStructType reflect.Type
	}

	setupSATest(t.Name())

	tests := []struct {
		name    string
		fields  fields
		args    args
		want    int64
		wantErr bool
	}{
		{

			name: "CollectOverallMetrics",
			fields: fields{
				dbLoader:   saTestDBLoader,
				logger:     saTestLogger,
				dbSchema:   MS_TEST_SCHEMA_NAME,
				thisSymbol: "msft",
			},
			args: args{
				symbol:         "msft",
				dataStructType: reflect.TypeFor[collector.StockOverview](),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			collector := collector.NewSACollector(
				tt.fields.dbLoader,
				tt.fields.logger,
				tt.fields.dbSchema,
			)

			got, err := collector.CollectOverallMetrics(tt.args.symbol, tt.args.dataStructType)
			if (err != nil) != tt.wantErr {
				t.Errorf("SACollector.CollectOverallMetrics() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got == 0 {
				t.Errorf("SACollector.CollectOverallMetrics() = %v, want %v", got, tt.want)
			}
		})
	}

	teardownSATest()
}

func TestSACollector_CollectFinancialsIncome(t *testing.T) {
	type fields struct {
		dbLoader   dbloader.DBLoader
		logger     *log.Logger
		dbSchema   string
		thisSymbol string
	}
	type args struct {
		symbol         string
		dataStructType reflect.Type
	}

	setupSATest(t.Name())

	tests := []struct {
		name    string
		fields  fields
		args    args
		want    int64
		wantErr bool
	}{
		{

			name: "CollectFinancialsIncome",
			fields: fields{
				dbLoader:   saTestDBLoader,
				logger:     saTestLogger,
				dbSchema:   MS_TEST_SCHEMA_NAME,
				thisSymbol: "msft",
			},
			args: args{
				symbol:         "msft",
				dataStructType: reflect.TypeFor[collector.FinancialsIncome](),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			collector := collector.NewSACollector(
				tt.fields.dbLoader,
				tt.fields.logger,
				tt.fields.dbSchema,
			)
			got, err := collector.CollectFinancialsIncome(tt.args.symbol, tt.args.dataStructType)
			if (err != nil) != tt.wantErr {
				t.Errorf("SACollector.CollectFinancialsIncome() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got <= 0 {
				t.Errorf("SACollector.CollectFinancialsIncome() = %v, want %v", got, tt.want)
			}
		})
	}

	teardownSATest()
}

func Test_stringToFloat64(t *testing.T) {
	type args struct {
		value string
	}
	tests := []struct {
		name    string
		args    args
		want    any
		wantErr bool
	}{
		{
			name:    "Negative",
			args:    args{value: "-5"},
			want:    float64(-5),
			wantErr: false,
		},
		{
			name:    "NegativePercentage",
			args:    args{value: "-12.45%"},
			want:    float64(-0.1245),
			wantErr: false,
		},
		{
			name:    "NegativePercentage",
			args:    args{value: "-"},
			want:    float64(0),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := collector.StringToFloat64(tt.args.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("stringToFloat64() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("stringToFloat64() = %v, want %v", got, tt.want)
			}
		})
	}
}
