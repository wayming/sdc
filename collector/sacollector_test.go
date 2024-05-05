package collector_test

import (
	"log"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/wayming/sdc/collector"
	"github.com/wayming/sdc/dbloader"
	testcommon "github.com/wayming/sdc/utils"
	"golang.org/x/net/html"
)

const SA_TEST_SCHEMA_NAME = "sdc_test"

var saTestDBLoader *dbloader.PGLoader

func setupSATest(testName string) {
	saTestDBLoader = dbloader.NewPGLoader(SA_TEST_SCHEMA_NAME, testcommon.TestLogger(testName))
	saTestDBLoader.Connect(os.Getenv("PGHOST"),
		os.Getenv("PGPORT"),
		os.Getenv("PGUSER"),
		os.Getenv("PGPASSWORD"),
		os.Getenv("PGDATABASE"))

	saTestDBLoader.DropSchema(SA_TEST_SCHEMA_NAME)
	saTestDBLoader.CreateSchema(SA_TEST_SCHEMA_NAME)

	// Load tickes from csv file for testing
	collector.CollectTickers(SA_TEST_SCHEMA_NAME, os.Getenv("SDC_HOME")+"/data/tickers5.json")

}

func teardownSATest() {
	defer saTestDBLoader.Disconnect()
	saTestDBLoader.DropSchema(SA_TEST_SCHEMA_NAME)
}

func TestMSCollector_ReadOverallPage(t *testing.T) {
	type fields struct {
		dbLoader   dbloader.DBLoader
		reader     collector.HttpReader
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
			reader:     collector.NewHttpDirectReader(),
			logger:     testcommon.TestLogger(t.Name()),
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
				tt.fields.reader,
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
		reader     collector.HttpReader
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
			reader:     collector.NewHttpDirectReader(),
			logger:     testcommon.TestLogger(t.Name()),
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
				tt.fields.reader,
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
		reader     collector.HttpReader
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
				reader:     collector.NewHttpDirectReader(),
				logger:     testcommon.TestLogger(t.Name()),
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
				tt.fields.reader,
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
		reader     collector.HttpReader
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
				reader:     collector.NewHttpDirectReader(),
				logger:     testcommon.TestLogger(t.Name()),
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
				tt.fields.reader,
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

func TestSearchText(t *testing.T) {
	type args struct {
		node *html.Node
		text string
	}
	htmlSnippets := "<div>This is a test string.</div>"
	htmlDoc, _ := html.Parse(strings.NewReader(htmlSnippets))
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "TestSearchText",
			args: args{
				node: htmlDoc,
				text: "test",
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := collector.SearchText(tt.args.node, tt.args.text); got != tt.want {
				t.Errorf("SearchText() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCollectFinancials(t *testing.T) {
	type args struct {
		schemaName string
		proxyFile  string
		parallel   int
		isContinue bool
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "TestCollectFinancials",
			args: args{
				schemaName: SA_TEST_SCHEMA_NAME,
				proxyFile:  os.Getenv("SDC_HOME") + "/data/proxies7.txt",
				parallel:   20,
				isContinue: false,
			},
			wantErr: false,
		},
	}
	setupSATest(t.Name())
	defer teardownSATest()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := collector.CollectFinancials(tt.args.schemaName, tt.args.proxyFile, tt.args.parallel, tt.args.isContinue); (err != nil) != tt.wantErr {
				t.Errorf("CollectFinancials() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
