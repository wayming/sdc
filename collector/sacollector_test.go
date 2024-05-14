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
	testcommon.SetupTest(testName)

	collector.CacheCleanup()
	saTestDBLoader = dbloader.NewPGLoader(SA_TEST_SCHEMA_NAME, testcommon.TestLogger(testName))
	saTestDBLoader.Connect(os.Getenv("PGHOST"),
		os.Getenv("PGPORT"),
		os.Getenv("PGUSER"),
		os.Getenv("PGPASSWORD"),
		os.Getenv("PGDATABASE"))

	saTestDBLoader.DropSchema(SA_TEST_SCHEMA_NAME)
	saTestDBLoader.CreateSchema(SA_TEST_SCHEMA_NAME)

	// Load tickes from csv file for testing
	collector.CollectTickers(SA_TEST_SCHEMA_NAME, os.Getenv("SDC_HOME")+"/data/tickers1000.json")
}

func teardownSATest() {
	defer saTestDBLoader.Disconnect()
	saTestDBLoader.DropSchema(SA_TEST_SCHEMA_NAME)
	collector.CacheCleanup()

	testcommon.TeardownTest()
}

func TestSACollector_ReadOverallPage(t *testing.T) {
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
			dbSchema:   SA_TEST_SCHEMA_NAME,
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

func TestSACollector_ReadPageTimeSeries(t *testing.T) {
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
			dbSchema:   SA_TEST_SCHEMA_NAME,
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
				dbSchema:   SA_TEST_SCHEMA_NAME,
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
				dbSchema:   SA_TEST_SCHEMA_NAME,
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
			if got := collector.SearchText(tt.args.node, tt.args.text); (got != nil) != tt.want {
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
				parallel:   5,
				isContinue: false,
			},
			wantErr: false,
		},
	}
	setupSATest(t.Name())
	// defer teardownSATest()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := collector.CollectFinancials(tt.args.schemaName, tt.args.proxyFile, tt.args.parallel, tt.args.isContinue); (err != nil) != tt.wantErr {
				t.Errorf("CollectFinancials() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCollectFinancialsForSymbol(t *testing.T) {
	type args struct {
		schemaName string
		symbol     string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "TestCollectFinancialsForSymbol",
			args: args{
				schemaName: SA_TEST_SCHEMA_NAME,
				symbol:     "aapl",
			},
			wantErr: false,
		},
	}

	setupSATest(t.Name())
	defer teardownSATest()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := collector.CollectFinancialsForSymbol(tt.args.schemaName, tt.args.symbol); (err != nil) != tt.wantErr {
				t.Errorf("CollectFinancialsForSymbol() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestSACollector_ReadAnalystRatingsPage(t *testing.T) {
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
	defer teardownSATest()

	tests := []struct {
		name    string
		fields  fields
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "ReadAnalystRatingsPage",
			fields: fields{
				dbLoader:   saTestDBLoader,
				reader:     collector.NewHttpDirectReader(),
				logger:     testcommon.TestLogger(t.Name()),
				dbSchema:   SA_TEST_SCHEMA_NAME,
				thisSymbol: "nvda",
			},
			args: args{
				url:                "https://stockanalysis.com/stocks/nvda/ratings/",
				params:             make(map[string]string, 0),
				dataStructTypeName: reflect.TypeFor[collector.AnalystsRating]().Name(),
			},
			want:    "json",
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
			got, err := collector.ReadAnalystRatingsPage(tt.args.url, tt.args.params, tt.args.dataStructTypeName)
			if (err != nil) != tt.wantErr {
				t.Errorf("SACollector.ReadAnalystRatingsPage() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("SACollector.ReadAnalystRatingsPage() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTextOfAdjacentDiv(t *testing.T) {
	type args struct {
		html      string
		firstData string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "TestTextOfAdjacentDiv",
			args: args{
				html: `
				<div class="mb-4 grid grid-cols-2 grid-rows-2 divide-contrast rounded-lg border border-contrast bg-contrast shadow md:grid-cols-4 md:grid-rows-1 md:divide-x">
				<div class="p-4 bp:p-5 sm:p-6">
					<div class="text-sm font-normal text-default xs:text-base">Total Analysts <span class="relative" role="tooltip"><span
								class="absolute -right-[13px] -top-[3px] cursor-pointer p-1 text-gray-300 hover:text-gray-600 dark:text-dark-400 dark:hover:text-dark-300">
								<!-- HTML_TAG_START --><svg class="h-[9px] w-[9px]" viewBox="0 0 4 16"
									fill="currentColor" style="max-width:20px">
									<path
										d="M0 6h4v10h-4v-10zm2-6c1.1 0 2 .9 2 2s-.9 2-2 2-2-.9-2-2 .9-2 2-2z" />
									</svg><!-- HTML_TAG_END --></span></span>
					</div>
					<div class="mt-1 break-words font-semibold leading-8 text-light tiny:text-lg xs:text-xl sm:text-2xl">
						42</div>
				</div>
				<div class="p-4 bp:p-5 sm:p-6 border-l border-contrast md:border-0">
					<div class="text-sm font-normal text-default xs:text-base">Consensus Rating <span class="relative" role="tooltip"><span
								class="absolute -right-[13px] -top-[3px] cursor-pointer p-1 text-gray-300 hover:text-gray-600 dark:text-dark-400 dark:hover:text-dark-300">
								<!-- HTML_TAG_START --><svg class="h-[9px] w-[9px]" viewBox="0 0 4 16"
									fill="currentColor" style="max-width:20px">
									<path
										d="M0 6h4v10h-4v-10zm2-6c1.1 0 2 .9 2 2s-.9 2-2 2-2-.9-2-2 .9-2 2-2z" />
									</svg><!-- HTML_TAG_END --></span></span>
					</div>
					<div class="mt-1 break-words font-semibold leading-8 text-light tiny:text-lg xs:text-xl sm:text-2xl">
						Strong Buy</div>
				</div>
				<div class="p-4 bp:p-5 sm:p-6 border-t border-contrast md:border-0">
					<div class="text-sm font-normal text-default xs:text-base">Price Target <span class="relative" role="tooltip"><span
								class="absolute -right-[13px] -top-[3px] cursor-pointer p-1 text-gray-300 hover:text-gray-600 dark:text-dark-400 dark:hover:text-dark-300">
								<!-- HTML_TAG_START --><svg class="h-[9px] w-[9px]" viewBox="0 0 4 16"
									fill="currentColor" style="max-width:20px">
									<path
										d="M0 6h4v10h-4v-10zm2-6c1.1 0 2 .9 2 2s-.9 2-2 2-2-.9-2-2 .9-2 2-2z" />
									</svg><!-- HTML_TAG_END --></span></span>
					</div>
					<div class="mt-1 break-words font-semibold leading-8 text-light tiny:text-lg xs:text-xl sm:text-2xl">
						$930.17</div>
				</div>
				<div class="p-4 bp:p-5 sm:p-6 border-t border-contrast md:border-0 border-l border-contrast md:border-0">
					<div class="text-sm font-normal text-default xs:text-base">Upside <span class="relative" role="tooltip"><span
								class="absolute -right-[13px] -top-[3px] cursor-pointer p-1 text-gray-300 hover:text-gray-600 dark:text-dark-400 dark:hover:text-dark-300">
								<!-- HTML_TAG_START --><svg class="h-[9px] w-[9px]" viewBox="0 0 4 16"
									fill="currentColor" style="max-width:20px">
									<path
										d="M0 6h4v10h-4v-10zm2-6c1.1 0 2 .9 2 2s-.9 2-2 2-2-.9-2-2 .9-2 2-2z" />
									</svg><!-- HTML_TAG_END --></span></span>
					</div>
					<div class="mt-1 break-words font-semibold leading-8 text-light tiny:text-lg xs:text-xl sm:text-2xl">
						+3.49%</div>
				</div>
			</div>
				`,
				firstData: "Total Analysts",
			},
			want: "42",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			htmlDoc, _ := html.Parse(strings.NewReader(tt.args.html))
			if got := collector.TextOfAdjacentDiv(htmlDoc, tt.args.firstData); got != tt.want {
				t.Errorf("TextOfAdjacentDiv() = %v, want %v", got, tt.want)
			}
		})
	}
}
