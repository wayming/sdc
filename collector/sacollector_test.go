package collector_test

import (
	"log"
	"os"
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
	saTestLogger.Println("Drop schema", SA_TEST_SCHEMA_NAME, "if exists")
	saTestDBLoader.DropSchema(SA_TEST_SCHEMA_NAME)
}

func TestMSCollector_ReadStockAnalysisPage(t *testing.T) {
	type fields struct {
		dbLoader dbloader.DBLoader
		logger   *log.Logger
		dbSchema string
	}
	type args struct {
		url    string
		params map[string]string
	}

	setupSATest(t.Name())

	commonTestConfig := struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		name: "LoadStockAnalysisPage",
		fields: fields{
			dbLoader: saTestDBLoader,
			logger:   saTestLogger,
			dbSchema: MS_TEST_SCHEMA_NAME,
		},
		args: args{
			url:    "",
			params: make(map[string]string, 0),
		},
		wantErr: false,
	}
	stockOverview := commonTestConfig
	stockOverview.args.url = "https://stockanalysis.com/stocks/msft/"
	financialsIncome := commonTestConfig
	financialsIncome.args.url = "https://stockanalysis.com/stocks/msft/financials/?p=quarterly"
	financialsBalanceShet := commonTestConfig
	financialsBalanceShet.args.url = "https://stockanalysis.com/stocks/msft/financials/balance-sheet/?p=quarterly"
	financialsCashFlow := commonTestConfig
	financialsCashFlow.args.url = "https://stockanalysis.com/stocks/msft/financials/cash-flow-statement/?p=quarterly"
	financialsRatios := commonTestConfig
	financialsRatios.args.url = "https://stockanalysis.com/stocks/msft/financials/ratios/?p=quarterly"

	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{stockOverview, financialsIncome, financialsBalanceShet, financialsCashFlow, financialsRatios}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			collector := collector.NewSACollector(
				tt.fields.dbLoader,
				tt.fields.logger,
				tt.fields.dbSchema,
			)
			got, err := collector.ReadStockAnalysisPage(tt.args.url, tt.args.params)
			if (err != nil) != tt.wantErr {
				t.Errorf("MSCollector.ReadStockAnalysisPage() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if len(got) == 0 {
				t.Errorf("MSCollector.ReadStockAnalysisPage() fails to parse %s", tt.args.url)
			}
		})
	}
	teardownSATest()
}