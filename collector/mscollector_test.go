package collector

import (
	"log"
	"os"
	"testing"

	"github.com/wayming/sdc/dbloader"
)

const TEST_SCHEMA_NAME = "sdc_test"
const TEST_LOG_FILE_BASE = "logs/collector_testlog"

var logger *log.Logger
var dbLoader *dbloader.PGLoader

func setup(testName string) {

	logName := TEST_LOG_FILE_BASE + "_" + testName + ".log"
	os.Remove(logName)
	file, _ := os.OpenFile(logName, os.O_CREATE|os.O_WRONLY|os.O_CREATE, 0666)
	logger = log.New(file, "mscollectortest: ", log.Ldate|log.Ltime)

	dbLoader = dbloader.NewPGLoader(TEST_SCHEMA_NAME, logger)
	dbLoader.Connect(os.Getenv("PGHOST"),
		os.Getenv("PGPORT"),
		os.Getenv("PGUSER"),
		os.Getenv("PGPASSWORD"),
		os.Getenv("PGDATABASE"))

	dbLoader.DropSchema(TEST_SCHEMA_NAME)
	dbLoader.CreateSchema(TEST_SCHEMA_NAME)
}

func teardown() {
	defer dbLoader.Disconnect()
	logger.Println("Drop schema", TEST_SCHEMA_NAME, "if exists")
	dbLoader.DropSchema(TEST_SCHEMA_NAME)
}

func TestMSCollector_CollectTickers(t *testing.T) {
	type fields struct {
		dbSchema    string
		dbLoader    *dbloader.PGLoader
		logger      *log.Logger
		msAccessKey string
	}

	setup(t.Name())

	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "CollectTickers",
			fields: fields{
				dbSchema:    TEST_SCHEMA_NAME,
				dbLoader:    dbLoader,
				logger:      logger,
				msAccessKey: os.Getenv("MSACCESSKEY"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			collector := &MSCollector{
				dbSchema:    tt.fields.dbSchema,
				dbLoader:    tt.fields.dbLoader,
				logger:      tt.fields.logger,
				msAccessKey: tt.fields.msAccessKey,
			}
			if err := collector.CollectTickers(); (err != nil) != tt.wantErr {
				t.Errorf("MSCollector.CollectTickers() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}

	teardown()
}

func TestMSCollector_CollectEOD(t *testing.T) {
	type fields struct {
		dbSchema    string
		dbLoader    *dbloader.PGLoader
		logger      *log.Logger
		msAccessKey string
	}

	setup(t.Name())

	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "CollectEOD",
			fields: fields{
				dbSchema:    TEST_SCHEMA_NAME,
				dbLoader:    dbLoader,
				logger:      logger,
				msAccessKey: os.Getenv("MSACCESSKEY"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			collector := &MSCollector{
				dbSchema:    tt.fields.dbSchema,
				dbLoader:    tt.fields.dbLoader,
				logger:      tt.fields.logger,
				msAccessKey: tt.fields.msAccessKey,
			}
			if err := collector.CollectTickers(); (err != nil) != tt.wantErr {
				t.Errorf("MSCollector.CollectTickers() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err := collector.CollectEOD(); (err != nil) != tt.wantErr {
				t.Errorf("MSCollector.CollectEOD() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}

	teardown()
}

func TestMSCollector_ReadStockAnalysisPage(t *testing.T) {
	type fields struct {
		dbSchema    string
		dbLoader    dbloader.DBLoader
		logger      *log.Logger
		msAccessKey string
	}
	type args struct {
		url    string
		params map[string]string
	}

	setup(t.Name())

	commonTestConfig := struct {
		name    string
		fields  fields
		args    args
		want    string
		wantErr bool
	}{
		name: "ReadStockAnalysisPage",
		fields: fields{
			dbSchema:    TEST_SCHEMA_NAME,
			dbLoader:    dbLoader,
			logger:      logger,
			msAccessKey: os.Getenv("MSACCESSKEY"),
		},
		args: args{
			url:    "",
			params: make(map[string]string, 0),
		},
		want:    "",
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
		want    string
		wantErr bool
	}{stockOverview, financialsIncome, financialsBalanceShet, financialsCashFlow, financialsRatios}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			collector := &MSCollector{
				dbSchema:    tt.fields.dbSchema,
				dbLoader:    tt.fields.dbLoader,
				logger:      tt.fields.logger,
				msAccessKey: tt.fields.msAccessKey,
			}
			got, err := collector.ReadStockAnalysisPage(tt.args.url, tt.args.params)
			if (err != nil) != tt.wantErr {
				t.Errorf("MSCollector.ReadStockAnalysisPage() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("MSCollector.ReadStockAnalysisPage() = %v, want %v", got, tt.want)
			}
		})
	}

	teardown()
}
