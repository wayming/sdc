package collector_test

import (
	"log"
	"os"
	"testing"

	"github.com/wayming/sdc/collector"
	"github.com/wayming/sdc/dbloader"
	testcommon "github.com/wayming/sdc/utils"
)

const MS_TEST_SCHEMA_NAME = "sdc_test"
const MS_TEST_LOG_FILE_BASE = "logs/collector_testlog"

var msTestDBLoader dbloader.DBLoader
var msTestLogger *log.Logger

func setupMSTest(testName string) {

	logName := MS_TEST_SCHEMA_NAME + "_" + testName + ".log"
	os.Remove(logName)
	file, _ := os.OpenFile(logName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0666)
	msTestLogger = log.New(file, "mscollectortest: ", log.Ldate|log.Ltime)

	msTestDBLoader = dbloader.NewPGLoader(MS_TEST_SCHEMA_NAME, msTestLogger)
	msTestDBLoader.Connect(os.Getenv("PGHOST"),
		os.Getenv("PGPORT"),
		os.Getenv("PGUSER"),
		os.Getenv("PGPASSWORD"),
		os.Getenv("PGDATABASE"))

	msTestDBLoader.DropSchema(MS_TEST_SCHEMA_NAME)
	msTestDBLoader.CreateSchema(MS_TEST_SCHEMA_NAME)
}

func teardownMSTest() {
	defer msTestDBLoader.Disconnect()
	msTestLogger.Println("Drop schema", MS_TEST_SCHEMA_NAME, "if exists")
	msTestDBLoader.DropSchema(MS_TEST_SCHEMA_NAME)
}

func TestMSCollector_CollectTickers(t *testing.T) {
	type fields struct {
		dbLoader    dbloader.DBLoader
		reader      collector.HttpReader
		logger      *log.Logger
		dbSchema    string
		msAccessKey string
	}

	setupMSTest(t.Name())

	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "CollectTickers",
			fields: fields{
				dbLoader:    msTestDBLoader,
				reader:      collector.NewHttpDirectReader(),
				logger:      testcommon.TestLogger(t.Name()),
				dbSchema:    MS_TEST_SCHEMA_NAME,
				msAccessKey: os.Getenv("MSACCESSKEY"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			collector := collector.NewMSCollector(
				tt.fields.dbLoader,
				tt.fields.reader,
				tt.fields.logger,
				tt.fields.dbSchema,
			)
			if total, err := collector.CollectTickers(); (err != nil || total == 0) != tt.wantErr {
				t.Errorf("MSCollector.CollectTickers() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}

	teardownMSTest()
}

func TestMSCollector_CollectEOD(t *testing.T) {
	type fields struct {
		dbLoader    dbloader.DBLoader
		reader      collector.HttpReader
		logger      *log.Logger
		dbSchema    string
		msAccessKey string
	}
	setupMSTest(t.Name())

	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "CollectEOD",
			fields: fields{
				dbLoader:    msTestDBLoader,
				reader:      collector.NewHttpDirectReader(),
				logger:      testcommon.TestLogger(t.Name()),
				dbSchema:    MS_TEST_SCHEMA_NAME,
				msAccessKey: os.Getenv("MSACCESSKEY"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			collector := collector.NewMSCollector(
				tt.fields.dbLoader,
				tt.fields.reader,
				tt.fields.logger,
				tt.fields.dbSchema,
			)
			if total, err := collector.CollectTickers(); (err != nil || total == 0) != tt.wantErr {
				t.Errorf("MSCollector.CollectTickers() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err := collector.CollectEOD(); (err != nil) != tt.wantErr {
				t.Errorf("MSCollector.CollectEOD() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}

	teardownMSTest()
}
