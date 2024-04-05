package collector

import (
	"log"
	"os"
	"testing"

	"github.com/wayming/sdc/dbloader"
)

const TEST_SCHEMA_NAME = "sdc_test"
const TEST_LOG_FILE = "logs/collector_testlog"

var logger *log.Logger
var dbLoader *dbloader.PGLoader

func setup() {

	file, _ := os.OpenFile(TEST_LOG_FILE, os.O_CREATE|os.O_WRONLY|os.O_CREATE, 0666)
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

	setup()

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

	setup()

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
