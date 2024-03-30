package collector

import (
	"log"
	"os"
	"testing"

	"github.com/wayming/sdc/dbloader"
)

const TEST_LOG_FILE = "logs/collector_testlog"

func TestMSCollector_CollectTickers(t *testing.T) {
	type fields struct {
		dbSchema    string
		dbLoader    *dbloader.PGLoader
		logger      *log.Logger
		msAccessKey string
	}

	file, _ := os.OpenFile(TEST_LOG_FILE, os.O_CREATE|os.O_WRONLY|os.O_CREATE, 0666)
	logger := log.New(file, "mscollectortest: ", log.Ldate|log.Ltime)

	dbLoader := dbloader.NewPGLoader(logger)
	dbLoader.Connect(os.Getenv("PGHOST"),
		os.Getenv("PGPORT"),
		os.Getenv("PGUSER"),
		os.Getenv("PGPASSWORD"),
		os.Getenv("PGDATABASE"))

	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "CollectTickers",
			fields: fields{
				dbSchema:    "sdc_test",
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
			dbLoader.CreateSchema(tt.fields.dbSchema)
			if err := collector.CollectTickers(); (err != nil) != tt.wantErr {
				t.Errorf("MSCollector.CollectTickers() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
