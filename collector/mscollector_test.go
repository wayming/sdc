package collector_test

// func TestMSCollector_CollectTickers(t *testing.T) {
// 	type fields struct {
// 		dbLoader    dbloader.DBLoader
// 		reader      collector.IHttpReader
// 		logger      *log.Logger
// 		dbSchema    string
// 		msAccessKey string
// 	}

// 	setupMSTest(t.Name())

// 	tests := []struct {
// 		name    string
// 		fields  fields
// 		wantErr bool
// 	}{
// 		{
// 			name: "CollectTickers",
// 			fields: fields{
// 				dbLoader:    msTestDBLoader,
// 				reader:      collector.NewHttpReader(collector.NewLocalClient()),
// 				logger:      testcommon.TestLogger(t.Name()),
// 				dbSchema:    MS_TEST_SCHEMA_NAME,
// 				msAccessKey: os.Getenv("MSACCESSKEY"),
// 			},
// 		},
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			collector := collector.NewMSCollector(
// 				tt.fields.dbLoader,
// 				tt.fields.reader,
// 				tt.fields.logger,
// 				tt.fields.dbSchema,
// 			)
// 			if total, err := collector.CollectTickers(); (err != nil || total == 0) != tt.wantErr {
// 				t.Errorf("MSCollector.CollectTickers() error = %v, wantErr %v", err, tt.wantErr)
// 			}
// 		})
// 	}

// 	teardownMSTest()
// }

// func TestMSCollector_CollectEOD(t *testing.T) {
// 	type fields struct {
// 		dbLoader    dbloader.DBLoader
// 		reader      collector.IHttpReader
// 		logger      *log.Logger
// 		dbSchema    string
// 		msAccessKey string
// 	}
// 	setupMSTest(t.Name())

// 	tests := []struct {
// 		name    string
// 		fields  fields
// 		wantErr bool
// 	}{
// 		{
// 			name: "CollectEOD",
// 			fields: fields{
// 				dbLoader:    msTestDBLoader,
// 				reader:      collector.NewHttpReader(collector.NewLocalClient()),
// 				logger:      testcommon.TestLogger(t.Name()),
// 				dbSchema:    MS_TEST_SCHEMA_NAME,
// 				msAccessKey: os.Getenv("MSACCESSKEY"),
// 			},
// 		},
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			collector := collector.NewMSCollector(
// 				tt.fields.dbLoader,
// 				tt.fields.reader,
// 				tt.fields.logger,
// 				tt.fields.dbSchema,
// 			)
// 			if total, err := collector.CollectTickers(); (err != nil || total == 0) != tt.wantErr {
// 				t.Errorf("MSCollector.CollectTickers() error = %v, wantErr %v", err, tt.wantErr)
// 			}
// 			if err := collector.CollectEOD(); (err != nil) != tt.wantErr {
// 				t.Errorf("MSCollector.CollectEOD() error = %v, wantErr %v", err, tt.wantErr)
// 			}
// 		})
// 	}

// 	teardownMSTest()
// }
