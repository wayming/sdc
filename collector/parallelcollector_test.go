package collector_test

import (
	"log"
	"os"
	"reflect"
	"testing"

	"github.com/wayming/sdc/cache"
	"github.com/wayming/sdc/collector"
	"github.com/wayming/sdc/dbloader"
	"github.com/wayming/sdc/sdclogger"
	testcommon "github.com/wayming/sdc/utils"
)

const PARALLEL_COLLECOR_TEST_SCHEMA_NAME = "sdc_test"

var pcTestDBLoader *dbloader.PGLoader
var pcTestLogger *log.Logger
var pcTestCacheManager *cache.CacheManager

func setupParallelCollectorTest(testName string) {
	testcommon.SetupTest(testName)
	collector.CacheCleanup()

	pcTestLogger = testcommon.TestLogger(testName)
	pcTestDBLoader = dbloader.NewPGLoader(PARALLEL_COLLECOR_TEST_SCHEMA_NAME, pcTestLogger)
	pcTestCacheManager = cache.NewCacheManager()
	pcTestDBLoader.Connect(os.Getenv("PGHOST"),
		os.Getenv("PGPORT"),
		os.Getenv("PGUSER"),
		os.Getenv("PGPASSWORD"),
		os.Getenv("PGDATABASE"))

	pcTestDBLoader.DropSchema(PARALLEL_COLLECOR_TEST_SCHEMA_NAME)
	pcTestDBLoader.CreateSchema(PARALLEL_COLLECOR_TEST_SCHEMA_NAME)

	if err := pcTestCacheManager.Connect(); err != nil {
		pcTestLogger.Fatalf("Failed to connect to cache. Error: %s", err.Error())
	}
	if _, err := cache.LoadProxies(
		pcTestCacheManager,
		collector.CACHE_KEY_PROXY,
		os.Getenv("SDC_HOME")+"/data/proxies10.txt"); err != nil {
		pcTestLogger.Fatalf("Failed to load proxy file %s. Error: %s", os.Getenv("SDC_HOME")+"/data/proxies10.txt", err.Error())
	}

	collector := collector.NewSACollector(pcTestDBLoader, nil, pcTestLogger, PARALLEL_COLLECOR_TEST_SCHEMA_NAME)
	if err := collector.CreateTables(); err != nil {
		sdclogger.SDCLoggerInstance.Printf("Failed to create tables. Error: %s", err)
		pcTestLogger.Fatalf("Failed to create tables. Error: %s", err)
	} else {
		pcTestLogger.Println("All tables created")
	}

}

func teardownpcTest() {
	defer pcTestDBLoader.Disconnect()
	defer pcTestCacheManager.Disconnect()

	pcTestDBLoader.DropSchema(PARALLEL_COLLECOR_TEST_SCHEMA_NAME)
	collector.CacheCleanup()

	testcommon.TeardownTest()
}

func TestRedirectedParallelCollector_Execute(t *testing.T) {
	type args struct {
		schemaName string
		parallel   int
	}
	tests := []struct {
		name    string
		c       collector.IParallelCollector
		args    args
		want    int64
		wantErr bool
	}{
		{
			name: "TestRedirectedParallelCollector_Execute",
			c:    collector.NewRedirectedParallelCollector(),
			args: args{
				schemaName: PARALLEL_COLLECOR_TEST_SCHEMA_NAME,
				parallel:   1,
			},
			want:    1,
			wantErr: false,
		},
	}

	setupParallelCollectorTest(t.Name())
	defer teardownpcTest()

	symbol := "fb"
	redirected := "meta"
	pcTestCacheManager.AddToSet(collector.CACHE_KEY_SYMBOL, symbol)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.c.Execute(tt.args.schemaName, tt.args.parallel)
			if (err != nil) != tt.wantErr {
				t.Errorf("RedirectedParallelCollector.Execute() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("RedirectedParallelCollector.Execute() = %v, want %v", got, tt.want)
			}

			symbols, _ := pcTestCacheManager.GetAllFromSet(collector.CACHE_KEY_SYMBOL_REDIRECTED)
			expected := []string{redirected}
			if !reflect.DeepEqual(symbols, expected) {
				t.Errorf("Got %v from %s key, want %v", symbols, collector.CACHE_KEY_SYMBOL_REDIRECTED, expected)
			}
		})
	}
}

func TestFinancialOverviewParallelCollector_Execute(t *testing.T) {
	type args struct {
		schemaName string
		parallel   int
	}
	tests := []struct {
		name    string
		c       collector.IParallelCollector
		args    args
		want    int64
		wantErr bool
	}{
		{
			name: "TestFinancialOverviewParallelCollector_Execute",
			c:    collector.NewFinancialOverviewParallelCollector(),
			args: args{
				schemaName: PARALLEL_COLLECOR_TEST_SCHEMA_NAME,
				parallel:   1,
			},
			want:    1,
			wantErr: true,
		},
	}

	setupParallelCollectorTest(t.Name())
	defer teardownpcTest()

	symbol := "NotExists"
	pcTestCacheManager.AddToSet(collector.CACHE_KEY_SYMBOL, symbol)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.c.Execute(tt.args.schemaName, tt.args.parallel)
			if (err != nil) != tt.wantErr {
				t.Errorf("FinancialOverviewParallelCollector.Execute() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("FinancialOverviewParallelCollector.Execute() = %v, want %v", got, tt.want)
			}

			symbols, _ := pcTestCacheManager.GetAllFromSet(collector.CACHE_KEY_SYMBOL_INVALID)
			expected := []string{symbol}
			if !reflect.DeepEqual(symbols, expected) {
				t.Errorf("Got %v from %s key, want %v", symbols, collector.CACHE_KEY_SYMBOL_REDIRECTED, expected)
			}
		})
	}
}

func TestFinancialDetailsParallelCollector_Execute(t *testing.T) {
	type args struct {
		schemaName string
		parallel   int
	}
	tests := []struct {
		name    string
		c       collector.IParallelCollector
		args    args
		want    int64
		wantErr bool
	}{
		{
			name: "TestFinancialDetailsParallelCollector_Execute",
			c:    collector.NewFinancialDetailsParallelCollector(),
			args: args{
				schemaName: PARALLEL_COLLECOR_TEST_SCHEMA_NAME,
				parallel:   1,
			},
			want:    1,
			wantErr: false,
		},
	}

	setupParallelCollectorTest(t.Name())
	defer teardownpcTest()

	symbol := "msft"
	pcTestCacheManager.AddToSet(collector.CACHE_KEY_SYMBOL, symbol)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.c.Execute(tt.args.schemaName, tt.args.parallel)
			if (err != nil) != tt.wantErr {
				t.Errorf("FinancialDetailsParallelCollector.Execute() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("FinancialDetailsParallelCollector.Execute() = %v, want %v", got, tt.want)
			}

			if numSymbols, _ := pcTestCacheManager.GetLength(collector.CACHE_KEY_SYMBOL_ERROR); numSymbols > 0 {
				t.Errorf("FinancialDetailsParallelCollector.Execute() got %d symbols in %s, want %d", numSymbols, collector.CACHE_KEY_SYMBOL_ERROR, 0)
			}
		})
	}
}
