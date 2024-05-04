package cache_test

import (
	"log"
	"os"
	"reflect"
	"testing"

	"github.com/wayming/sdc/cache"
	"github.com/wayming/sdc/dbloader"
	testcommon "github.com/wayming/sdc/utils"
)

const CACHE_KEY_SYMBOL_TEST = "PROXIESTEST"
const SCHEMA_TEST = "TestLoadSymbols"

const SYMBOL_JSON_TEXT = `[
	{"symbol": "MSFT"}, {"symbol": "AAPL"}
]`

type SymbolJsonEntityStruct struct {
	Symbol string `json:"symbol`
}

var logger *log.Logger

func SetupCacheManagerTest(testName string) {
	testcommon.SetupTest(testName)
	logger = testcommon.TestLogger(testName)
	dbLoader := dbloader.NewPGLoader(SCHEMA_TEST, logger)
	dbLoader.Connect(os.Getenv("PGHOST"),
		os.Getenv("PGPORT"),
		os.Getenv("PGUSER"),
		os.Getenv("PGPASSWORD"),
		os.Getenv("PGDATABASE"))
	defer dbLoader.Disconnect()

	dbLoader.CreateSchema(SCHEMA_TEST)

	// Add rows
	dbLoader.LoadByJsonText(SYMBOL_JSON_TEXT, "ms_tickers", reflect.TypeFor[SymbolJsonEntityStruct]())
}

func TeardownCacheManagerTest() {
	dbLoader := dbloader.NewPGLoader(SCHEMA_TEST, logger)
	dbLoader.Connect(os.Getenv("PGHOST"),
		os.Getenv("PGPORT"),
		os.Getenv("PGUSER"),
		os.Getenv("PGPASSWORD"),
		os.Getenv("PGDATABASE"))
	defer dbLoader.Disconnect()
	dbLoader.DropSchema(SCHEMA_TEST)

	testcommon.TeardownTest()
	logger = nil
}

func TestLoadSymbols(t *testing.T) {
	type args struct {
		cache      *cache.CacheManager
		key        string
		fromSchema string
	}
	tests := []struct {
		name    string
		args    args
		want    int
		wantErr bool
	}{
		{
			name: "TestLoadSymbols",
			args: args{
				cache:      cache.NewCacheManager(),
				key:        CACHE_KEY_SYMBOL_TEST,
				fromSchema: SCHEMA_TEST,
			},
			want:    2,
			wantErr: false,
		},
	}

	SetupCacheManagerTest(t.Name())
	defer TeardownCacheManagerTest()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := cache.LoadSymbols(tt.args.cache, tt.args.key, tt.args.fromSchema)
			if (err != nil) != tt.wantErr {
				t.Errorf("LoadSymbols() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("LoadSymbols() = %v, want %v", got, tt.want)
			}
		})
	}
}
