package cache_test

import (
	"os"
	"reflect"
	"testing"

	"github.com/wayming/sdc/cache"
	"github.com/wayming/sdc/dbloader"
	"github.com/wayming/sdc/json2db"
	"github.com/wayming/sdc/sdclogger"
	testcommon "github.com/wayming/sdc/utils"
)

const CACHE_KEY_SYMBOL_TEST = "PROXIESTEST"
const SCHEMA_TEST = "TestLoadSymbols"

const SYMBOL_JSON_TEXT = `[
	{"symbol", "MSFT"}, {"symbol", "AAPL"}
]`

type SymbolJsonEntityStruct struct {
	Symbol string `json:"symbol`
}

func SetupCacheManagerTest(testName string) {
	testcommon.SetupTest(testName)

	dbLoader := dbloader.NewPGLoader(SCHEMA_TEST, &sdclogger.SDCLoggerInstance.Logger)
	dbLoader.Connect(os.Getenv("PGHOST"),
		os.Getenv("PGPORT"),
		os.Getenv("PGUSER"),
		os.Getenv("PGPASSWORD"),
		os.Getenv("PGDATABASE"))
	defer dbLoader.Disconnect()

	dbLoader.CreateSchema(SCHEMA_TEST)

	// Create table
	d := &json2db.JsonToPGSQLConverter{}
	create, _ := d.GenCreateTable("ms_tickers", reflect.TypeFor[SymbolJsonEntityStruct]())
	dbLoader.Exec(create)

	// Insert rows
	dbLoader.LoadByJsonText(SYMBOL_JSON_TEXT, "ms_tickers", reflect.TypeFor[SymbolJsonEntityStruct]())
}

func TeardownCacheManagerTest() {
	dbLoader := dbloader.NewPGLoader(SCHEMA_TEST, &sdclogger.SDCLoggerInstance.Logger)
	dbLoader.Connect(os.Getenv("PGHOST"),
		os.Getenv("PGPORT"),
		os.Getenv("PGUSER"),
		os.Getenv("PGPASSWORD"),
		os.Getenv("PGDATABASE"))
	defer dbLoader.Disconnect()
	dbLoader.DropSchema(SCHEMA_TEST)

	testcommon.TeardownTest()
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
