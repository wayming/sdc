package collector_test

import (
	"os"
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/wayming/sdc/cache"
	"github.com/wayming/sdc/collector"
	"github.com/wayming/sdc/config"
	"github.com/wayming/sdc/dbloader"
	"github.com/wayming/sdc/sdclogger"
	testcommon "github.com/wayming/sdc/utils"
)

func TestParallelCollector_Execute(t *testing.T) {

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	parallel := 2
	numSymbols := 4
	yfDBMock := dbloader.NewMockDBLoader(mockCtrl)
	yfDBMock.EXPECT().CreateSchema(config.SchemaName).AnyTimes()
	yfDBMock.EXPECT().Exec("SET search_path TO sdc").AnyTimes()
	yfDBMock.EXPECT().Disconnect().AnyTimes()
	yfDBMock.EXPECT().CreateTableByJsonStruct(
		testcommon.NewStringPatternMatcher(collector.FYDataTables[collector.FY_EOD]+".*"),
		collector.FYDataTypes[collector.FY_EOD]).Times(numSymbols)
	yfDBMock.EXPECT().LoadByJsonText(
		gomock.Any(),
		collector.FYDataTables[collector.FY_EOD]+"_msft",
		collector.FYDataTypes[collector.FY_EOD]).Times(numSymbols)
	yfDBMock.EXPECT().RunQuery(testcommon.NewStringPatternMatcher("SELECT symbol FROM fy_tickers.*"), gomock.Any()).
		DoAndReturn(func(sql string, resultType reflect.Type, args ...any) (interface{}, error) {
			// Validate the struct type
			if resultType.NumField() != 1 {
				t.Errorf("Expecting one field for the result struct, however, got %d", resultType.NumField())
			}
			if resultType.Field(0).Type.Kind() != reflect.String {
				t.Errorf("Expecting a string field for the result struct, however, got %v", resultType.Field(0).Type.Kind())
			}

			// Create a slice of the result type
			sliceType := reflect.SliceOf(resultType)
			result := reflect.MakeSlice(sliceType, 0, 0)

			// Create a new instance of result type
			row := reflect.New(resultType).Elem()
			row.Field(0).SetString("MSFT")
			for i := 0; i < numSymbols; i++ {
				result = reflect.Append(result, row)
			}
			return result.Interface(), nil
		})

	yfCacheMock := cache.NewMockICacheManager(mockCtrl)
	yfCacheMock.EXPECT().Connect().AnyTimes()
	yfCacheMock.EXPECT().Disconnect().AnyTimes()

	// Parallel Collector Begin
	yfCacheMock.EXPECT().GetLength(collector.CACHE_KEY_SYMBOL).
		Return(int64(numSymbols), nil).Times(1)

	// Parallel Collect Process
	yfCacheMock.EXPECT().AddToSet(collector.CACHE_KEY_SYMBOL, "MSFT").Times(numSymbols)
	yfCacheMock.EXPECT().
		PopFromSet(collector.CACHE_KEY_SYMBOL).
		Return("MSFT", nil).
		Times(numSymbols) // Return the same symbol
	yfCacheMock.EXPECT().
		PopFromSet(collector.CACHE_KEY_SYMBOL).
		Return("", nil).
		AnyTimes() // No symbol left

	// Parallel Collector End
	yfCacheMock.EXPECT().GetLength(collector.CACHE_KEY_SYMBOL).
		Return(int64(0), nil).AnyTimes()
	yfCacheMock.EXPECT().GetLength(collector.CACHE_KEY_SYMBOL_ERROR).Return(int64(0), nil)
	yfCacheMock.EXPECT().GetLength(collector.CACHE_KEY_SYMBOL_INVALID).Return(int64(0), nil)

	testBuilder := collector.YFWorkerBuilder{}
	testBuilder.WithDB(yfDBMock)

	pc := collector.ParallelCollector{
		func() collector.IWorkerBuilder {
			b := collector.YFWorkerBuilder{}
			b.WithDB(yfDBMock)
			b.WithReader(collector.NewHttpReader(collector.NewLocalClient()))
			b.WithExporter(collector.NewYFDBExporter(yfDBMock, config.SchemaName))
			b.WithCache(yfCacheMock)
			b.WithLogger(&sdclogger.SDCLoggerInstance.Logger)
			return &b
		},
		yfCacheMock,
		collector.PCParams{},
	}

	t.Run("TestParallelCollector_Execute", func(t *testing.T) {
		err := pc.Execute(parallel)
		if err != nil {
			t.Errorf("ParallelCollector.Execute() error = %v", err)
			return
		}
	})
}

func TestNewEODParallelCollector(t *testing.T) {
	t.Run("TestNewEODParallelCollector", func(t *testing.T) {
		c := collector.NewEODParallelCollector(collector.PCParams{
			IsContinue:  false,
			TickersJSON: os.Getenv("SDC_HOME") + "/data/YF/fy_tickers_100.json",
		})
		if err := c.Execute(10); err != nil {
			t.Errorf("NewEODParallelCollector() = %v", err)
		}
	})
}
