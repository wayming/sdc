package collector_test

import (
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/wayming/sdc/cache"
	"github.com/wayming/sdc/collector"
	"github.com/wayming/sdc/config"
	"github.com/wayming/sdc/dbloader"
	testcommon "github.com/wayming/sdc/utils"
)

func TestParallelCollector_Execute(t *testing.T) {

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	parallel := 10
	numSymbols := 2
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
			result = reflect.Append(result, row)
			result = reflect.Append(result, row)
			return result.Interface(), nil
		})

	yfCacheMock := cache.NewMockICacheManager(mockCtrl)
	yfCacheMock.EXPECT().Connect().AnyTimes()
	yfCacheMock.EXPECT().Disconnect().AnyTimes()

	yfCacheMock.EXPECT().GetLength(collector.CACHE_KEY_SYMBOL).
		Return(int64(parallel), nil).Times(1)
	yfCacheMock.EXPECT().GetLength(collector.CACHE_KEY_SYMBOL).
		Return(int64(0), nil).AnyTimes()
	yfCacheMock.EXPECT().GetLength(collector.CACHE_KEY_SYMBOL_ERROR).Return(int64(0), nil)
	yfCacheMock.EXPECT().GetLength(collector.CACHE_KEY_SYMBOL_INVALID).Return(int64(0), nil)

	yfCacheMock.EXPECT().
		GetFromSet(collector.CACHE_KEY_SYMBOL).
		Return("msft", nil). // First call returns "msft"
		Times(2)             // Allow for any number of additional calls
	yfCacheMock.EXPECT().
		GetFromSet(collector.CACHE_KEY_SYMBOL).
		Return("", nil). // First call returns "msft"
		AnyTimes()       // Allow for any number of additional calls

	testBuilder := collector.YFWorkerBuilder{}
	testBuilder.WithDB(yfDBMock)

	pc := collector.ParallelCollector{}
	pc.SetWorkerBuilder(&testBuilder)
	pc.SetCacheManager(yfCacheMock)

	t.Run("TestParallelCollector_Execute", func(t *testing.T) {
		err := pc.Execute(parallel)
		if err != nil {
			t.Errorf("ParallelCollector.Execute() error = %v", err)
			return
		}
	})
}
