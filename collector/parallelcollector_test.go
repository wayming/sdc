package collector_test

import (
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"
	. "github.com/wayming/sdc/collector"
	"github.com/wayming/sdc/config"
	"github.com/wayming/sdc/sdclogger"
	testcommon "github.com/wayming/sdc/testcommon"
)

func TestParallelCollector_Execute(t *testing.T) {
	fixture := testcommon.NewMockTestFixture(t)
	defer fixture.Teardown(t)

	parallel := 2
	numSymbols := 4

	fixture.DBExpect().CreateTableByJsonStruct(
		testcommon.NewStringPatternMatcher(FYDataTables[FY_EOD]+".*"),
		FYDataTypes[FY_EOD]).Times(numSymbols)
	fixture.DBExpect().LoadByJsonText(
		gomock.Any(),
		FYDataTables[FY_EOD]+"_msft",
		FYDataTypes[FY_EOD]).Times(numSymbols)
	fixture.DBExpect().RunQuery(testcommon.NewStringPatternMatcher("select symbol from fy_tickers.*"), gomock.Any()).
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

	// Parallel Collector Begin
	fixture.CacheExpect().GetLength(CACHE_KEY_SYMBOL).
		Return(int64(numSymbols), nil).Times(1)

	// Parallel Collect Process
	fixture.CacheExpect().AddToSet(CACHE_KEY_SYMBOL, "MSFT").Times(numSymbols)
	fixture.CacheExpect().
		PopFromSet(CACHE_KEY_SYMBOL).
		Return("MSFT", nil).
		Times(numSymbols) // Return the same symbol
	fixture.CacheExpect().
		PopFromSet(CACHE_KEY_SYMBOL).
		Return("", nil).
		AnyTimes() // No symbol left

	// Parallel Collector End
	fixture.CacheExpect().GetLength(CACHE_KEY_SYMBOL).
		Return(int64(0), nil).AnyTimes()
	fixture.CacheExpect().GetLength(CACHE_KEY_SYMBOL_ERROR).Return(int64(0), nil)
	fixture.CacheExpect().GetLength(CACHE_KEY_SYMBOL_INVALID).Return(int64(0), nil)

	testBuilder := YFWorkerBuilder{}
	testBuilder.WithDB(fixture.DBMock())

	pc := ParallelCollector{
		func() IWorkerBuilder {
			b := YFWorkerBuilder{}
			b.WithDB(fixture.DBMock())
			b.WithReader(NewHttpReader(NewLocalClient()))
			b.WithExporter(NewDBExporter(fixture.DBMock(), config.SchemaName))
			b.WithCache(fixture.CacheMock())
			b.WithLogger(&sdclogger.SDCLoggerInstance.Logger)
			return &b
		},
		fixture.CacheMock(),
		PCParams{},
	}

	t.Run("TestParallelCollector_Execute", func(t *testing.T) {
		err := pc.Execute(parallel)
		if err != nil {
			t.Errorf("ParallelCollector.Execute() error = %v", err)
			return
		}
	})
}
