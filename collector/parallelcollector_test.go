package collector_test

import (
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

	parallel := 2
	yfDBMock := dbloader.NewMockDBLoader(mockCtrl)
	yfDBMock.EXPECT().CreateSchema(config.SchemaName).AnyTimes()
	yfDBMock.EXPECT().Exec("SET search_path TO sdc").AnyTimes()
	yfDBMock.EXPECT().Disconnect().AnyTimes()
	yfDBMock.EXPECT().CreateTableByJsonStruct(
		testcommon.NewStringPatternMatcher(collector.FYDataTables[collector.FY_EOD]+".*"),
		collector.FYDataTypes[collector.FY_EOD]).AnyTimes()
	yfDBMock.EXPECT().LoadByJsonText(
		gomock.Any(),
		collector.FYDataTables[collector.FY_EOD]+"_msft",
		collector.FYDataTypes[collector.FY_EOD]).Times(parallel)

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
		got, err := pc.Execute(parallel)
		if err != nil {
			t.Errorf("ParallelCollector.Execute() error = %v", err)
			return
		}
		if got != int64(parallel) {
			t.Errorf("ParallelCollector.Execute() = %v, want %v", got, parallel)
		}
	})
}
