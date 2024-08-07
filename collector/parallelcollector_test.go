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

func setupParallelCollectorTest(testName string) {
	testcommon.SetupTest(testName)
}

func teardownpcTest() {
	testcommon.TeardownTest()
}

func TestParallelCollector_Execute(t *testing.T) {
	setupParallelCollectorTest(t.Name())
	defer teardownpcTest()

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	yfDBMock := dbloader.NewMockDBLoader(mockCtrl)
	yfDBMock.EXPECT().CreateSchema(config.SchemaName)
	yfDBMock.EXPECT().Exec("SET search_path TO sdc")
	yfDBMock.EXPECT().Disconnect().AnyTimes().Return()
	yfDBMock.EXPECT().CreateTableByJsonStruct(testcommon.NewStringPatternMatcher(collector.FYDataTables[collector.FY_EOD]+".*"), collector.FYDataTypes[collector.FY_EOD]).Times(10)

	yfCacheMock := cache.NewMockICacheManager(mockCtrl)
	yfCacheMock.EXPECT().Connect().Return(nil)
	yfCacheMock.EXPECT().Disconnect()
	yfCacheMock.EXPECT().GetLength("SYMBOLS").Return(int64(10), nil)
	yfCacheMock.EXPECT().GetFromSet(collector.CACHE_KEY_SYMBOL).AnyTimes().Return("msft", nil)
	testBuilder := collector.YFWorkerBuilder{}
	testBuilder.WithDB(yfDBMock)

	pc := collector.ParallelCollector{}
	pc.SetWorkerBuilder(&testBuilder)
	pc.SetCacheManager(yfCacheMock)

	t.Run("TestParallelCollector_Execute", func(t *testing.T) {
		got, err := pc.Execute(10)
		if err != nil {
			t.Errorf("ParallelCollector.Execute() error = %v", err)
			return
		}
		if got != 100 {
			t.Errorf("ParallelCollector.Execute() = %v, want %v", got, 100)
		}
	})
}
