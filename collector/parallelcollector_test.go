package collector_test

import (
	"log"
	"os"
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/wayming/sdc/cache"
	. "github.com/wayming/sdc/collector"
	"github.com/wayming/sdc/config"
	"github.com/wayming/sdc/dbloader"
	"github.com/wayming/sdc/sdclogger"
	testcommon "github.com/wayming/sdc/testcommon"
)

type PCController struct {
	mockCtl   *gomock.Controller
	dbMock    *dbloader.MockDBLoader
	cacheMock *cache.MockICacheManager
	logger    *log.Logger
}

var pcSuite *testcommon.TestSuite
var pcController PCController

func init() {
	pcSuite = testcommon.NewTestSuite(&pcController)
}

// GlobalSetup executed once for each test file
func (c *PCController) GlobalSetup() {
	c.logger = testcommon.TestLogger("yfcollector_test")
	c.logger.Println("GlobalSetup for PCController")
}

// GlobalTeardown executed once for each test file
func (c *PCController) GlobalTeardown() {
	c.logger.Println("GlobalTeardown for PCController")

}

// Setup executed once for each individual test
func (c *PCController) Setup(t *testing.T) {
	c.logger.Println("Custom setup for PCController")
	c.mockCtl = gomock.NewController(t)

	c.dbMock = dbloader.NewMockDBLoader(c.mockCtl)
	c.dbMock.EXPECT().CreateSchema(config.SchemaName)
	c.dbMock.EXPECT().Exec("SET search_path TO yf_test")
	c.dbMock.EXPECT().Disconnect().AnyTimes()

	c.cacheMock = cache.NewMockICacheManager(c.mockCtl)
	c.cacheMock.EXPECT().Connect().AnyTimes()
	c.cacheMock.EXPECT().Disconnect().AnyTimes()

}

// Teardown executed once for each individual test
func (c *PCController) Teardown(t *testing.T) {
	c.logger.Println("Custom teardown for PCController")
	c.mockCtl.Finish()
}

func TestParallelCollector_Execute(t *testing.T) {
	parallel := 2
	numSymbols := 4

	pcController.dbMock.EXPECT().CreateTableByJsonStruct(
		testcommon.NewStringPatternMatcher(FYDataTables[FY_EOD]+".*"),
		FYDataTypes[FY_EOD]).Times(numSymbols)
	pcController.dbMock.EXPECT().LoadByJsonText(
		gomock.Any(),
		FYDataTables[FY_EOD]+"_msft",
		FYDataTypes[FY_EOD]).Times(numSymbols)
	pcController.dbMock.EXPECT().RunQuery(testcommon.NewStringPatternMatcher("SELECT symbol FROM fy_tickers.*"), gomock.Any()).
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
	pcController.cacheMock.EXPECT().GetLength(CACHE_KEY_SYMBOL).
		Return(int64(numSymbols), nil).Times(1)

	// Parallel Collect Process
	pcController.cacheMock.EXPECT().AddToSet(CACHE_KEY_SYMBOL, "MSFT").Times(numSymbols)
	pcController.cacheMock.EXPECT().
		PopFromSet(CACHE_KEY_SYMBOL).
		Return("MSFT", nil).
		Times(numSymbols) // Return the same symbol
	pcController.cacheMock.EXPECT().
		PopFromSet(CACHE_KEY_SYMBOL).
		Return("", nil).
		AnyTimes() // No symbol left

	// Parallel Collector End
	pcController.cacheMock.EXPECT().GetLength(CACHE_KEY_SYMBOL).
		Return(int64(0), nil).AnyTimes()
	pcController.cacheMock.EXPECT().GetLength(CACHE_KEY_SYMBOL_ERROR).Return(int64(0), nil)
	pcController.cacheMock.EXPECT().GetLength(CACHE_KEY_SYMBOL_INVALID).Return(int64(0), nil)

	testBuilder := YFWorkerBuilder{}
	testBuilder.WithDB(pcController.dbMock)

	pc := ParallelCollector{
		func() IWorkerBuilder {
			b := YFWorkerBuilder{}
			b.WithDB(pcController.dbMock)
			b.WithReader(NewHttpReader(NewLocalClient()))
			b.WithExporter(NewDBExporter(pcController.dbMock, config.SchemaName))
			b.WithCache(pcController.cacheMock)
			b.WithLogger(&sdclogger.SDCLoggerInstance.Logger)
			return &b
		},
		pcController.cacheMock,
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

func TestNewEODParallelCollector(t *testing.T) {
	t.Run("TestNewEODParallelCollector", func(t *testing.T) {
		c := NewEODParallelCollector(PCParams{
			IsContinue:  false,
			TickersJSON: os.Getenv("SDC_HOME") + "/data/YF/fy_tickers_100.json",
		})
		if err := c.Execute(10); err != nil {
			t.Errorf("NewEODParallelCollector() = %v", err)
		}
	})
}
