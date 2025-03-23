package collector

import (
	"reflect"
	"time"

	"github.com/wayming/sdc/cache"
	"github.com/wayming/sdc/common"
	"github.com/wayming/sdc/config"
	"github.com/wayming/sdc/sdclogger"
)

type NDSymbolsCacheExporter struct {
	cache cache.CacheManager
}

func NewNDSymbollCacheExporter() *NDSymbolsCacheExporter {
	c := cache.NewCacheManager()
	c.Connect()
	return &NDSymbolsCacheExporter{cache: *c}
}

func (e NDSymbolsCacheExporter) Export(entityType reflect.Type, table string, data string, symbol string) error {
	return e.cache.AddToSet(config.CACHE_KEY_SYMBOLS, symbol)
}

func NewNDSymbolsFileExporter() *FileExporter {
	dateStr := time.Now().Format("20060102")
	exportPath := config.DATA_DIR + "/" + dateStr + "/tickers"

	if err := common.CreateDirIfNotExists(exportPath); err != nil {
		sdclogger.SDCLoggerInstance.Fatalf("Failed to create directory %s: %v", exportPath, err)
	}
	return &FileExporter{path: exportPath}
}
