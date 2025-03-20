package collector

import (
	"time"

	"github.com/wayming/sdc/common"
	"github.com/wayming/sdc/config"
	"github.com/wayming/sdc/sdclogger"
)

func NewNDSymbolsFileExporter() *FileExporter {
	dateStr := time.Now().Format("20060102")
	exportPath := config.DataDir + "/" + dateStr + "/tickers"

	if err := common.CreateDirIfNotExists(exportPath); err != nil {
		sdclogger.SDCLoggerInstance.Fatalf("Failed to create directory %s: %v", exportPath, err)
	}
	return &FileExporter{path: exportPath}
}
