package sdclogger

import (
	"log"
	"os"
)

const LOG_FILE = "logs/sdc.log"

func NewLoggerByLogName(logFile string) *log.Logger {
	file, _ := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	return log.New(file, "SDC", log.Ldate|log.Ltime)
}

func NewLoggerByFile(file *os.File) *log.Logger {
	return log.New(file, "SDC", log.Ldate|log.Ltime)
}

// Global logger
var SDCLoggerInstance *log.Logger = NewLoggerByLogName(LOG_FILE)
