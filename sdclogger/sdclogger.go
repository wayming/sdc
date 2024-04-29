package sdclogger

import (
	"log"
	"os"
)

const LOG_FILE = "logs/sdc.log"

type SDCLogger struct {
	log.Logger
}

func NewSDCLoggerByLogName(logFile string) *SDCLogger {
	file, _ := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	return &SDCLogger{
		Logger: *log.New(file, "SDC", log.Ldate|log.Ltime), // Initializing log.Logger directly
	}
}

func NewSDCLoggerByFile(file *os.File) *SDCLogger {
	return &SDCLogger{
		Logger: *log.New(file, "SDC", log.Ldate|log.Ltime), // Initializing log.Logger directly
	}
}

var SDCLoggerInstance *SDCLogger = NewSDCLoggerByLogName(LOG_FILE)
