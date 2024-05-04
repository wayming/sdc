package testcommon

import (
	"log"
	"os"

	"github.com/wayming/sdc/sdclogger"
)

func SetupTest(testName string) {

	logName := "logs/" + testName + ".log"
	file, _ := os.OpenFile(logName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)

	// Redirect stdout and stderr to log file
	os.Stdout = file
	os.Stderr = file

	sdclogger.SDCLoggerInstance = sdclogger.NewSDCLoggerByFile(file)
}

func TeardownTest() {
}

func TestLogger(testName string) *log.Logger {
	logFile := "logs/" + testName + ".log"
	os.Remove(logFile)
	file, _ := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	testLogger := log.New(file, "commontest: ", log.Ldate|log.Ltime)
	// Redirect stdout and stderr to log file
	os.Stdout = file
	os.Stderr = file
	return testLogger
}
