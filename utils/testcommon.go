package testcommon

import (
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
