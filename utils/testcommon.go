package utils

import (
	"bytes"
	"log"
	"os"
	"os/exec"

	"github.com/wayming/sdc/sdclogger"
)

func SetupTest(testName string) {
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
	sdclogger.SDCLoggerInstance = sdclogger.NewSDCLoggerByFile(file)
	return testLogger
}

func RunReidsCliCommand(redisCmd string) {
	// Create the command
	cmd := exec.Command("redis-cli", "-h", os.Getenv("REDISHOST"))

	// Create a pipe to write commands to redis-cli
	stdin, err := cmd.StdinPipe()
	if err != nil {
		sdclogger.SDCLoggerInstance.Fatalf("Failed to create stdin pipe: %v", err)
	}

	// Create a buffer to capture the output
	var out bytes.Buffer
	cmd.Stdout = &out

	// Start the command
	if err := cmd.Start(); err != nil {
		sdclogger.SDCLoggerInstance.Fatalf("Failed to start command: %v", err)
	}

	// Write commands to the pipe
	_, err = stdin.Write([]byte(redisCmd))
	if err != nil {
		sdclogger.SDCLoggerInstance.Fatalf("Failed to write to stdin: %v", err)
	}
	stdin.Close() // Close stdin to indicate that we are done sending input

	// Wait for the command to finish
	if err := cmd.Wait(); err != nil {
		sdclogger.SDCLoggerInstance.Fatalf("Command failed: %v", err)
	}

	// Print the output
	sdclogger.SDCLoggerInstance.Printf("Output:\n%s\n", out.String())
}
