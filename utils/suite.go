package utils

import (
	"os"
	"testing"
)

type TestSuiteController interface {
	GlobalSetup()
	GlobalTeardown()
	Setup(t *testing.T)
	Teardown(t *testing.T)
}

// BaseTestSuite is the base struct for all test suites
type BaseTestSuite struct {
	Controller TestSuiteController
}

// RunTest executes a test with setup and teardown
func (suite *BaseTestSuite) RunTest(t *testing.T, testFunc func(t *testing.T)) {
	// Call the specific Setup and Teardown of the test suite
	suite.Controller.Setup(t)          // Call base setup
	defer suite.Controller.Teardown(t) // Ensure base teardown is called after the test

	testFunc(t) // Execute the test function
}

// TestMain function to handle global setup and teardown
func TestMain(m *testing.M, ctl TestSuiteController) {
	ctl.GlobalSetup() // Global setup

	exitCode := m.Run() // Run tests

	ctl.GlobalTeardown() // Global teardown

	os.Exit(exitCode)
}
