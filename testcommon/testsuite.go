package testcommon

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
type TestSuite struct {
	Controller TestSuiteController
}

// RunTest executes a test with setup and teardown
func (suite *TestSuite) RunTest(name string, t *testing.T, testFunc func(t *testing.T)) {
	// Call the specific Setup and Teardown of the test suite
	suite.Controller.Setup(t)          // Call base setup
	defer suite.Controller.Teardown(t) // Ensure base teardown is called after the test

	t.Run(name, testFunc) // Execute the test function
}

// TestMain function to handle global setup and teardown
func MyTestMain(m *testing.M, ct TestSuiteController) {
	ct.GlobalSetup() // Global setup

	exitCode := m.Run() // Run tests

	ct.GlobalTeardown() // Global teardown

	os.Exit(exitCode)
}
