package testcommon

import (
	"testing"
)

var teardownFuncs []func()

func RegisterTeardown(fn func()) {
	teardownFuncs = append(teardownFuncs, fn)
}
func GetTeardown() []func() {
	return teardownFuncs
}

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

func (suite *TestSuite) Init() {
	suite.Controller.GlobalSetup()
	RegisterTeardown(suite.Controller.GlobalTeardown)
}

func NewTestSuite(c TestSuiteController) *TestSuite {
	ts := &TestSuite{c}
	ts.Init()
	return ts
}

// RunTest executes a test with setup and teardown
func (suite *TestSuite) RunTest(name string, t *testing.T, testFunc func(t *testing.T)) {
	// Call the specific Setup and Teardown of the test suite
	suite.Controller.Setup(t)          // Call base setup
	defer suite.Controller.Teardown(t) // Ensure base teardown is called after the test

	t.Run(name, testFunc) // Execute the test function
}
