// mypackage/mypackage_test.go
package utils_test

import (
	"fmt"
	"testing"

	"github.com/wayming/sdc/utils"
)

// MyTestSuite embeds BaseTestSuite to create a specific test suite
type MyTestSuiteController struct {
	Resource string
}

// BaseTestSuite is the base struct for all test suites
type MyTestSuite struct {
	utils.BaseTestSuite
}

var suite *MyTestSuite
var controller MyTestSuiteController

// GlobalSetup overrides the base setup
func (c *MyTestSuiteController) GlobalSetup() {
	fmt.Println("GlobalSetup for MyTestSuite") // Custom logic
	c.Resource += "GlobalSetup"
}

// GlobalTeardown overrides the base setup
func (c *MyTestSuiteController) GlobalTeardown() {
	fmt.Println("GlobalTeardown for MyTestSuite") // Custom logic
	c.Resource += "GlobalTeardown"

}

// Setup overrides the base setup
func (c *MyTestSuiteController) Setup(t *testing.T) {
	fmt.Println("Custom setup for MyTestSuite") // Custom logic
	c.Resource += "setup"

}

// Teardown overrides the base teardown
func (c *MyTestSuiteController) Teardown(t *testing.T) {
	fmt.Println("Custom teardown for MyTestSuite") // Custom logic
	c.Resource += "teardown"

}

// TestMain function for the test suite
func TestMain(m *testing.M) {
	suite = &MyTestSuite{utils.BaseTestSuite{Controller: &controller}}
	utils.TestMain(m, &controller) // Pass the suite directly

}

// Example test function
func TestExample(t *testing.T) {
	suite.RunTest(t, func(t *testing.T) {
		t.Log("TestExample executed with global Resource:")
		fmt.Println(controller.Resource)
	})
}
