// mypackage/mypackage_test.go
package testcommon_test

import (
	"fmt"
	"testing"

	"github.com/wayming/sdc/testcommon"
)

// MyTestSuite embeds BaseTestSuite to create a specific test suite
type MyController struct {
	Resource string
}

var suite1 *testcommon.TestSuite
var ctl1 MyController

func init() {
	suite1 = testcommon.NewTestSuite(&ctl1)
}

// GlobalSetup overrides the base setup
func (c *MyController) GlobalSetup() {
	fmt.Println("GlobalSetup for MyController") // Custom logic
	c.Resource += "GlobalSetup"
}

// GlobalTeardown overrides the base setup
func (c *MyController) GlobalTeardown() {
	fmt.Println("GlobalTeardown for MyController") // Custom logic
	c.Resource += "GlobalTeardown"

}

// Setup overrides the base setup
func (c *MyController) Setup(t *testing.T) {
	fmt.Println("Custom setup for MyController") // Custom logic
	c.Resource += "setup"

}

// Teardown overrides the base teardown
func (c *MyController) Teardown(t *testing.T) {
	fmt.Println("Custom teardown for MyController") // Custom logic
	c.Resource += "teardown"

}

// Example test function
func TestMyExample(t *testing.T) {
	suite1.RunTest("TestExample", t, func(t *testing.T) {
		t.Logf("TestExample executed with global Resource:%s", ctl1.Resource)
	})
}
