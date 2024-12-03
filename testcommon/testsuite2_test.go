// mypackage/mypackage_test.go
package testcommon_test

import (
	"fmt"
	"testing"

	"github.com/wayming/sdc/testcommon"
)

type MyController2 struct {
	Resource string
}

var suite2 *testcommon.TestSuite
var ctl2 MyController2

func init() {
	suite2 = testcommon.NewTestSuite(&ctl2)
}

// GlobalSetup overrides the base setup
func (c *MyController2) GlobalSetup() {
	fmt.Println("GlobalSetup for MyController2") // Custom logic
	c.Resource += "GlobalSetup"
}

// GlobalTeardown overrides the base setup
func (c *MyController2) GlobalTeardown() {
	fmt.Println("GlobalTeardown for MyController2") // Custom logic
	c.Resource += "GlobalTeardown"

}

// Setup overrides the base setup
func (c *MyController2) Setup(t *testing.T) {
	fmt.Println("Custom setup for MyController2") // Custom logic
	c.Resource += "setup"

}

// Teardown overrides the base teardown
func (c *MyController2) Teardown(t *testing.T) {
	fmt.Println("Custom teardown for MyController2") // Custom logic
	c.Resource += "teardown"

}

// Example test function
func TestMyExample2(t *testing.T) {
	suite2.RunTest("TestExample2", t, func(t *testing.T) {
		t.Logf("TestExample executed with global Resource:%s", ctl2.Resource)
	})
}
