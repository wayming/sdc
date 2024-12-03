// mypackage/mypackage_test.go
package testcommon_test

import (
	"os"
	"testing"

	"github.com/wayming/sdc/testcommon"
)

// TestMain function to handle global setup and teardown
func TestMain(m *testing.M) {
	// ct.GlobalSetup() // Global setup

	exitCode := m.Run() // Run tests

	// ct.GlobalTeardown() // Global teardown
	testcommon.GetTeardown()()

	os.Exit(exitCode)
}
