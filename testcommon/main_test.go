// mypackage/mypackage_test.go
package testcommon_test

import (
	"os"
	"testing"
)

var teardownFuncs []func()

func RegisterTeardown(fn func()) {
	teardownFuncs = append(teardownFuncs, fn)
}

// TestMain function to handle global setup and teardown
func TestMain(m *testing.M) {
	// ct.GlobalSetup() // Global setup

	exitCode := m.Run() // Run tests

	// ct.GlobalTeardown() // Global teardown
	for _, fn := range teardownFuncs {
		fn()
	}
	os.Exit(exitCode)
}
