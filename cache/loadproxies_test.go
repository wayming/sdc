package cache_test

import (
	"os"
	"testing"

	"github.com/wayming/sdc/cache"
	"github.com/wayming/sdc/sdclogger"
	testcommon "github.com/wayming/sdc/utils"
)

func TestLoadProxies(t *testing.T) {
	type args struct {
		proxyFile string
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "TestLoadProxies",
			args: args{
				proxyFile: os.Getenv("SDC_HOME") + "/data/proxies4.txt",
			},
		},
		// TODO: Add test cases.
	}
	testcommon.SetupTest(t.Name())
	defer testcommon.TeardownTest()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := cache.LoadProxies(tt.args.proxyFile); len(got) == 0 {
				t.Errorf("LoadProxies() = %v, expecting more than one available proxy server", got)
			} else {
				sdclogger.SDCLoggerInstance.Printf("LoadProxies() = %v", got)
			}
		})
	}

}
