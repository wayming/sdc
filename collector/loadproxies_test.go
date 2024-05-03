package collector_test

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
			m := cache.NewCacheManager()
			m.Connect()
			defer m.Disconnect()

			added, err := cache.LoadProxies(tt.args.proxyFile, m)
			if added == 0 && err != nil {
				t.Errorf("Failed to load proxies. Error: %s", err.Error())
			}

			fetched := 0
			for proxy, _ := m.GetProxy(); proxy != ""; proxy, _ = m.GetProxy() {
				sdclogger.SDCLoggerInstance.Printf("Got proxy %s from cache", proxy)
				fetched++

				m.DeleteProxy(proxy)
				sdclogger.SDCLoggerInstance.Printf("Delete proxy %s from cache", proxy)
			}

			if added != fetched {
				t.Errorf("Expecting %d proxies, got %d", added, fetched)
			}

		})
	}

}
