package cache_test

import (
	"os"
	"testing"

	"github.com/wayming/sdc/cache"
	"github.com/wayming/sdc/sdclogger"
	testcommon "github.com/wayming/sdc/utils"
)

const CACHE_KEY_PROXY_TEST = "PROXIESTEST"

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
			proxyCache := cache.NewCacheManager()
			proxyCache.Connect()
			defer proxyCache.Disconnect()

			added, err := cache.LoadProxies(proxyCache, CACHE_KEY_PROXY_TEST, tt.args.proxyFile)
			if added == 0 && err != nil {
				t.Errorf("Failed to load proxies. Error: %s", err.Error())
			}

			fetched := 0
			for proxy, _ := proxyCache.GetFromSet(CACHE_KEY_PROXY_TEST); proxy != ""; proxy, _ = proxyCache.GetFromSet(CACHE_KEY_PROXY_TEST) {
				sdclogger.SDCLoggerInstance.Printf("Got proxy %s from cache key %s", proxy, CACHE_KEY_PROXY_TEST)
				fetched++

				proxyCache.DeleteFromSet(CACHE_KEY_PROXY_TEST, proxy)
				sdclogger.SDCLoggerInstance.Printf("Delete proxy %s from cache key %s", proxy, CACHE_KEY_PROXY_TEST)
			}

			if added != fetched {
				t.Errorf("Expecting %d proxies, got %d", added, fetched)
			}

		})
	}

}
