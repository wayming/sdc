package collector_test

import (
	"os"
	"os/exec"
	"regexp"
	"testing"

	"github.com/wayming/sdc/cache"
	"github.com/wayming/sdc/collector"
	"github.com/wayming/sdc/sdclogger"
	testcommon "github.com/wayming/sdc/utils"
)

const PROXY_CACHE_TEST_KEY = "PROXIESTEST"

func setupHttpReaderTest(testName string) {
	testcommon.SetupTest(testName)
}

func teardownHttpReaderTest() {
	if err := exec.Command(
		// "/usr/bin/redis-cli ", "-h", os.Getenv("REDISHOST"), "DEL", PROXY_CACHE_KEY).Run(); err != nil {
		"/usr/bin/ls", "/usr/bin/redis-cli").Run(); err != nil {
		sdclogger.SDCLoggerInstance.Printf("Failed to delete key %s from cache. Error: %s", PROXY_CACHE_TEST_KEY, err.Error())
	}
	testcommon.TeardownTest()
}

func TestHttpProxyReader_Read(t *testing.T) {
	type fields struct {
		ProxyFile string
	}
	type args struct {
		url    string
		params map[string]string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		contain string
		wantErr bool
	}{
		{
			name: "TestHttpProxyReader_Read",
			fields: fields{
				ProxyFile: os.Getenv("SDC_HOME") + "/data/proxies4.txt",
			},
			args: args{
				url:    "https://stockanalysis.com/stocks/msft",
				params: nil,
			},
			contain: "Microsoft",
			wantErr: false,
		},
	}
	setupHttpReaderTest(t.Name())
	defer teardownHttpReaderTest()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cacheManager := cache.NewCacheManager()
			if err := cacheManager.Connect(); err != nil {
				t.Errorf("Failed to connect to cache. Error: %s", err.Error())
			}
			defer cacheManager.Disconnect()
			if _, err := cache.LoadProxies(cacheManager, PROXY_CACHE_TEST_KEY, tt.fields.ProxyFile); err != nil {
				t.Errorf("Failed to load proxy file %s. Error: %s", tt.fields.ProxyFile, err.Error())

			}
			reader := collector.NewHttpProxyReader(cacheManager, PROXY_CACHE_TEST_KEY, "100")
			got, err := reader.Read(tt.args.url, tt.args.params)
			if (err != nil) != tt.wantErr {
				t.Errorf("HttpProxyReader.Read() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			match, _ := regexp.MatchString(tt.contain, got)
			if !match {
				t.Errorf("Failed to get the exected string %s from %s", tt.contain, got)
			}
		})
	}
}

func TestHttpDirectReader_Read(t *testing.T) {
	type args struct {
		url    string
		params map[string]string
	}
	tests := []struct {
		name    string
		args    args
		contain string
		wantErr bool
	}{
		{
			name: "TestHttpDirectReader_Read",
			args: args{
				url:    "https://stockanalysis.com/stocks/msft",
				params: nil,
			},
			contain: "Microsoft",
			wantErr: false,
		},
	}

	setupHttpReaderTest(t.Name())
	defer teardownHttpReaderTest()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := collector.NewHttpDirectReader()
			got, err := reader.Read(tt.args.url, tt.args.params)
			if (err != nil) != tt.wantErr {
				t.Errorf("HttpProxyReader.Read() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			sdclogger.SDCLoggerInstance.Println(got)
			match, _ := regexp.MatchString(tt.contain, got)
			if !match {
				t.Errorf("Failed to get the exected string %s from %s", tt.contain, got)
			}
		})
	}
}

func TestDirectReader_RedirectedUrl(t *testing.T) {
	type args struct {
		url string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "TestDirectReader_RedirectedUrl",
			args: args{
				url: "https://stockanalysis.com/stocks/fb/financials/?p=quarterly",
			},
			want:    "https://stockanalysis.com/stocks/meta/financials/?period=quarterly",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := collector.NewHttpDirectReader()
			got, err := reader.RedirectedUrl(tt.args.url)
			if (err != nil) != tt.wantErr {
				t.Errorf("RedirectUrl() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("RedirectUrl() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHttpProxyReader_RedirectedUrl(t *testing.T) {
	type fields struct {
		ProxyFile string
	}
	type args struct {
		url    string
		params map[string]string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "TestHttpProxyReader_Read",
			fields: fields{
				ProxyFile: os.Getenv("SDC_HOME") + "/data/proxies7.txt",
			},
			args: args{
				url:    "https://stockanalysis.com/stocks/fb/financials/?p=quarterly",
				params: nil,
			},
			want:    "https://stockanalysis.com/stocks/meta/financials/?period=quarterly",
			wantErr: false,
		},
	}
	setupHttpReaderTest(t.Name())
	defer teardownHttpReaderTest()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cacheManager := cache.NewCacheManager()
			if err := cacheManager.Connect(); err != nil {
				t.Errorf("Failed to connect to cache. Error: %s", err.Error())
			}
			defer cacheManager.Disconnect()
			if _, err := cache.LoadProxies(cacheManager, PROXY_CACHE_TEST_KEY, tt.fields.ProxyFile); err != nil {
				t.Errorf("Failed to load proxy file %s. Error: %s", tt.fields.ProxyFile, err.Error())

			}
			reader := collector.NewHttpProxyReader(cacheManager, PROXY_CACHE_TEST_KEY, "100")
			got, err := reader.RedirectedUrl(tt.args.url)
			if (err != nil) != tt.wantErr {
				t.Errorf("HttpProxyReader.RedirectedUrl() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("HttpProxyReader.RedirectedUrl() = %v, want %v", got, tt.want)
			}
		})
	}
}
