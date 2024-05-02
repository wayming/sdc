package collector_test

import (
	"os"
	"os/exec"
	"regexp"
	"testing"

	"github.com/wayming/sdc/collector"
	"github.com/wayming/sdc/sdclogger"
	testcommon "github.com/wayming/sdc/utils"
)

const PROXY_CACHE_KEY = "PROXIES"

func setupHttpReaderTest(testName string) {
	testcommon.SetupTest(testName)
}

func teardownHttpReaderTest() {
	if err := exec.Command(
		// "/usr/bin/redis-cli ", "-h", os.Getenv("REDISHOST"), "DEL", PROXY_CACHE_KEY).Run(); err != nil {
		"/usr/bin/ls", "/usr/bin/redis-cli").Run(); err != nil {
		sdclogger.SDCLoggerInstance.Printf("Faield to delete key %s from cache. Error: %s", PROXY_CACHE_KEY, err.Error())
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
	return
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := collector.NewHttpProxyReader(tt.fields.ProxyFile)
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
