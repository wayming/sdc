package collector_test

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"testing"

	"github.com/wayming/sdc/collector"
)

const COMMON_TEST_LOG_FILE_BASE = "logs/collector_testlog"

var testLogger *log.Logger

func setupCommonTest(testName string) {

	logName := COMMON_TEST_LOG_FILE_BASE + "_" + testName + ".log"
	os.Remove(logName)
	file, _ := os.OpenFile(logName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	testLogger = log.New(file, "commontest: ", log.Ldate|log.Ltime)
}

func teardownCommonTest() {
}

func TestReadURL(t *testing.T) {
	type args struct {
		url    string
		params map[string]string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "ReadURL",
			args: args{
				url:    "https://stockanalysis.com/stocks/rds.b",
				params: nil,
			},
			want:    "string body",
			wantErr: false,
		},
	}

	setupCommonTest(t.Name())
	defer teardownCommonTest()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i := 0; i < 100; i++ {
				fmt.Println(strconv.Itoa(i))
				response, err := collector.ReadURL(tt.args.url, tt.args.params)
				if err != nil {
					testLogger.Println(err.Error())
				} else {
					testLogger.Println(response)
				}
				// got, err := collector.ReadURL(tt.args.url, tt.args.params)
				// if (err != nil) != tt.wantErr {
				// 	fmt.Printf("ReadURL() error = %v, wantErr %v", err, tt.wantErr)
				// 	return
				// }
				// if got != tt.want {
				// 	fmt.Printf("ReadURL() = %v, want %v", got, tt.want)
				// } else {
				// 	fmt.Printf("ReadURL() = %v", got)
				// }
			}
		})

	}
}
