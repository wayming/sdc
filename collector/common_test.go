package collector_test

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"testing"

	"github.com/wayming/sdc/collector"
)

const COMMON_TEST_LOG_FILE_BASE = "logs/collector_testlog"

var GET_PAGE = os.Getenv("SDC_HOME") + "/utils/getpage.sh"

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
		url       string
		params    map[string]string
		repeats   int
		parallel  int
		proxyFile string
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
				url:       "https://stockanalysis.com/stocks/msft",
				params:    nil,
				repeats:   1000,
				parallel:  20,
				proxyFile: os.Getenv("SDC_HOME") + "/data/proxies.txt",
			},
			want:    "string body",
			wantErr: false,
		},
	}

	setupCommonTest(t.Name())
	defer teardownCommonTest()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inChan := make(chan string, tt.args.repeats)
			ouChan := make(chan string, tt.args.repeats)

			for i := 0; i < tt.args.parallel; i++ {
				go func(in chan string, ou chan string) {
					for cmd := range in {
						err := exec.Command("/bin/bash", "-c", cmd).Run()
						if err != nil {
							t.Logf("Failed to run comand %s, Error: %s", cmd, err.Error())

						} else {
							t.Logf("%s command done", cmd)
						}
						ouChan <- "Done"
					}
				}(inChan, ouChan)
			}

			proxies := collector.GetProxies(tt.args.proxyFile)
			t.Logf("Got valid proxy servers %v", proxies)
			if len(proxies) == 0 {
				t.Fatalf("No valid proxy server found from %s", tt.args.proxyFile)
			}
			for i := 0; i < tt.args.repeats; i++ {
				inChan <- fmt.Sprintf(
					"wget -O logs/page%d.html -a logs/wget%d.html -e use_proxy=yes -e https_proxy=%s %s",
					i, i, proxies[i%len(proxies)], tt.args.url)
			}
			close(inChan)

			for i := 0; i < tt.args.repeats; i++ {
				<-ouChan
			}
			// // outChan := make(chan string)
			// for i := 0; i < 2; i++ {
			// 	// go func(idx int, outChan chan string) {
			// 	goId := strconv.Itoa(i)
			// 	// limiter := rate.NewLimiter(rate.Every(1*time.Second), 1)
			// 	j := 0
			// 	for j < 1000 {
			// 		iterId := goId + "." + strconv.Itoa(j)
			// 		// Wait for the limiter to allow the request
			// 		// if limiter.Allow() == false {
			// 		// 	// If too many requests, wait and retry
			// 		// time.Sleep(1 * time.Second) // Adjust sleep duration as needed
			// 		// 	continue
			// 		// }
			// 		// response, err := collector.ReadURL(tt.args.url, tt.args.params)
			// 		cmd := exec.Command(GET_PAGE, tt.args.url)

			// 		var out bytes.Buffer
			// 		cmd.Stdout = &out
			// 		cmd.Stderr = &out
			// 		err := cmd.Run()
			// 		// cmd.Output()

			// 		if err != nil {
			// 			fmt.Println("Iteration " + iterId + ": " + err.Error())

			// 			fmt.Println(time.Now().String() + "Iteration " + iterId + ": " + err.Error())
			// 			fmt.Println(time.Now().String() + "Iteration " + iterId + ": delay 1 seconds")
			// 			// time.Sleep(1 * time.Second)
			// 		} else {
			// 			// response, err := collector.ReadURL(tt.args.url, tt.args.params)
			// 			cmd := exec.Command("sed", "-n", "\"s#.*marketCap:\\\"\\(.*\\)\",revenue:.*#\\1#p\"", goId+".html")

			// 			var out bytes.Buffer
			// 			cmd.Stdout = &out
			// 			cmd.Stderr = &out
			// 			err := cmd.Run()
			// 			if err == nil {
			// 				fmt.Println(out.String())
			// 				fmt.Println("Iteration " + iterId + ": Done")
			// 				fmt.Println(time.Now().String() + "Iteration " + iterId + ": Done")
			// 				j++
			// 			}

			// 		}
			// 	}
			// 	// outChan <- strconv.Itoa(idx) + "Done"

			// 	// }(i, outChan)
			// }

			// // for i := 0; i < 10; i++ {
			// // 	<-outChan
			// // }

			// // got, err := collector.ReadURL(tt.args.url, tt.args.params)
			// // if (err != nil) != tt.wantErr {
			// // 	fmt.Printf("ReadURL() error = %v, wantErr %v", err, tt.wantErr)
			// // 	return
			// // }
			// // if got != tt.want {
			// // 	fmt.Printf("ReadURL() = %v, want %v", got, tt.want)
			// // } else {
			// // 	fmt.Printf("ReadURL() = %v", got)
			// // }

		})

	}
}

func TestGetProxies(t *testing.T) {
	type args struct {
		textFile string
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		{
			name: "TestGetProxies",
			args: args{
				textFile: os.Getenv("SDC_HOME") + "/data/proxies.txt",
			},
		},
	}

	setupCommonTest(t.Name())
	defer teardownCommonTest()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := collector.GetProxies(tt.args.textFile); len(got) == 0 {
				t.Errorf("GetProxies() fails to get any active proxies")
			} else {
				t.Logf("Got validate proxies %v", got)
			}
		})
	}
}
