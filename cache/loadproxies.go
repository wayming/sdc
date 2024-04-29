package cache

import (
	"os"
	"os/exec"
	"strings"

	"github.com/wayming/sdc/sdclogger"
)

func LoadProxies(proxyFile string) []string {
	content, err := os.ReadFile(proxyFile)
	if err != nil {
		sdclogger.SDCLoggerInstance.Println("Failed to get proxies from file " + proxyFile + ". Error: " + err.Error())
		return nil
	}
	validProxies := testProxies(strings.Split(string(content), "\n"))
	return validProxies
}

func isProxyValid(proxy string) bool {
	parts := strings.Split(proxy, ":")
	cmd := exec.Command("nc", "-w", "5", "-zv", parts[0], parts[1])
	if err := cmd.Run(); err != nil {
		sdclogger.SDCLoggerInstance.Println("Faield to ping "+proxy+". Error: ", err.Error())
		return false
	}
	if cmd.ProcessState.ExitCode() != 0 {
		return false
	}

	cmd = exec.Command("wget",
		"--timeout", "2",
		"-e", "use_proxy=yes",
		"-e", "http_proxy="+proxy,
		"-O", "logs/index."+parts[0]+"."+parts[1]+".html",
		"-o", "logs/wget."+parts[0]+"."+parts[1]+".log",
		"https://stockanalysis.com/")
	if err := cmd.Run(); err != nil {
		sdclogger.SDCLoggerInstance.Println("Faield to ping "+proxy+". Error: ", err.Error())
		return false
	}
	if cmd.ProcessState.ExitCode() != 0 {
		return false
	}
	return true
}

func testProxies(proxies []string) []string {
	inChan := make(chan string, len(proxies))
	ouChan := make(chan string, len(proxies))
	defer close(ouChan)

	numWorkers := 20
	for i := 0; i < numWorkers; i++ {
		go func(inChan chan string, ouChan chan string) {
			for proxy := range inChan {
				if isProxyValid(proxy) {
					ouChan <- proxy
				} else {
					ouChan <- ""
				}
			}
		}(inChan, ouChan)
	}

	// Dispatch tasks
	for _, proxy := range proxies {
		inChan <- proxy
	}
	close(inChan)

	// Harvest results
	var validProxies []string
	for i := 0; i < len(proxies); i++ {
		validProxy, ok := <-ouChan
		if ok && len(validProxy) > 0 {
			validProxies = append(validProxies, validProxy)
		}
	}

	return validProxies
}