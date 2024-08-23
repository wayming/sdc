package cache

import (
	"errors"
	"os"
	"os/exec"
	"strings"

	"github.com/wayming/sdc/sdclogger"
)

func LoadProxies(cm ICacheManager, key string, proxyFile string) (int, error) {
	content, err := os.ReadFile(proxyFile)
	if err != nil {
		sdclogger.SDCLoggerInstance.Println("Failed to get proxies from file " + proxyFile + ". Error: " + err.Error())
		return 0, errors.New(err.Error())
	}
	validProxies := testProxies(strings.Split(string(content), "\n"))
	added := 0

	for _, proxy := range validProxies {
		if err := cm.AddToSet(key, proxy); err != nil {
			sdclogger.SDCLoggerInstance.Println(err.Error())
		} else {
			added++
		}
	}
	sdclogger.SDCLoggerInstance.Printf("%d proxy servers loaded into cache.", added)

	return added, nil
}

func isProxyValid(proxy string) bool {
	parts := strings.Split(proxy, ":")
	proxyURL := parts[0] + ":" + parts[1]
	proxyUser := parts[2]
	proxyPassword := parts[3]
	cmd := exec.Command("nc", "-w", "5", "-zv", parts[0], parts[1])
	if err := cmd.Run(); err != nil {
		sdclogger.SDCLoggerInstance.Println("Failed to ping "+proxyURL+". Error: ", err.Error())
		return false
	}
	if cmd.ProcessState.ExitCode() != 0 {
		return false
	}

	cmd = exec.Command("wget",
		"--timeout", "2",
		"-e", "use_proxy=yes",
		"-e", "http_proxy="+proxyURL,
		"--proxy-user", proxyUser,
		"--proxy-password", proxyPassword,
		"-O", "logs/proxy_test.html",
		"-a", "logs/proxy_test.log",
		"http://example.com")
	if err := cmd.Run(); err != nil {
		sdclogger.SDCLoggerInstance.Println("Failed to ping "+proxyURL+". Error: ", err.Error())
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
