package collector

import (
	"errors"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/wayming/sdc/cache"
	"github.com/wayming/sdc/sdclogger"
	"golang.org/x/time/rate"
)

var counter int
var counterMutex sync.Mutex

func nextId() int {
	counterMutex.Lock()
	defer counterMutex.Unlock()

	counter++
	return counter
}

type HttpReader interface {
	Read(url string, params map[string]string) (string, error)
}

type HttpProxyReader struct {
	Cache *cache.CacheManager
	key   string
}

func HttpCode(url string) (int, error) {
	cmd := exec.Command("curl", "-w", "%{http_code}", url)
	output, err := cmd.Output()
	if err != nil {
		return 0, err
	}

	httpCode, err := strconv.Atoi(string(output))
	if err != nil {
		return 0, err
	}

	return httpCode, nil
}

func NewHttpProxyReader(cache *cache.CacheManager, goID string) *HttpProxyReader {
	reader := &HttpProxyReader{Cache: cache, key: goID}
	return reader
}

func (reader *HttpProxyReader) Read(url string, params map[string]string) (string, error) {
	fileName := strings.ReplaceAll(url, "http://", "")
	fileName = strings.ReplaceAll(fileName, "/", "_")
	htmlFile := "logs/reader" + reader.key + "_" + fileName + ".html"
	for {
		proxy, err := reader.Cache.GetFromSet(CACHE_KEY_PROXY)
		if err != nil {
			return "", err
		}
		if proxy == "" {
			return "", errors.New("no proxy server available")
		}

		cmd := exec.Command("wget",
			"--timeout=10", "--tries=1",
			"-O", htmlFile,
			"-a", "logs/reader"+reader.key+"_wget.log",
			"-e", "use_proxy=yes",
			"--proxy-user="+os.Getenv("PROXYUSER"),
			"--proxy-password="+os.Getenv("PROXYPASSWORD"),
			"-e", "https_proxy="+proxy, url)
		err = cmd.Run()
		if err != nil {
			sdclogger.SDCLoggerInstance.Printf("Reader[%s]: Failed to run comand [%s], Error: %s", reader.key, strings.Join(cmd.Args, " "), err.Error())
			if cmd.ProcessState.ExitCode() == 8 {
				httpCode, httpCodeErr := HttpCode(url)
				if httpCodeErr != nil {
					sdclogger.SDCLoggerInstance.Printf("Failed to get http code for url %s", url)
				}

				if httpCode == 301 {
					sdclogger.SDCLoggerInstance.Printf("url %s has been redirected.", url)
				}

				sdclogger.SDCLoggerInstance.Println("Do not retry for server error response.")
				return "", NewHttpServerError(err.Error(), httpCode)
			}

			// Try next proxy
			reader.Cache.DeleteFromSet(CACHE_KEY_PROXY, proxy)
			len, err := reader.Cache.GetLength(CACHE_KEY_PROXY)
			if err != nil {
				sdclogger.SDCLoggerInstance.Printf("Reader[%s]: Failed to get number of proxies. Error: %s", reader.key, err.Error())
			} else {
				sdclogger.SDCLoggerInstance.Printf("Reader[%s]: Remove proxy server %s, %d left.", reader.key, proxy, len)
			}
			continue
		}

		sdclogger.SDCLoggerInstance.Printf("Reader[%s]: command [%s] done", reader.key, strings.Join(cmd.Args, " "))
		htmlContent, err := os.ReadFile(htmlFile)
		if err != nil {
			sdclogger.SDCLoggerInstance.Printf("Failed to read file %s. Error: %s", htmlFile, err.Error())
		} else {
			return string(htmlContent), nil
		}
	}
}

type HttpDirectReader struct {
	key string
}

func NewHttpDirectReader() *HttpDirectReader {
	return &HttpDirectReader{key: strconv.Itoa(nextId())}
}
func (reader *HttpDirectReader) Read(url string, params map[string]string) (string, error) {
	limiter := rate.NewLimiter(rate.Limit(1), 1)
	defaultRetryInterval := 65
	for {
		if !limiter.Allow() {
			time.Sleep(time.Second)
			continue
		}

		httpClient := http.Client{}
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			return "", errors.New("Failed to create GET request for url" + url + ", Error: " + err.Error())
		}
		req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8")
		// req.Header.Set("Accept-Encoding", "gzip, deflate, br")
		req.Header.Set("Accept-Language", "en-US,en;q=0.5")
		req.Header.Set("Alt-Used", "stockanalysis.com")
		req.Header.Set("Connection", "keep-alive")
		// req.Header.Set("Cookie", "g23wfclI9hIxrY.TqVFcMkts6WN4kBYZagz.Vqya3Wc-1714137516-1.0.1.1-QqwYy6Zb82ZLilqUkb4lXgSRcz6If.k8cmnSWdNVzN9tPyqnGxeevAOCEdVh36p2.zftMkLN_CxdCraTLn9_bw; landingPageVariation=20aprB; convActions=Navigation_Menu_Desktop; sb-auth-auth-token=%5B%22eyJhbGciOiJIUzI1NiIsImtpZCI6ImhCUzR1OE1MbWZTaHpBY2YiLCJ0eXAiOiJKV1QifQ.eyJhdWQiOiJhdXRoZW50aWNhdGVkIiwiZXhwIjoxNzE1NTc1NTQ4LCJpYXQiOjE3MTQ5NzA3NDgsImlzcyI6Imh0dHBzOi8vdXJmbnpwYnNhZXV2ZmNoZGZqZ3ouc3VwYWJhc2UuY28vYXV0aC92MSIsInN1YiI6ImIzYjgzYWZmLTM5ODctNDRmMi…Y2F0ZWQiLCJhYWwiOiJhYWwxIiwiYW1yIjpbeyJtZXRob2QiOiJvYXV0aCIsInRpbWVzdGFtcCI6MTcxNDk3MDc0OH1dLCJzZXNzaW9uX2lkIjoiMmI1MDU4NTAtODg4Ni00NDcwLWJhNmYtYWM0MTMwN2U2NDcwIiwiaXNfYW5vbnltb3VzIjpmYWxzZX0.SUjEKO1S1orwwcBk6zSbyv4IUckFYBUBmaGVQ9R_ds8%22%2C%22cYRDgS_EAcI6-bvN9T0CAg%22%2C%22ya29.a0AXooCgt9uOK6lUWEMXXoIuHD9uzT8oDfOu1A8WUT_HLuqcEnRPDYZsBIiHkWAjH794YRWhN1oGQ6XLx-GtlsEhXoB90wvfxiu44OD4XzvvXaGik07Uq-6-HiE5zZRjvEGw_Fc_RnRST7pndus4bikEttfWVQQK_YvwaCgYKAVUSARMSFQHGX2MiVfk2ITTSs-kkCJDCpWNBHg0169%22%2Cnull%2Cnull%5D")
		// req.Header.Set("Cookie", "cf_clearance=g23wfclI9hIxrY.TqVFcMkts6WN4kBYZagz.Vqya3Wc-1714137516-1.0.1.1-QqwYy6Zb82ZLilqUkb4lXgSRcz6If.k8cmnSWdNVzN9tPyqnGxeevAOCEdVh36p2.zftMkLN_CxdCraTLn9_bw; landingPageVariation=20aprB; convActions=Navigation_Menu_Desktop; sb-auth-auth-token=%5B%22eyJhbGciOiJIUzI1NiIsImtpZCI6ImhCUzR1OE1MbWZTaHpBY2YiLCJ0eXAiOiJKV1QifQ.eyJhdWQiOiJhdXRoZW50aWNhdGVkIiwiZXhwIjoxNzE1NTc1NTQ4LCJpYXQiOjE3MTQ5NzA3NDgsImlzcyI6Imh0dHBzOi8vdXJmbnpwYnNhZXV2ZmNoZGZqZ3ouc3VwYWJhc2UuY28vYXV0aC92MSIsInN1YiI6ImIzYjgzYWZmLTM5ODctNDRmMi…XRob2QiOiJvYXV0aCIsInRpbWVzdGFtcCI6MTcxNDk3MDc0OH1dLCJzZXNzaW9uX2lkIjoiMmI1MDU4NTAtODg4Ni00NDcwLWJhNmYtYWM0MTMwN2U2NDcwIiwiaXNfYW5vbnltb3VzIjpmYWxzZX0.SUjEKO1S1orwwcBk6zSbyv4IUckFYBUBmaGVQ9R_ds8%22%2C%22cYRDgS_EAcI6-bvN9T0CAg%22%2C%22ya29.a0AXooCgt9uOK6lUWEMXXoIuHD9uzT8oDfOu1A8WUT_HLuqcEnRPDYZsBIiHkWAjH794YRWhN1oGQ6XLx-GtlsEhXoB90wvfxiu44OD4XzvvXaGik07Uq-6-HiE5zZRjvEGw_Fc_RnRST7pndus4bikEttfWVQQK_YvwaCgYKAVUSARMSFQHGX2MiVfk2ITTSs-kkCJDCpWNBHg0169%22%2Cnull%2Cnull%5D; cf_chl_3=9e9f56cdfcf0509; cf_chl_rc_m=1")
		req.Header.Set("Host", "stockanalysis.com")
		req.Header.Set("If-None-Match", "W/\"lx5gry\"")
		req.Header.Set("Sec-Fetch-Dest", "document")
		req.Header.Set("Sec-Fetch-Mode", "navigate")
		req.Header.Set("Sec-Fetch-Site", "none")
		req.Header.Set("Sec-Fetch-User", "?1")
		req.Header.Set("Upgrade-Insecure-Requests", "1")
		req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0")
		req.Header.Set("Referer", "www.google.com")
		q := req.URL.Query()
		for key, val := range params {
			q.Add(key, val)
		}
		req.URL.RawQuery = q.Encode()

		var res *http.Response
		res, err = httpClient.Do(req)
		if err != nil {
			return "", errors.New("Failed to perform request to url" + url + ", Error: " + err.Error())
		}

		if res.StatusCode != http.StatusOK {
			if res.StatusCode == http.StatusTooManyRequests {

				// Access Retry-After header attribute
				retryAfter := res.Header.Get("Retry-After")
				if retryAfter != "" {
					// Parse Retry-After header value to get duration
					duration, err := time.ParseDuration(retryAfter)
					if err != nil {
						return "", errors.New(
							"Failed to get Retry-After attribute after getting response status " +
								res.Status + ", Error: " + err.Error() + ". Requested url is " + url)
					}
					sdclogger.SDCLoggerInstance.Println("Delay " + retryAfter + " seconds")
					time.Sleep(time.Duration(duration) * time.Second)
					continue
				} else {
					sdclogger.SDCLoggerInstance.Println("Delay " + strconv.Itoa(defaultRetryInterval) + " seconds")
					time.Sleep(time.Duration(defaultRetryInterval) * time.Second)
					continue
				}
			}
			// Return on error other than too many requests or retrr exhausted
			return "", errors.New("Received non-succes status " + res.Status + " in requesting url " + url)
		}
		defer res.Body.Close()

		body, err := io.ReadAll(res.Body)
		if err == nil {
			return string(body), nil
		}
	}
}
