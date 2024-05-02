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
	ProxyFile string
	Cache     *cache.CacheManager
	key       string
}

func NewHttpProxyReader(proxyFile string) *HttpProxyReader {
	reader := &HttpProxyReader{ProxyFile: proxyFile, Cache: cache.NewCacheManager(), key: strconv.Itoa(nextId())}
	if err := reader.Cache.Connect(); err != nil {
		sdclogger.SDCLoggerInstance.Printf("Failed to connect to the cache. Error: %s", err.Error())
		return nil
	}

	loadedProxies, err := cache.LoadProxies(reader.ProxyFile, reader.Cache)
	if err != nil {
		sdclogger.SDCLoggerInstance.Printf("Failed to initialise proxies in the cache. Error: %s", err.Error())
		return nil
	}
	sdclogger.SDCLoggerInstance.Printf("Loaded %d proxies to the cache.", loadedProxies)
	return reader
}

func (reader *HttpProxyReader) Read(url string, params map[string]string) (string, error) {
	htmlFile := "logs/page" + reader.key + ".html"
	for {
		proxy, err := reader.Cache.GetProxy()
		if err != nil {
			return "", err
		}
		if proxy == "" {
			return "", errors.New("proxy server running out")
		}

		cmd := exec.Command("wget",
			"--timeout=10", "--tries=1",
			"-O", htmlFile,
			"-o", "logs/wget"+reader.key+".html",
			"-e", "use_proxy=yes",
			"-e", "https_proxy="+proxy, url)
		err = cmd.Run()
		if err != nil {
			sdclogger.SDCLoggerInstance.Printf("Reader[%s]: Failed to run comand [%s], Error: %s", reader.key, strings.Join(cmd.Args, " "), err.Error())
			reader.Cache.DeleteProxy(proxy)
			len, err := reader.Cache.Proxies()
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

type HttpLocalReader struct {
	key string
}

func NewHttpLocalReader(proxyFile string) *HttpLocalReader {
	return &HttpLocalReader{key: strconv.Itoa(nextId())}
}
func (reader *HttpLocalReader) Read(url string, params map[string]string) (string, error) {
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
		req.Header.Set("Cookie", "cf_clearance=E.RfE9p.mpcE7wK5lMn2Y51DU72WAVzAIbg19_.bgzM-1713659522-1.0.1.1-WIJMZLHY3by9CZ7br9U3jAtS7CMnML0Fsb6uROGe0oKZxcKkFFhcnhZrxDNd1Rm4XlgtkvZ6u.a1.kJt1LhCAg; landingPageVariation=20aprB; convActions=Footer_Links")
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
		if err != nil {
			return string(body), err
		}
	}
}
