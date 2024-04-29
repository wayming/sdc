package collector

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"reflect"
	"strings"
)

type HttpReader struct {
	ProxyFile string
	logger    *log.Logger
}

func NewHttpReader(proxyFile string, logger *log.Logger) *HttpReader {
	return &HttpReader{ProxyFile: proxyFile, logger: logger}
}

func GetProxies(textFile string) []string {
	content, err := os.ReadFile(textFile)
	if err != nil {
		fmt.Println("Failed to get proxies from file " + textFile + ". Error: " + err.Error())
		return nil
	}
	validProxies := TestProxies(strings.Split(string(content), "\n"))
	return validProxies
}
func IsProxyValid(proxy string) bool {
	parts := strings.Split(proxy, ":")
	cmd := exec.Command("nc", "-w", "5", "-zv", parts[0], parts[1])
	if err := cmd.Run(); err != nil {
		fmt.Println("Faield to ping "+proxy+". Error: ", err.Error())
		return false
	}
	if cmd.ProcessState.ExitCode() != 0 {
		return false
	}

	cmd = exec.Command("wget", "--timeout", "2", "-e", "use_proxy=yes", "-e", "http_proxy="+proxy, "https://stockanalysis.com/")
	if err := cmd.Run(); err != nil {
		fmt.Println("Faield to ping "+proxy+". Error: ", err.Error())
		return false
	}
	if cmd.ProcessState.ExitCode() != 0 {
		return false
	}
	return true
}
func TestProxies(proxies []string) []string {
	inChan := make(chan string, len(proxies))
	ouChan := make(chan string, len(proxies))
	defer close(ouChan)

	numWorkers := 50
	for i := 0; i < numWorkers; i++ {
		go func(inChan chan string, ouChan chan string) {
			for proxy := range inChan {
				if IsProxyValid(proxy) {
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

func ReadURL(url string, params map[string]string) (string, error) {
	htmlContent := ""
	bodyString := ""

	// const maxDelay = 200
	// delay := 1
	// for delay < maxDelay {

	// 	httpClient := http.Client{}
	// 	req, err := http.NewRequest("GET", url, nil)
	// 	if err != nil {
	// 		return htmlContent, errors.New("Failed to create GET request for url" + url + ", Error: " + err.Error())
	// 	}

	// 	q := req.URL.Query()
	// 	for key, val := range params {
	// 		q.Add(key, val)
	// 	}
	// 	req.URL.RawQuery = q.Encode()

	// 	var res *http.Response

	// 	res, err = httpClient.Do(req)
	// 	if err != nil {
	// 		return htmlContent, errors.New("Failed to perform request to url" + url + ", Error: " + err.Error())
	// 	}
	// 	if res.StatusCode != http.StatusOK {
	// 		if res.StatusCode == http.StatusTooManyRequests {
	// 			fmt.Println("Delay " + strconv.Itoa(delay) + " seconds")
	// 			time.Sleep(time.Duration(delay) * time.Second)
	// 			delay = delay * 2
	// 			if delay <= maxDelay {
	// 				continue
	// 			}
	// 		}
	// 		// Return on error other than too many requests or retrr exhausted
	// 		return htmlContent, errors.New("Received non-succes status " + res.Status + " in requesting url " + url)
	// 	}
	// 	defer res.Body.Close()

	// 	body, err := io.ReadAll(res.Body)
	// 	if err != nil {
	// 		return string(body), err
	// 	}
	// 	bodyString = string(body)
	// 	break
	// }

	// limiter := rate.NewLimiter(rate.Limit(1), 1)
	// defaultRetryInterval := 65
	for {
		// if !limiter.Allow() {
		// 	time.Sleep(time.Second)
		// 	continue
		// }

		httpClient := http.Client{}
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			return htmlContent, errors.New("Failed to create GET request for url" + url + ", Error: " + err.Error())
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
		// fmt.Println("Request>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
		// // List all attributes of the response header
		// for key, values := range req.Header {
		// 	for _, value := range values {
		// 		fmt.Printf("%s: %s\n", key, value)
		// 	}
		// }
		// fmt.Println("Request<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")

		var res *http.Response

		res, err = httpClient.Do(req)
		if err != nil {
			return htmlContent, errors.New("Failed to perform request to url" + url + ", Error: " + err.Error())
		}
		// fmt.Println("Response>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

		// for key, values := range res.Header {
		// 	for _, value := range values {
		// 		fmt.Printf("%s: %s\n", key, value)
		// 	}
		// }
		// fmt.Println("Response<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")

		if res.StatusCode != http.StatusOK {
			// if res.StatusCode == http.StatusTooManyRequests {

			// 	// Access Retry-After header attribute
			// 	retryAfter := res.Header.Get("Retry-After")
			// 	if retryAfter != "" {
			// 		// Parse Retry-After header value to get duration
			// 		duration, err := time.ParseDuration(retryAfter)
			// 		if err != nil {
			// 			return htmlContent, errors.New(
			// 				"Failed to get Retry-After attribute after getting response status " +
			// 					res.Status + ", Error: " + err.Error() + ". Requested url is " + url)
			// 		}
			// 		fmt.Println("Delay " + retryAfter + " seconds")
			// 		time.Sleep(time.Duration(duration) * time.Second)
			// 		continue
			// 	} else {
			// 		fmt.Println("Delay " + strconv.Itoa(defaultRetryInterval) + " seconds")
			// 		time.Sleep(time.Duration(defaultRetryInterval) * time.Second)
			// 		continue
			// 	}
			// }
			// Return on error other than too many requests or retrr exhausted
			return htmlContent, errors.New("Received non-succes status " + res.Status + " in requesting url " + url)
		}
		defer res.Body.Close()

		body, err := io.ReadAll(res.Body)
		if err != nil {
			return string(body), err
		}
		bodyString = string(body)
		break
	}
	return bodyString, nil
}

type JsonFieldMetadata struct {
	FieldName    string
	FieldType    reflect.Type
	FieldJsonTag string
}

func GetJsonStructMetadata(jsonStructType reflect.Type) map[string]JsonFieldMetadata {
	fieldTypeMap := make(map[string]JsonFieldMetadata)
	for idx := 0; idx < jsonStructType.NumField(); idx++ {
		field := jsonStructType.Field(idx)
		fieldTypeMap[field.Name] = JsonFieldMetadata{field.Name, field.Type, field.Tag.Get("json")}
	}
	return fieldTypeMap
}

func GetFieldTypeByTag(fieldsMetadata map[string]JsonFieldMetadata, tag string) reflect.Type {
	for _, v := range fieldsMetadata {
		if v.FieldJsonTag == tag {
			return v.FieldType
		}
	}

	return nil
}
