package collector

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/wayming/sdc/sdclogger"
)

type IHttpReader interface {
	Read(url string, params map[string]string) (string, error)
	RedirectedUrl(url string) (string, error)
}

type HttpReader struct {
	client *http.Client
}

func NewHttpReader(c *http.Client) *HttpReader {
	return &HttpReader{client: c}
}

// Get redirected url. Return empty string if the specified url is not redirected.
func (reader HttpReader) RedirectedUrl(url string) (string, error) {
	tokens := strings.Split(url, "//")
	if len(tokens) != 2 {
		return "", fmt.Errorf("unknown url format for %s", url)
	}
	baseURL := tokens[0] + "//" + strings.Split(tokens[1], "/")[0]
	wgetCmd := NewWgetCmd("--max-redirect=0", "-S", url)

	if redirectedUrl, err := wgetCmd.RedirectedUrl(); err != nil {
		return "", err
	} else {
		return baseURL + redirectedUrl, nil
	}
}

func (r *HttpReader) Read(url string, params map[string]string) (string, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", fmt.Errorf("Failed to create GET request for %s: %v", url, err)
	}

	// req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8")
	// req.Header.Set("Accept-Encoding", "gzip, deflate, br")
	// req.Header.Set("Accept-Language", "en-US,en;q=0.5")
	// req.Header.Set("Alt-Used", "stockanalysis.com")
	// req.Header.Set("Connection", "keep-alive")
	// req.Header.Set("Host", "stockanalysis.com")
	// req.Header.Set("If-None-Match", "W/\"lx5gry\"")
	// req.Header.Set("Sec-Fetch-Dest", "document")
	// req.Header.Set("Sec-Fetch-Mode", "navigate")
	// req.Header.Set("Sec-Fetch-Site", "none")
	// req.Header.Set("Sec-Fetch-User", "?1")
	// req.Header.Set("Upgrade-Insecure-Requests", "1")
	// req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0")
	// req.Header.Set("Referer", "www.google.com")
	q := req.URL.Query()
	for key, val := range params {
		q.Add(key, val)
	}

	req.URL.RawQuery = q.Encode()

	var res *http.Response
	res, err = r.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("Failed to perform request for %s: %v", url, err)
	}

	if res.StatusCode != http.StatusOK {
		// Return on error other than too many requests or retrr exhausted
		return "",
			NewHttpServerError(
				res.StatusCode, res.Header,
				fmt.Sprintf("Received non-succes status %s when requesting %s", res.Status, url))
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err == nil {
		return string(body), nil
	} else {
		return "", nil
	}
}

type LocalClient struct {
	http.Client
}

type ProxyClient struct {
	http.Client
}

func NewLocalClient() *http.Client {
	return &http.Client{}
}

func NewProxyClient(proxy string) *http.Client {
	proxyURL, err := url.Parse("http://" + proxy)
	if err != nil {
		sdclogger.SDCLoggerInstance.Fatalf("Failed to parse proxy url http://%s: %v", proxy, err)
	}

	transport := &http.Transport{Proxy: http.ProxyURL(proxyURL)}

	return &http.Client{Transport: transport}
}
