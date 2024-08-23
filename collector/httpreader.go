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
	// req, err := http.NewRequest("GET", url, nil)
	// if err != nil {
	// 	return "", fmt.Errorf("failed to create GET request for %s: %v", url, err)
	// }

	// q := req.URL.Query()
	// for key, val := range params {
	// 	q.Add(key, val)
	// }

	// req.URL.RawQuery = q.Encode()

	// var res *http.Response
	// res, err = r.client.Do(req)
	res, err := r.client.Get(url)
	if err != nil {
		return "", fmt.Errorf("failed to perform request for %s: %v", url, err)
	}

	if res.StatusCode != http.StatusOK {
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

func NewProxyClient(proxyRecord string) (*http.Client, error) {
	proxyParts := strings.Split(proxyRecord, ":")

	// Define the proxy URL
	proxyURL, err := url.Parse("http://" + proxyParts[2] + ":" + proxyParts[3] + "@" + proxyParts[0] + ":" + proxyParts[1])
	if err != nil {
		sdclogger.SDCLoggerInstance.Printf("Failed to parse proxy URL: %v", err)
		return nil, err
	}

	// Create a new HTTP transport with the proxy settings
	proxyTransport := &http.Transport{
		Proxy: http.ProxyURL(proxyURL),
	}

	return &http.Client{Transport: proxyTransport}, nil
}
