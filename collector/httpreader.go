package collector

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"golang.org/x/net/proxy"
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
		return "", fmt.Errorf("failed to create GET request for %s: %v", url, err)
	}

	q := req.URL.Query()
	for key, val := range params {
		q.Add(key, val)
	}

	req.URL.RawQuery = q.Encode()

	var res *http.Response
	res, err = r.client.Do(req)
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

func NewProxyClient(proxyURL string) (*http.Client, error) {
	proxyParts := strings.Split(proxyURL, ":")
	if len(proxyParts) != 4 {
		return nil, fmt.Errorf("Failed to parse proxy string %s.")
	}
	// Proxy server details
	proxyAddr := fmt.Sprintf("%s:%s", proxyParts[0], proxyParts[1])
	proxyUser := proxyParts[2]
	proxyPass := proxyParts[3]

	// Set up the proxy
	auth := proxy.Auth{
		User:     proxyUser,
		Password: proxyPass,
	}

	// Create a SOCKS5 dialer with authentication
	dialer, err := proxy.SOCKS5("tcp", proxyAddr, &auth, proxy.Direct)
	if err != nil {
		log.Fatalf("Error creating SOCKS5 proxy: %v", err)
	}

	// Create an HTTP transport that uses the proxy
	httpTransport := &http.Transport{
		Dial: dialer.Dial,
	}

	return &http.Client{Transport: httpTransport}, nil
}
