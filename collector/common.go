package collector

import (
	"errors"
	"io"
	"net/http"
)

func concatMaps(maps ...map[string]string) (map[string]string, error) {
	results := make(map[string]string)
	for _, m := range maps {
		for k, v := range m {
			if v2, ok := results[k]; ok {
				// Check confliction
				if v != v2 {
					return nil, errors.New("Failed to concat maps, key " + k + " has conflict values " + v + " and " + v2)
				}
			}
			results[k] = v
		}
	}
	return results, nil
}
func ReadURL(url string, params map[string]string) (string, error) {
	var htmlContent string

	httpClient := http.Client{}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return htmlContent, errors.New("Failed to create GET request for url" + url + ", Error: " + err.Error())
	}

	q := req.URL.Query()
	for key, val := range params {
		q.Add(key, val)
	}
	req.URL.RawQuery = q.Encode()

	res, err := httpClient.Do(req)
	if err != nil {
		return htmlContent, errors.New("Failed to perform request to url" + url + ", Error: " + err.Error())
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return string(body), err
	}
	return string(body), nil
}
