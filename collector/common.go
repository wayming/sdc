package collector

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
)

func concatMaps(maps ...map[string]interface{}) (map[string]interface{}, error) {
	results := make(map[string]interface{})
	for _, m := range maps {
		for k, v := range m {
			if v2, ok := results[k]; ok {
				// Check confliction
				if v != v2 {
					errMsg := fmt.Sprintf("Failed to concat maps, key %s has conflict values %v and %v", k, v, v2)
					return nil, errors.New(errMsg)
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

func JsonStructFieldTypeMap(jsonStructType reflect.Type) map[string]reflect.Type {
	fieldTypeMap := make(map[string]reflect.Type)
	for idx := 0; idx < jsonStructType.NumField(); idx++ {
		fieldTypeMap[jsonStructType.Field(idx).Name] = jsonStructType.Field(idx).Type
	}
	return fieldTypeMap
}
