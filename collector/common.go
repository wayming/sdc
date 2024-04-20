package collector

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"time"

	"github.com/wayming/sdc/dbloader"
)

const LOG_FILE = "logs/sdc.log"

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

func DropSchema(schema string) error {
	file, _ := os.OpenFile(LOG_FILE, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	logger := log.New(file, "sdc: ", log.Ldate|log.Ltime)
	defer file.Close()

	dbLoader := dbloader.NewPGLoader(schema, logger)
	dbLoader.Connect(os.Getenv("PGHOST"),
		os.Getenv("PGPORT"),
		os.Getenv("PGUSER"),
		os.Getenv("PGPASSWORD"),
		os.Getenv("PGDATABASE"))
	dbLoader.DropSchema(schema)
	return nil
}

func ReadURL(url string, params map[string]string) (string, error) {
	htmlContent := ""
	bodyString := ""

	const maxDelay = 20
	delay := 1
	for delay < maxDelay {

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

		var res *http.Response

		res, err = httpClient.Do(req)
		if err != nil {
			return htmlContent, errors.New("Failed to perform request to url" + url + ", Error: " + err.Error())
		}
		if res.StatusCode != http.StatusOK {
			if res.StatusCode == http.StatusTooManyRequests {
				fmt.Println("Delay " + strconv.Itoa(delay) + " seconds")
				time.Sleep(time.Duration(delay) * time.Second)
				delay = delay * 2
				if delay <= maxDelay {
					continue
				}
			}
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
