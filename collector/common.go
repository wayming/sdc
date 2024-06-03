package collector

import (
	"errors"
	"fmt"
	"log"
	"os"
	"reflect"

	"github.com/wayming/sdc/cache"
	"github.com/wayming/sdc/dbloader"
)

const LOG_FILE = "logs/sdc.log"
const PROXY_FILE = "data/proxies.txt"
const CACHE_KEY_PROXY = "PROXIES"
const CACHE_KEY_SYMBOL = "SYMBOLS"
const CACHE_KEY_SYMBOL_ERROR = "SYMBOLS_ERROR"
const CACHE_KEY_SYMBOL_INVALID = "SYMBOLS_INVALID"
const CACHE_KEY_SYMBOL_REDIRECTED = "SYMBOLS_REDIRECTED"

const TABLE_SA_OVERALL = "sa_overall"
const TABLE_SA_FINANCIALS_INCOME = "sa_financials_income"
const TABLE_SA_FINANCIALS_BALANCE_SHEET = "sa_financials_balance_sheet"
const TABLE_SA_FINANCIALS_CASH_FLOW = "sa_financials_cash_flow"
const TABLE_SA_FINANCIALS_RATIOS = "sa_financials_ratios"
const TABLE_SA_ANALYST_RATINGS = "sa_analyst_ratings"
const TABLE_SA_SYMBOL_REDIRECT = "sa_symbol_redirect"
const TABLE_MS_TICKERS = "ms_tickers"

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

func ClearCache() error {
	cm := cache.NewCacheManager()
	if err := cm.Connect(); err != nil {
		return err
	}
	defer cm.Disconnect()

	if err := cm.DeleteSet(CACHE_KEY_PROXY); err != nil {
		return err
	}
	if err := cm.DeleteSet(CACHE_KEY_SYMBOL); err != nil {
		return err
	}
	if err := cm.DeleteSet(CACHE_KEY_SYMBOL_ERROR); err != nil {
		return err
	}

	return nil
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

func CacheCleanup() {
	cm := cache.NewCacheManager()
	cm.Connect()
	cm.DeleteSet(CACHE_KEY_PROXY)
	cm.DeleteSet(CACHE_KEY_SYMBOL)
	cm.DeleteSet(CACHE_KEY_SYMBOL_ERROR)
	cm.DeleteSet(CACHE_KEY_SYMBOL_REDIRECTED)
	cm.DeleteSet(CACHE_KEY_SYMBOL_INVALID)
	cm.Disconnect()
}
