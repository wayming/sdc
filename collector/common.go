package collector

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"regexp"

	"github.com/wayming/sdc/cache"
	"github.com/wayming/sdc/config"
	"github.com/wayming/sdc/dbloader"
	"github.com/wayming/sdc/sdclogger"
)

const LOG_FILE = "logs/sdc.log"
const PROXY_FILE = "data/proxies.txt"
const CACHE_KEY_PROXY = "PROXIES"
const CACHE_KEY_SYMBOL = "SYMBOLS"
const CACHE_KEY_SYMBOL_ERROR = "SYMBOLS_ERROR"
const CACHE_KEY_SYMBOL_INVALID = "SYMBOLS_INVALID"
const CACHE_KEY_SYMBOL_NODATA = "SYMBOLS_NODATA"
const CACHE_KEY_SYMBOL_REDIRECTED = "SYMBOLS_REDIRECTED"

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
	if err := cm.DeleteSet(CACHE_KEY_SYMBOL_REDIRECTED); err != nil {
		return err
	}
	if err := cm.DeleteSet(CACHE_KEY_SYMBOL_INVALID); err != nil {
		return err
	}
	return nil
}
func DropSchema(schema string) error {
	dbLoader := dbloader.NewPGLoader(schema, sdclogger.SDCLoggerInstance)
	dbLoader.Connect(os.Getenv("PGHOST"),
		os.Getenv("PGPORT"),
		os.Getenv("PGUSER"),
		os.Getenv("PGPASSWORD"),
		os.Getenv("PGDATABASE"))
	return dbLoader.DropSchema(schema)
}

type JsonFieldMetadata struct {
	FieldName string
	FieldType reflect.Type
	FieldTags map[string]string
}

func GetJsonStructMetadata(jsonStructType reflect.Type) map[string]JsonFieldMetadata {
	fieldTypeMap := make(map[string]JsonFieldMetadata)
	for idx := 0; idx < jsonStructType.NumField(); idx++ {
		field := jsonStructType.Field(idx)
		tagsMap := make(map[string]string, 0)
		for _, tagKey := range [2]string{"json", "db"} {
			t, ok := field.Tag.Lookup(tagKey)
			if ok {
				tagsMap[tagKey] = t
			}
		}

		fieldTypeMap[field.Name] = JsonFieldMetadata{field.Name, field.Type, tagsMap}
	}
	return fieldTypeMap
}

func GetFieldTypeByTag(fieldsMetadata map[string]JsonFieldMetadata, tag string) reflect.Type {
	for _, v := range fieldsMetadata {
		if v.FieldTags["json"] == tag {
			return v.FieldType
		}
	}

	return nil
}

func GetPrimaryKeyFiledNames(fieldsMetadata map[string]JsonFieldMetadata) []string {
	var names []string
	for k, v := range fieldsMetadata {
		if v.FieldTags["db"] == "PrimaryKey" {
			names = append(names, k)
		}
	}
	return names
}

func IsKeyField(fieldsMetadata map[string]JsonFieldMetadata, jsonTagValue string) bool {
	for _, v := range fieldsMetadata {
		jsonTag, ok := v.FieldTags["json"]
		if !ok {
			continue
		}

		dbTag, ok := v.FieldTags["db"]
		if !ok {
			continue
		}

		if jsonTag == jsonTagValue && dbTag == "PrimaryKey" {
			return true
		}
	}

	return false
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

func CountMatches(text string, pattern string) (int, error) {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return 0, err
	}
	matches := re.FindAllString(text, -1)
	fmt.Printf("%v", matches)
	return len(re.FindAllString(text, -1)), nil
}

func LoadSymbolFromDBToCahce(tableName string, key string) error {
	type queryResult struct {
		Symbol string
	}

	dbLoader := dbloader.NewPGLoader(config.SCHEMA_NAME, sdclogger.SDCLoggerInstance)
	dbLoader.Connect(os.Getenv("PGHOST"),
		os.Getenv("PGPORT"),
		os.Getenv("PGUSER"),
		os.Getenv("PGPASSWORD"),
		os.Getenv("PGDATABASE"))
	defer dbLoader.Disconnect()

	c := cache.NewCacheManager()
	defer c.Disconnect()

	sql := "SELECT symbol FROM " + tableName
	results, err := dbLoader.RunQuery(sql, reflect.TypeFor[queryResult]())
	if err != nil {
		return errors.New("Failed to run query [" + sql + "]. Error: " + err.Error())
	}
	queryResults, ok := results.([]queryResult)
	if !ok {
		return errors.New("failed to assert the slice of queryResults")
	} else {
		sdclogger.SDCLoggerInstance.Printf("%d symbols retrieved from table %s", len(queryResults), tableName)
	}

	for _, row := range queryResults {
		if row.Symbol == "" {
			sdclogger.SDCLoggerInstance.Printf("Ignore the empty symbol.")
			continue
		}
		if err := c.AddToSet(key, row.Symbol); err != nil {
			return err
		}
	}
	return nil
}

func LoadSymbolFromCahce(keyFrom string, keyTo string) error {
	c := cache.NewCacheManager()
	defer c.Disconnect()

	if err := c.CopySet(keyFrom, keyTo); err != nil {
		return fmt.Errorf("failed to restore the error symbols. Error: %s", err.Error())
	}
	return nil
}
