package cache

import (
	"errors"
	"os"
	"reflect"
	"strings"

	"github.com/wayming/sdc/dbloader"
	"github.com/wayming/sdc/sdclogger"
)

func LoadSymbols(cm ICacheManager, key string, fromSchema string) (int64, error) {
	dbLoader := dbloader.NewPGLoader(fromSchema, &sdclogger.SDCLoggerInstance.Logger)
	dbLoader.Connect(os.Getenv("PGHOST"),
		os.Getenv("PGPORT"),
		os.Getenv("PGUSER"),
		os.Getenv("PGPASSWORD"),
		os.Getenv("PGDATABASE"))
	defer dbLoader.Disconnect()

	type queryResult struct {
		Symbol string
	}

	sqlQuerySymbol := "select symbol from ms_tickers"
	results, err := dbLoader.RunQuery(sqlQuerySymbol, reflect.TypeFor[queryResult]())
	if err != nil {
		return 0, errors.New("Failed to run query [" + sqlQuerySymbol + "]. Error: " + err.Error())
	}
	queryResults, ok := results.([]queryResult)
	if !ok {
		return 0, errors.New("failed to run assert the query results are returned as a slice of queryResults")
	}

	for _, row := range queryResults {
		cm.AddToSet(key, strings.ToLower(row.Symbol))
	}

	return int64(len(queryResults)), nil
}
