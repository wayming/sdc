package main

import (
	"fmt"
	"log"
	"os"
	"reflect"
	"strconv"

	"github.com/wayming/sdc/collector"
)

func Greet(name int) {
	fmt.Println("Hello, " + strconv.Itoa(name))
}

var FUNCTIONS_MAAP = map[string]interface{}{
	"Greet":               Greet,
	"load_nasdaq_tickers": load_nasdaq_tickers,
}

func load_nasdaq_tickers(filePath string, nThreads int) {
	wiManager, err := collector.NewNDSymbolsLoaderWorkItemManager(filePath)
	if err != nil {
		log.Panicf("Failed to create symbols loader. Error: %v", err)
	}
	collector.NewParallelNDSymbolsLoader(collector.NewNDSymbolsLoaderBuilder(), wiManager).Execute(nThreads)
}

func main() {
	if len(os.Args) < 2 {
		log.Panic("Not enough input parameters")
	}

	funcName := os.Args[1]

	var funcParams []interface{}
	for _, arg := range os.Args[2:] {
		if num, err := strconv.Atoi(arg); err == nil {
			funcParams = append(funcParams, num)
		} else if floatNum, err := strconv.ParseFloat(arg, 64); err == nil {
			funcParams = append(funcParams, floatNum)
		} else if boolVal, err := strconv.ParseBool(arg); err == nil {
			funcParams = append(funcParams, boolVal)
		} else {
			funcParams = append(funcParams, arg)
		}
	}

	fn, ok := FUNCTIONS_MAAP[funcName]
	if !ok {
		log.Panicf("Unknown function %s", funcName)
	}

	callable := reflect.ValueOf(fn)
	if callable.Type().NumIn() != len(funcParams) {
		log.Panicf("Invalid number of paramerters, expected %d, got %d",
			callable.Type().NumIn(), len(funcParams))
	}

	var callableParams []reflect.Value
	for idx, param := range funcParams {
		paramType := callable.Type().In(idx)
		paramValue := reflect.ValueOf(param)
		if paramValue.Type().ConvertibleTo(paramType) {
			callableParams = append(callableParams, paramValue.Convert(paramType))
		} else {
			log.Panicf("Incompatiable parameter types, expected %v, got %v", paramType, paramValue.Type())
		}
	}

	result := callable.Call(callableParams)
	if len(result) > 0 {
		log.Printf("Results: %v", result[0].Interface())
	}
}
