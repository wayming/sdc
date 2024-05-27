package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/wayming/sdc/collector"
)

const SCHEMA_NAME = "sdc"

func main() {
	loadOpt := flag.String("load", "",
		"Load stock infomration into database. "+
			"Supported options include:\n"+
			"tickers: Download tickers information from MS and load them into database.\n"+
			"financials: Download financials information for all tickers from SA and load them into database.")
	tickersJSONOpt := flag.String("tickers_json", "", "Load tickers from JSON file instead of MS.")
	symbolOpt := flag.String("symbol", "", "Load financials for the specified symbol only. Can only be used with option -load financials")
	parallelOpt := flag.Int("parallel", 1, "Parallel streams of loading")
	resetDBOpt := flag.Bool("reset_db", false, "Drop the existing data.")
	resetCacheOpt := flag.Bool("reset_cache", false, "Reset the caches.")
	proxyOpt := flag.String("proxy", "", "File with list of proxy servers. Must be set when loading financials for multiple symbols.")
	continueOpt := flag.Bool("continue", false, "Whether or not continue with the load")

	flag.Parse()

	if symbolOpt == nil && proxyOpt == nil {
		flag.Usage()
		fmt.Println("proxy file required when loading financial for multiple symbols.")
	}
	var err error

	if *resetDBOpt {
		if err := collector.DropSchema(SCHEMA_NAME); err != nil {
			fmt.Println(err.Error())
		} else {
			fmt.Println("Drop schema " + SCHEMA_NAME + " done.")
		}
	}
	if *resetCacheOpt {
		if err := collector.ClearCache(); err != nil {
			fmt.Println(err.Error())
		} else {
			fmt.Println("Reset cache done.")
		}
	}

	var num int64
	if len(*loadOpt) > 0 {
		switch *loadOpt {
		case "tickers":
			if tickersJSONOpt == nil {
				num, err = collector.CollectTickers(SCHEMA_NAME, "")
			} else {
				num, err = collector.CollectTickers(SCHEMA_NAME, *tickersJSONOpt)
			}
			if err != nil {
				fmt.Println(err.Error())
				os.Exit(1)
			} else {
				fmt.Printf("%d tickers loaded\n", num)
			}
		case "financials":
			if len(*symbolOpt) > 0 {
				err = collector.CollectFinancialsForSymbol(SCHEMA_NAME, *symbolOpt)
				num = 1
			} else {
				num, err = collector.CollectFinancials(SCHEMA_NAME, *proxyOpt, *parallelOpt, *continueOpt)
			}
			if err != nil {
				fmt.Println(err.Error())
				os.Exit(1)
			} else {
				fmt.Printf("%d symbols loaded\n", num)
			}
		default:
			fmt.Println("Unknown load option " + *loadOpt)
			os.Exit(1)
		}

	}
	os.Exit(0)
}
