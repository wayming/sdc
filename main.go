package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"

	"github.com/wayming/sdc/collector"
	"github.com/wayming/sdc/config"
)

func main() {

	runtime.GOMAXPROCS(200)

	loadOpt := flag.String("load", "",
		"Load stock infomration into database. "+
			"Supported options include:\n"+
			"tickers: Download tickers information from YF and load them into database.\n"+
			"EOD: Download EOD for all tickers from YF and load them into database.\n"+
			"financials: Download financial data from SA and load them into database.")
	tickersJSONOpt := flag.String("tickers_json", "", "Load tickers from JSON file instead of YF. The csv file name is used as the table name.")
	symbolOpt := flag.String("symbol", "", "Load financials for the specified symbol only. Can only be used with option -load financialOverviews or financialDetails")
	parallelOpt := flag.Int("parallel", 1, "Parallel streams of loading")
	resetDBOpt := flag.Bool("reset_db", false, "Drop the existing data.")
	resetCacheOpt := flag.Bool("reset_cache", false, "Reset caches.")
	proxyOpt := flag.String("proxy", "", "File with list of proxy servers.")
	continueOpt := flag.Bool("continue", false, "Whether or not continue with the load")

	flag.Parse()

	if symbolOpt == nil && proxyOpt == nil {
		flag.Usage()
		fmt.Println("proxy file required when loading financial for multiple symbols.")
	}
	var err error
	if *resetDBOpt {
		if err := collector.DropSchema(config.SchemaName); err != nil {
			fmt.Println(err.Error())
		} else {
			fmt.Println("Drop schema " + config.SchemaName + " done.")
		}
	}
	if *resetCacheOpt {
		if err := collector.ClearCache(); err != nil {
			fmt.Println(err.Error())
		} else {
			fmt.Println("Reset cache done.")
		}
	}

	params := collector.PCParams{
		IsContinue:  *continueOpt,
		TickersJSON: *tickersJSONOpt,
		ProxyFile:   *proxyOpt,
	}
	if len(*loadOpt) > 0 {
		switch *loadOpt {
		case "tickers":
			err = collector.YFCollect(*tickersJSONOpt, true, false)
			if err != nil {
				fmt.Println(err.Error())
				os.Exit(1)
			} else {
				fmt.Println("All tickers were loaded")
			}
		case "EOD":
			col := collector.NewEODParallelCollector(params)
			if err := col.Execute(*parallelOpt); err != nil {
				fmt.Println(err.Error())
				os.Exit(1)
			} else {
				fmt.Println("All EODs of tickers were loaded")
			}
		case "financials":
			if len(*symbolOpt) > 0 {
				err = collector.CollectFinancialsForSymbol(*symbolOpt)
			} else {
				pCollector := collector.NewFinancialParallelCollector(params)
				err = pCollector.Execute(*parallelOpt)
			}
			if err != nil {
				fmt.Println(err.Error())
				os.Exit(1)
			} else {
				fmt.Println("Financial details for all tickers were loaded")
			}
		default:
			fmt.Println("Unknown load option " + *loadOpt)
			os.Exit(1)
		}

	}
	os.Exit(0)
}
