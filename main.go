package main

import (
	"fmt"

	"github.com/wayming/sdc/json2db"
)

func main() {
	ticker := `{"name":"Microsoft Corporation","symbol":"MSFT","has_intraday":false,"has_eod":true,"country":null,"stock_exchange":{"name":"NASDAQ Stock Exchange","acronym":"NASDAQ","mic":"XNAS","country":"USA","country_code":"US","city":"New York","website":"www.nasdaq.com"}}`
	ddlGen := json2db.NewDDLGenPG()
	fmt.Println(ddlGen.Gen(ticker))
	fmt.Println("main")
}
