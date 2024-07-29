package collector

import "reflect"

const FY_TICKERS = "FYTickers"
const FY_EOD = "FYEOD"

type FYTickers struct {
	Symbol            string  `json:"symbol"`
	Name              string  `json:"name"`
	Price             float64 `json:"price"`
	Exchange          string  `json:"exchange"`
	ExchangeShortName string  `json:"exchangeShortName"`
	Type              string  `json:"type"`
}

type FYEOD struct {
	Date       string  `json:"date"`
	Open       float64 `json:"open"`
	High       float64 `json:"high"`
	Low        float64 `json:"low"`
	Close      float64 `json:"close"`
	Volume     int64   `json:"volume"`
	SplitRatio float64 `json:"split_ratio"`
	Dividend   float64 `json:"dividend"`
}

type FYEODBody struct {
	Data []FYEOD `json:"results"`
}

var FYDataTables = map[string]string{
	FY_TICKERS: "fy_tickers",
	FY_EOD:     "fy_eod",
}

var FYDataTypes = map[string]reflect.Type{
	FY_TICKERS: reflect.TypeFor[FYTickers](),
	FY_EOD:     reflect.TypeFor[FYEOD](),
}
