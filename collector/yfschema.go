package collector

import "reflect"

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

type FYTickersBody struct {
	Data []FYTickers `json:"results"`
}

var FYDataTables = map[string]string{
	"FYTickers": "fy_tickers",
	"FYEOD":     "fy_eod",
}

var FYDataTypes = map[string]reflect.Type{
	"FYTickers": reflect.TypeFor[FYTickers](),
	"FYEOD":     reflect.TypeFor[FYEOD](),
}
