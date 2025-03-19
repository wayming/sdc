package collector

import (
	"reflect"
)

const ND_TICKERS = "NDTicker"

type NDTickers struct {
	Symbol   string `json:"symbol"`
	Name     string `json:"name"`
	Country  string `json:"country"`
	IPOYear  string `json:"ipo_year"`
	Sector   string `json:"sector"`
	Industry string `json:"industry"`
}

var NDSymDataTables = map[string]string{
	ND_TICKERS: "nd_tickers",
}

var NDSymDataTypes = map[string]reflect.Type{
	ND_TICKERS: reflect.TypeFor[NDTickers](),
}
