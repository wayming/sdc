package collector

import (
	"reflect"
)

const ND_TICKERS = "NDTicker"

type NDTickers struct {
	Symbol   string `json:"Symbol"`
	Name     string `json:"Name"`
	Country  string `json:"Country"`
	IPOYear  string `json:"IPOYear"`
	Sector   string `json:"Sector"`
	Industry string `json:"Industry"`
}

var NDSymDataTables = map[string]string{
	ND_TICKERS: "nd_tickers",
}

var NDSymDataTypes = map[string]reflect.Type{
	ND_TICKERS: reflect.TypeFor[NDTickers](),
}
