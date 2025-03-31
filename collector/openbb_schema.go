package collector

import "reflect"

const OPENBB_HIST_PRICE_INTRADAY = "OpenBBHistPriceIntraday"

type HistoryPriceIntraday struct {
	Open   float64 `json:"open"`
	High   float64 `json:"high"`
	Low    float64 `json:"low"`
	Close  float64 `json:"close"`
	Volume float64 `json:"volume"`
	Date   string  `json:"date" db:"PrimaryKey"`
}

type HistoryPriceIntradayResponse struct {
	Results []HistoryPriceIntraday `json:"results"`
}

var OpenBBDataTables = map[string]string{
	OPENBB_HIST_PRICE_INTRADAY: "history_price_intraday",
}

var OpenBBDataTypes = map[string]reflect.Type{
	OPENBB_HIST_PRICE_INTRADAY: reflect.TypeFor[HistoryPriceIntraday](),
}
