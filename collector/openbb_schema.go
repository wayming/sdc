package collector

import "reflect"

const OPENBB_HIST_PRICE_INTRADAY = "OpenBBHistPriceIntraday"

type HistoryPriceIntraday struct {
	Symbol     string  `json:"symbol" db:"PrimaryKey"`
	Open       float64 `json:"open"`
	High       float64 `json:"high"`
	Low        float64 `json:"low"`
	Close      float64 `json:"close"`
	Volume     float64 `json:"volume"`
	Date       string  `json:"date"`
	SplitRatio string  `json:"split_ratio"`
	Dividend   string  `json:"dividend"`
}

var OpenBBDataTables = map[string]string{
	OPENBB_HIST_PRICE_INTRADAY: "history_price_intraday",
}

var OpenBBDataTypes = map[string]reflect.Type{
	OPENBB_HIST_PRICE_INTRADAY: reflect.TypeFor[HistoryPriceIntraday](),
}
