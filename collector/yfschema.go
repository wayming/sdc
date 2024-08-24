package collector

import "reflect"

const YF_TICKERS = "YFTickers"
const YF_EOD = "YFEOD"

type YFTickers struct {
	Symbol          string  `json:"symbol"`
	Name            string  `json:"name"`
	NASDAQTraded    string  `json:"nasdaq_traded"`
	Exchange        string  `json:"exchange"`
	MarketCategory  string  `json:"market_category"`
	ETF             string  `json:"etf"`
	RoundLotSize    float64 `json:"round_lot_size"`
	TestIssue       string  `json:"test_issue"`
	FinancialStatus string  `json:"financial_status"`
	CQSSymbol       string  `json:"cqs_symbol"`
	NASDAQSymbol    string  `json:"nasdaq_symbol"`
	NextShares      string  `json:"next_shares"`
}

type YFEOD struct {
	Date       string  `json:"date"`
	Open       float64 `json:"open"`
	High       float64 `json:"high"`
	Low        float64 `json:"low"`
	Close      float64 `json:"close"`
	Volume     float64 `json:"volume"`
	SplitRatio float64 `json:"split_ratio"`
	Dividend   float64 `json:"dividend"`
}

type YFEODResponse struct {
	Results []YFEOD `json:"results"`
}

type YFTickersResponse struct {
	Results  []YFTickers   `json:"results"`
	Provider string        `json:"provider"`
	Warnings []Warning     `json:"warnings"`
	Chart    interface{}   `json:"chart"` // Can be any type, using interface{} to represent null or other types
	Extra    ExtraMetadata `json:"extra"`
}

// Warning represents a warning message in the response
type Warning struct {
	Category string `json:"category"`
	Message  string `json:"message"`
}

// ExtraMetadata represents the extra field in the response
type ExtraMetadata struct {
	Metadata Metadata `json:"metadata"`
}

// Metadata represents the metadata structure inside extra
type Metadata struct {
	Arguments Arguments `json:"arguments"`
	Duration  int64     `json:"duration"`
	Route     string    `json:"route"`
	Timestamp string    `json:"timestamp"`
}

// Arguments represents the arguments structure inside metadata
type Arguments struct {
	ProviderChoices ProviderChoices `json:"provider_choices"`
	StandardParams  StandardParams  `json:"standard_params"`
	ExtraParams     ExtraParams     `json:"extra_params"`
}

// ProviderChoices represents the provider choices structure
type ProviderChoices struct {
	Provider string `json:"provider"`
}

// StandardParams represents the standard parameters structure
type StandardParams struct {
	Query    string `json:"query"`
	IsSymbol bool   `json:"is_symbol"`
	UseCache bool   `json:"use_cache"`
}

// ExtraParams represents the extra parameters structure
type ExtraParams struct {
	Active bool  `json:"active"`
	Limit  int   `json:"limit"`
	IsETF  *bool `json:"is_etf"` // Use pointer to handle null values
	IsFund bool  `json:"is_fund"`
}

var YFDataTables = map[string]string{
	YF_TICKERS: "yf_tickers",
	YF_EOD:     "yf_eod",
}

var YFDataTypes = map[string]reflect.Type{
	YF_TICKERS: reflect.TypeFor[YFTickers](),
	YF_EOD:     reflect.TypeFor[YFEOD](),
}
