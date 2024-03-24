package json2db

type Tickers struct {
	Name          string `json:"name"`
	Symbol        string `json:"symbol"`
	HasIntraday   bool   `json:"has_intraday"`
	HasEod        bool   `json:"has_eod"`
	Country       string `json:"country,omitempty"`
	StockExchange struct {
		Name        string `json:"name"`
		Acronym     string `json:"acronym"`
		Mic         string `json:"mic"`
		Country     string `json:"country"`
		CountryCode string `json:"country_code"`
		City        string `json:"city"`
		Website     string `json:"website"`
	} `json:"stock_exchange"`
}

type Intraday struct {
	Open     float64 `json:"open"`
	High     float64 `json:"high"`
	Low      float64 `json:"low"`
	Last     float64 `json:"last"`
	Close    float64 `json:"close"`
	Volume   float64 `json:"volume"`
	Date     string  `json:"date"`
	Symbol   string  `json:"symbol"`
	Exchange string  `json:"exchange"`
}

type EOD struct {
	Open        float64 `json:"open"`
	High        float64 `json:"high"`
	Low         float64 `json:"low"`
	Close       float64 `json:"close"`
	Volume      float64 `json:"volume"`
	AdjHigh     float64 `json:"adj_high"`
	AdjLow      float64 `json:"adj_low"`
	AdjClose    float64 `json:"adj_close"`
	AdjOpen     float64 `json:"adj_open"`
	AdjVolume   float64 `json:"adj_volume"`
	SplitFactor float64 `json:"split_factor"`
	Dividend    float64 `json:"dividend"`
	Symbol      string  `json:"symbol"`
	Exchange    string  `json:"exchange"`
	Date        string  `json:"date"`
}
