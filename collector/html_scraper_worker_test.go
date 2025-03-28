package collector

import (
	"fmt"
	"testing"
)

func TestHtmlScraper_getDataCategory(t *testing.T) {
	t.Run("TestHtmlScraper_getDataCategory", func(t *testing.T) {
		want := "SAFinancialsRatios"
		scraper := &HtmlScraper{}
		if got := scraper.getDataCategory("data/testdata/AAPL/SAFinancialsRatios.json"); got != want {
			t.Errorf("HtmlScraper.getDataCategory() = %v, want %v", got, want)
		}
	})

}

func TestHtmlScraper_normaliseJSONText(t *testing.T) {
	JSONText := `[
	{
        "Asset Turnover": "0.60",
        "Buyback Yield / Dilution": "2.38%",
        "Current Ratio": "2.20",
        "Debt / EBITDA Ratio": "1.86",
        "Debt / Equity Ratio": "0.59",
        "Debt / FCF Ratio": "2.70",
        "Dividend Yield": "0.63%",
        "EV/EBIT Ratio": "29.68",
        "EV/EBITDA Ratio": "25.28",
        "EV/FCF Ratio": "34.66",
        "EV/Sales Ratio": "6.96",
        "Earnings Yield": "2.91%",
        "Enterprise Value": "45,472",
        "FCF Yield": "3.04%",
        "Fiscal Quarter": "Q1 2025 ",
        "Forward PE": "27.21",
        "Inventory Turnover": "3.18",
        "Last Close Price": "151.52",
        "Market Cap Growth": "13.40%",
        "Market Capitalization": "43,227",
        "P/FCF Ratio": "32.95",
        "P/OCF Ratio": "25.47",
        "P/TBV Ratio": "39.88",
        "PB Ratio": "7.17",
        "PE Ratio": "34.33",
        "PEG Ratio": "3.82",
        "PS Ratio": "6.62",
        "Payout Ratio": "22.33%",
        "Quick Ratio": "1.50",
        "Return on Assets (ROA)": "8.21%",
        "Return on Capital (ROIC)": "10.14%",
        "Return on Capital Employed (ROCE)": "15.25%",
        "Return on Equity (ROE)": "23.79%",
        "Total Shareholder Return": "3.01%"
    }
]`
	want := `[
    {
        "asset_turnover": 0.6,
        "buyback_yield_dilution": 0.023799999999999998,
        "current_ratio": 2.2,
        "debt_ebitda_ratio": 1.86,
        "debt_equity_ratio": 0.59,
        "debt_fcf_ratio": 2.7,
        "dividend_yield": 0.0063,
        "earnings_yield": 0.0291,
        "enterprise_value": 45472,
        "ev_ebit_ratio": 29.68,
        "ev_ebitda_ratio": 25.28,
        "ev_fcf_ratio": 34.66,
        "ev_sales_ratio": 6.96,
        "fcf_yield": 0.0304,
        "fiscal_quarter": "2024-09-30",
        "forward_pe": 27.21,
        "inventory_turnover": 3.18,
        "last_close_price": 151.52,
        "market_cap_growth": 0.134,
        "market_capitalization": 43227,
        "p_fcf_ratio": 32.95,
        "p_ocf_ratio": 25.47,
        "p_tbv_ratio": 39.88,
        "payout_ratio": 0.2233,
        "pb_ratio": 7.17,
        "pe_ratio": 34.33,
        "peg_ratio": 3.82,
        "ps_ratio": 6.62,
        "quick_ratio": 1.5,
        "return_on_assets_roa": 0.0821,
        "return_on_capital_employed_roce": 0.1525,
        "return_on_capital_roic": 0.1014,
        "return_on_equity_roe": 0.2379,
        "total_shareholder_return": 0.0301
    }
]`
	t.Run("TestHtmlScraper_normaliseJSONText", func(t *testing.T) {
		scraper := &HtmlScraper{norm: &SAJsonNormaliser{}, structMeta: AllSAMetricsFields()}
		got, err := scraper.normaliseJSONText(JSONText, "SAFinancialsRatios")
		if err != nil {
			t.Errorf("HtmlScraper.normaliseJSONText() error = %v", err)
			return
		}
		fmt.Println(got)
		fmt.Println(want)
		if got != want {
			t.Errorf("HtmlScraper.normaliseJSONText() = %v, want %v", got, want)
		}
	})

}
