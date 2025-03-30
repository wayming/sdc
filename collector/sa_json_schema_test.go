package collector

import (
	"reflect"
	"strings"
	"testing"

	"github.com/wayming/sdc/json2db"
)

const JSON_RATIO_TEXT = `
[
	{
        "Asset Turnover": "0.75",
        "Buyback Yield / Dilution": "4.56%",
        "Current Ratio": "1.36",
        "Debt / EBITDA Ratio": "1.52",
        "Debt / Equity Ratio": "1.87",
        "Debt / FCF Ratio": "1.67",
        "Dividend Yield": "0.73%",
        "EV/EBIT Ratio": "27.89",
        "EV/EBITDA Ratio": "23.90",
        "EV/FCF Ratio": "25.20",
        "EV/Sales Ratio": "6.74",
        "Earnings Yield": "2.99%",
        "Enterprise Value": "1,848,842",
        "FCF Yield": "3.82%",
        "Fiscal Quarter": "Q4 2020 ",
        "Forward PE": "30.80",
        "Inventory Turnover": "39.82",
        "Last Close Price": "109.48",
        "Market Cap Growth": "94.19%",
        "Market Capitalization": "1,920,273",
        "P/FCF Ratio": "26.17",
        "P/OCF Ratio": "23.80",
        "P/TBV Ratio": "29.39",
        "PB Ratio": "29.39",
        "PE Ratio": "33.45",
        "PEG Ratio": "2.69",
        "PS Ratio": "7.00",
        "Payout Ratio": "27.71%",
        "Quick Ratio": "1.22",
        "Return on Assets (ROA)": "10.26%",
        "Return on Capital (ROIC)": "16.71%",
        "Return on Capital Employed (ROCE)": "30.30%",
        "Return on Equity (ROE)": "59.73%",
        "Total Shareholder Return": "5.29%"
    }
]
`

func TestRatioSQLGen(t *testing.T) {
	wantSQL := ""
	wantRows := make([][]interface{}, 0)
	t.Run("TestRatioSQLGen", func(t *testing.T) {
		gotSQL, gotRows, err :=
			json2db.NewJsonToPGSQLConverter().GenInsertSQL(
				JSON_RATIO_TEXT, SADataTables[SA_FINANCIALRATIOS], SADataTypes[SA_FINANCIALRATIOS])
		if err != nil {
			t.Errorf("JsonToPGSQLConverter.GenInsertSQL() error = %v", err)
			return
		}
		if strings.TrimSpace(gotSQL) != strings.TrimSpace(wantSQL) {
			t.Errorf("JsonToPGSQLConverter.GenInsertSQL() gotSQL = %v, wantSQL %v", gotSQL, wantSQL)
		}

		if !reflect.DeepEqual(gotRows, wantRows) {
			t.Errorf("JsonToPGSQLConverter.GenInsertSQL() gotRows = %v, wantRows %v", gotRows, wantRows)
		}
	})
}
