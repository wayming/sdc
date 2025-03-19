// Load the downloaded file of https://www.nasdaq.com/market-activity/stocks/screener
// Remove the duplicate stocks
package collector

import (
	"reflect"
	"testing"
)

func TestRemoveDuplicateRowsShortSymbolFront(t *testing.T) {

	t.Run("TestRemoveDuplicateRows", func(t *testing.T) {
		inData := [3]string{
			"ABR,Arbor Realty Trust Common Stock,$12.06,-0.20,-1.631%,2285435389.00,United States,2004,2704657,Real Estate,Real Estate Investment Trusts",
			"ABR^D,Arbor Realty Trust 6.375% Series D Cumulative Redeemable Preferred Stock Liquidation Preference $25.00 per Share,$17.85,-0.01,-0.056%,,United States,,16857,,",
			"ABR^E,Arbor Realty Trust 6.25% Series E Cumulative Redeemable Preferred Stock,$17.65,-0.13,-0.731%,,United States,,10740,,"}
		got, err := RemoveDuplicateRows(inData[:])
		if err != nil {
			t.Errorf("RemoveDuplicateRows() error = %v", err)
			return
		}
		want := map[string]string{
			"ABR": "ABR,Arbor Realty Trust Common Stock,$12.06,-0.20,-1.631%,2285435389.00,United States,2004,2704657,Real Estate,Real Estate Investment Trusts"}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("RemoveDuplicateRows() = %v, want %v", got, want)
		}
	})
}

func TestRemoveDuplicateRowsShortSymbolBack(t *testing.T) {

	t.Run("TestRemoveDuplicateRows", func(t *testing.T) {
		inData := [3]string{
			"ABR^D,Arbor Realty Trust 6.375% Series D Cumulative Redeemable Preferred Stock Liquidation Preference $25.00 per Share,$17.85,-0.01,-0.056%,,United States,,16857,,",
			"ABR^E,Arbor Realty Trust 6.25% Series E Cumulative Redeemable Preferred Stock,$17.65,-0.13,-0.731%,,United States,,10740,,",
			"ABR,Arbor Realty Trust Common Stock,$12.06,-0.20,-1.631%,2285435389.00,United States,2004,2704657,Real Estate,Real Estate Investment Trusts"}
		got, err := RemoveDuplicateRows(inData[:])
		if err != nil {
			t.Errorf("RemoveDuplicateRows() error = %v", err)
			return
		}
		want := map[string]string{
			"ABR": "ABR,Arbor Realty Trust Common Stock,$12.06,-0.20,-1.631%,2285435389.00,United States,2004,2704657,Real Estate,Real Estate Investment Trusts"}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("RemoveDuplicateRows() = %v, want %v", got, want)
		}
	})
}
