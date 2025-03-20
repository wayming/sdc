// Load the downloaded file of https://www.nasdaq.com/market-activity/stocks/screener
// Remove the duplicate stocks
package collector_test

import (
	"reflect"
	"testing"

	"github.com/wayming/sdc/collector"
	testcommon "github.com/wayming/sdc/testcommon"
)

func TestRemoveDuplicateRowsShortSymbolFront(t *testing.T) {

	t.Run("TestRemoveDuplicateRows", func(t *testing.T) {
		inData := [3]string{
			"ABR,Arbor Realty Trust Common Stock,$12.06,-0.20,-1.631%,2285435389.00,United States,2004,2704657,Real Estate,Real Estate Investment Trusts",
			"ABR^D,Arbor Realty Trust 6.375% Series D Cumulative Redeemable Preferred Stock Liquidation Preference $25.00 per Share,$17.85,-0.01,-0.056%,,United States,,16857,,",
			"ABR^E,Arbor Realty Trust 6.25% Series E Cumulative Redeemable Preferred Stock,$17.65,-0.13,-0.731%,,United States,,10740,,"}
		got, err := collector.RemoveDuplicateRows(inData[:])
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
		got, err := collector.RemoveDuplicateRows(inData[:])
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

func TestNDSymbolsLoader_Do(t *testing.T) {
	t.Run("TestNDSymbolsLoader_Do", func(t *testing.T) {
		fixture := testcommon.NewMockTestFixture(t).WithExportMock()
		sl := collector.NewNDSymbolsLoader(fixture.ExporterMock(), fixture.Logger(), "")
		wi := collector.NewNDSymbolsLoaderWorkItem(
			"FMN",
			"FMN,Federated Hermes Premier Municipal Income Fund,$10.93,-0.04,-0.365%,942765511.00,United States,2002,112147,Finance,Investment Managers",
			[]string{
				"Symbol",
				"Name",
				"LastSale",
				"NetChange",
				"%Change",
				"MarketCap",
				"Country",
				"IPOYear",
				"Volume",
				"Sector",
				"Industry",
			})
		expectJson := `{"Country":"United States","IPOYear":"2002","Industry":"Investment Managers","Name":"Federated Hermes Premier Municipal Income Fund","Sector":"Finance","Symbol":"FMN"}`
		fixture.ExporterMock().EXPECT().Export(
			collector.NDSymDataTypes[collector.ND_TICKERS],
			collector.NDSymDataTables[collector.ND_TICKERS],
			expectJson, "FMN")

		if err := sl.Do(*wi); err != nil {
			t.Errorf("NDSymbolsLoader.Do() error = %v", err)
		}
	})

}
