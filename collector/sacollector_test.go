package collector_test

import (
	"testing"

	. "github.com/wayming/sdc/collector"
	testcommon "github.com/wayming/sdc/testcommon"
)

func TestSACollector_MapRedirectedSymbol(t *testing.T) {
	fixture := testcommon.NewMockTestFixture(t)
	defer fixture.Teardown(t)

	fixture.DBExpect().LoadByJsonText(
		testcommon.NewStringPatternMatcher(".*meta.*fb.*"),
		SADataTables[SA_REDIRECTED_SYMBOLS],
		SADataTypes[SA_REDIRECTED_SYMBOLS]).Times(1)

	c := NewSACollector(fixture.Reader(), fixture.Exporter(), fixture.DBMock(), fixture.Logger())
	got, err := c.MapRedirectedSymbol("fb")
	if err != nil {
		t.Fatalf("Failed to call MapRedirectedSymbol(), error %v", err)
	}

	if got != "meta" {
		t.Fatalf("Expecting fb reditrected to meta, however got %s", got)
	}
}

func TestSACollector_CollectFinancialOverview(t *testing.T) {
	fixture := testcommon.NewMockTestFixture(t)
	defer fixture.Teardown(t)

	expectNumOfRows := int64(10)
	fixture.DBExpect().LoadByJsonText(
		testcommon.NewStringPatternMatcher(".*\"Symbol\":\"msft\"*"),
		SADataTables[SA_STOCKOVERVIEW],
		SADataTypes[SA_STOCKOVERVIEW]).Times(1).Return(expectNumOfRows, nil)

	c := NewSACollector(fixture.Reader(), fixture.Exporter(), fixture.DBMock(), fixture.Logger())
	num, err := c.CollectFinancialOverview("msft")
	if err != nil {
		t.Fatalf("Failed to call CollectFinancialOverview(), error %v", err)
	}

	if num != expectNumOfRows {
		t.Fatalf("Expecting %d rows were inserted into database table. However %d rows inserted.", expectNumOfRows, num)
	}
}

func TestSACollector_CollectFinancialsIncome(t *testing.T) {
	fixture := testcommon.NewMockTestFixture(t)
	defer fixture.Teardown(t)

	expectNumOfRows := int64(10)
	fixture.DBExpect().LoadByJsonText(
		testcommon.NewStringPatternMatcher(".*\"Symbol\":\"msft\"*"),
		SADataTables[SA_FINANCIALSINCOME],
		SADataTypes[SA_FINANCIALSINCOME]).Times(1).Return(expectNumOfRows, nil)

	c := NewSACollector(fixture.Reader(), fixture.Exporter(), fixture.DBMock(), fixture.Logger())
	num, err := c.CollectFinancialsIncome("msft")
	if err != nil {
		t.Fatalf("Failed to call CollectFinancialsIncome(), error %v", err)
	}

	if num != expectNumOfRows {
		t.Fatalf("Expecting %d rows were inserted into database table. However %d rows inserted.", expectNumOfRows, num)
	}
}

func TestSACollector_CollectBalanceSheet(t *testing.T) {
	fixture := testcommon.NewMockTestFixture(t)
	defer fixture.Teardown(t)

	expectNumOfRows := int64(10)
	fixture.DBExpect().LoadByJsonText(
		testcommon.NewStringPatternMatcher(".*\"Symbol\":\"msft\"*"),
		SADataTables[SA_FINANCIALSBALANCESHEET],
		SADataTypes[SA_FINANCIALSBALANCESHEET]).Times(1).Return(expectNumOfRows, nil)

	c := NewSACollector(fixture.Reader(), fixture.Exporter(), fixture.DBMock(), fixture.Logger())
	num, err := c.CollectFinancialsBalanceSheet("msft")
	if err != nil {
		t.Fatalf("Failed to call CollectBalanceSheet(), error %v", err)
	}

	if num != expectNumOfRows {
		t.Fatalf("Expecting %d rows were inserted into database table. However %d rows inserted.", expectNumOfRows, num)
	}
}

func TestSACollector_CollectCashFlow(t *testing.T) {
	fixture := testcommon.NewMockTestFixture(t)
	defer fixture.Teardown(t)

	expectNumOfRows := int64(10)
	fixture.DBExpect().LoadByJsonText(
		testcommon.NewStringPatternMatcher(".*\"Symbol\":\"msft\"*"),
		SADataTables[SA_FINANCIALSCASHFLOW],
		SADataTypes[SA_FINANCIALSCASHFLOW]).Times(1).Return(expectNumOfRows, nil)

	c := NewSACollector(fixture.Reader(), fixture.Exporter(), fixture.DBMock(), fixture.Logger())
	num, err := c.CollectFinancialsCashFlow("msft")
	if err != nil {
		t.Fatalf("Failed to call CollectCashFlow(), error %v", err)
	}

	if num != expectNumOfRows {
		t.Fatalf("Expecting %d rows were inserted into database table. However %d rows inserted.", expectNumOfRows, num)
	}
}

func TestSACollector_CollectRatios(t *testing.T) {
	fixture := testcommon.NewMockTestFixture(t)
	defer fixture.Teardown(t)

	expectNumOfRows := int64(10)
	fixture.DBExpect().LoadByJsonText(
		testcommon.NewStringPatternMatcher(".*\"Symbol\":\"msft\"*"),
		SADataTables[SA_FINANCIALRATIOS],
		SADataTypes[SA_FINANCIALRATIOS]).Times(1).Return(expectNumOfRows, nil)

	c := NewSACollector(fixture.Reader(), fixture.Exporter(), fixture.DBMock(), fixture.Logger())
	num, err := c.CollectFinancialsRatios("msft")
	if err != nil {
		t.Fatalf("Failed to call CollectRatios(), error %v", err)
	}

	if num != expectNumOfRows {
		t.Fatalf("Expecting %d rows were inserted into database table. However %d rows inserted.", expectNumOfRows, num)
	}
}

// func TestCollectFinancialsForSymbol(t *testing.T) {
// 	if err := CollectFinancialsForSymbol("adi"); err != nil {
// 		t.Errorf("CollectFinancialsForSymbol() error = %v", err)
// 	}
// }
