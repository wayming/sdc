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

	fixture.DBExpect().LoadByJsonText(
		testcommon.NewStringPatternMatcher(".*\"Symbol\":\"fb\"*"),
		SADataTables[SA_STOCKOVERVIEW],
		SADataTypes[SA_STOCKOVERVIEW]).Times(1)

	c := NewSACollector(fixture.Reader(), fixture.Exporter(), fixture.DBMock(), fixture.Logger())
	num, err := c.CollectFinancialOverview("fb")
	if err != nil {
		t.Fatalf("Failed to call CollectFinancialOverview(), error %v", err)
	}

	if num > 0 {
		t.Fatalf("Expecting a few rows were inserted into database table. However %d rows inserted.", num)
	}
}
