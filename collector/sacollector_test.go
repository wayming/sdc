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
