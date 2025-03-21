package collector_test

import (
	"regexp"
	"testing"

	. "github.com/wayming/sdc/collector"
	"github.com/wayming/sdc/sdclogger"
	testcommon "github.com/wayming/sdc/testcommon"
)

func TestHttpReader_Read_Proxy(t *testing.T) {
	oneProxy, _ := testcommon.GetProxy()
	c, _ := NewProxyClient(oneProxy)
	r := NewHttpReader(c)

	var params map[string]string
	want := "This domain is for use in illustrative examples in documents"
	got, err := r.Read("http://example.com", params)
	if err != nil {
		t.Errorf("HttpReader.Read() error = %v", err)
	}
	match, _ := regexp.MatchString(want, got)
	if !match {
		t.Errorf("HttpReader.Read() = %v, want %v", got, want)
	}
}

func TestHttpReader_RedirectedUrl(t *testing.T) {
	fixture := testcommon.NewTestFixture(t)
	defer fixture.Teardown(t)
	sdclogger.SDCLoggerInstance = fixture.Logger()

	r := NewHttpReader(NewLocalClient())

	want := "https://stockanalysis.com/stocks/meta/"
	got, err := r.RedirectedUrl("https://stockanalysis.com/stocks/fb/")
	if err != nil {
		t.Errorf("HttpReader.Read() error = %v", err)
	}
	if got != want {
		t.Errorf("HttpReader.Read() = %v, want %v", got, want)
	}
}
