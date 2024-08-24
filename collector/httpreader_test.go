package collector_test

import (
	"regexp"
	"testing"

	. "github.com/wayming/sdc/collector"
	testcommon "github.com/wayming/sdc/testcommon"
)

func TestHttpReader_Read(t *testing.T) {
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
