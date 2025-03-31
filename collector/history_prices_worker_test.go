package collector

import (
	"reflect"
	"testing"
)

func TestHistPriceDownloader_normaliseJSONText(t *testing.T) {
	inputJSONText := `{"results":[{"date":"2010-01-04","open":7.622499942779541,"high":7.660714149475098,"low":7.585000038146973,"close":7.643214225769043,"volume":493729600,"split_ratio":0.0,"dividend":0.0},{"date":"2010-01-05","open":7.664286136627197,"high":7.699643135070801,"low":7.6160712242126465,"close":7.656428813934326,"volume":601904800,"split_ratio":0.0,"dividend":0.0}]}`
	want := `[
    {
        "close": 7.643214225769043,
        "date": "2010-01-04",
        "dividend": 0,
        "high": 7.660714149475098,
        "low": 7.585000038146973,
        "open": 7.622499942779541,
        "split_ratio": 0,
        "volume": 493729600
    },
    {
        "close": 7.656428813934326,
        "date": "2010-01-05",
        "dividend": 0,
        "high": 7.699643135070801,
        "low": 7.6160712242126465,
        "open": 7.664286136627197,
        "split_ratio": 0,
        "volume": 601904800
    }
]`
	t.Run("TestHistPriceDownloader_normaliseJSONText", func(t *testing.T) {
		d := &HistPriceDownloader{norm: &SAJsonNormaliser{}}
		got, err := d.normaliseJSONText(inputJSONText)
		if err != nil {
			t.Errorf("HistPriceDownloader.normaliseJSONText() error = %v", err)
			return
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("normaliseJSONText() = %v, want %v", got, want)
		}
	})

}
