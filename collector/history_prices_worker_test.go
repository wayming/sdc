package collector

import (
	"fmt"
	"testing"
)

func TestHistPriceDownloader_normaliseJSONText(t *testing.T) {
	inputJSONText := `{"results":[{"date":"2010-01-04","open":7.622499942779541,"high":7.660714149475098,"low":7.585000038146973,"close":7.643214225769043,"volume":493729600,"split_ratio":0.0,"dividend":0.0},{"date":"2010-01-05","open":7.664286136627197,"high":7.699643135070801,"low":7.6160712242126465,"close":7.656428813934326,"volume":601904800,"split_ratio":0.0,"dividend":0.0}]}"`

	t.Run("TestHistPriceDownloader_normaliseJSONText", func(t *testing.T) {
		d := &HistPriceDownloader{}
		got, err := d.normaliseJSONText(inputJSONText)
		if err != nil {
			t.Errorf("HistPriceDownloader.normaliseJSONText() error = %v", err)
			return
		}
		fmt.Println(got)
	})

}
