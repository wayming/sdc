package json2db

import "testing"

func TestPGDDLGenrator_Gen(t *testing.T) {
	type args struct {
		jsonText string
	}
	tests := []struct {
		name string
		d    *PGDDLGenrator
		args args
		want string
	}{
		{
			name: "Sanity",
			d:    NewPGDDLGenrator(),
			args: args{jsonText: `{
				"name": "Microsoft Corporation",
				"symbol": "MSFT",
				"has_intraday": false,
				"has_eod": true,
				"country": null,
				"stock_exchange": {
					"name": "NASDAQ Stock Exchange",
					"acronym": "NASDAQ",
					"mic": "XNAS",
					"country": "USA",
					"country_code": "US",
					"city": "New York",
					"website": "www.nasdaq.com"
				}
			}`},
			want: `INSERT INTO ....`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &PGDDLGenrator{}
			if got := d.Do(tt.args.jsonText, "sdc_tickers"); got != tt.want {
				t.Errorf("PGDDLGenrator.Gen() = %v, want %v", got, tt.want)
			}
		})
	}
}
