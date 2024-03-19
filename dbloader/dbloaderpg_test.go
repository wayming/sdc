package dbloader

import (
	"os"
	"testing"
)

const JSON_TEXT = `{
    "pagination": {
        "limit": 10000,
        "offset": 0,
        "count": 250,
        "total": 250
    },
    "data": [
        {
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
        }
    ]
}`

func TestPGLoader_LoadByJsonResponse(t *testing.T) {
	type args struct {
		JsonResponse string
		tableName    string
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "PGLoader_Load",
			args: args{
				JsonResponse: JSON_TEXT,
				tableName:    "sdc_tickers",
			},
			want: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			loader := NewPGLoader()
			loader.Connect(os.Getenv("PGHOST"),
				os.Getenv("PGPORT"),
				os.Getenv("PGUSER"),
				os.Getenv("PGPASSWORD"),
				os.Getenv("PGDATABASE"))
			defer loader.Disconnect()
			if got := loader.LoadByJsonResponse(tt.args.JsonResponse, tt.args.tableName); got != tt.want {
				t.Errorf("PGLoader.Load() = %v, want %v", got, tt.want)
			}
		})
	}
}
