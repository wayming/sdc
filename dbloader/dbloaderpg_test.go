package dbloader

import (
	"os"
	"testing"
)

func TestPGLoader_Load(t *testing.T) {
	type args struct {
		url string
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "PGLoader_Load",
			args: args{
				url: "http://api.marketstack.com/v1/tickers?access_key=eb6471557c8bebc9fcbcec3667c752dc&exchange=XNAS&limit=10",
			},
			want: 11,
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
			if got := loader.Load(tt.args.url); got != tt.want {
				t.Errorf("PGLoader.Load() = %v, want %v", got, tt.want)
			}
		})
	}
}
