package collector_test

import (
	"testing"

	"github.com/wayming/sdc/collector"
)

func TestReadURL(t *testing.T) {
	type args struct {
		url    string
		params map[string]string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "ReadURL",
			args: args{
				url:    "https://stockanalysis.com/stocks/rds.b",
				params: nil,
			},
			want:    "string body",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i := 0; i < 10000; i++ {
				collector.ReadURL(tt.args.url, tt.args.params)
				// got, err := collector.ReadURL(tt.args.url, tt.args.params)
				// if (err != nil) != tt.wantErr {
				// 	fmt.Printf("ReadURL() error = %v, wantErr %v", err, tt.wantErr)
				// 	return
				// }
				// if got != tt.want {
				// 	fmt.Printf("ReadURL() = %v, want %v", got, tt.want)
				// } else {
				// 	fmt.Printf("ReadURL() = %v", got)
				// }
			}
		})

	}
}
