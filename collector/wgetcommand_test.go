package collector

import (
	"testing"
)

func Test_getHttpCode(t *testing.T) {
	type args struct {
		output string
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "Test_getHttpCode",
			args: args{
				output: "HTTP/1.1 308 Permanent Redirect",
			},
			want: 308,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getHttpCode(tt.args.output); got != tt.want {
				t.Errorf("getHttpCode() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWgetCmd_RedirectedUrl(t *testing.T) {
	tests := []struct {
		name    string
		url     string
		want    string
		wantErr bool
	}{
		{
			name:    "TestWgetCmd_RedirectedUrl",
			url:     "https://stockanalysis.com/stocks/fb/financials/?p=quarterly",
			want:    "/stocks/meta/financials/?period=quarterly",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewWgetCmd("--max-redirect=0", "-S", tt.url)
			got, err := c.RedirectedUrl()
			if (err != nil) != tt.wantErr {
				t.Errorf("WgetCmd.RedirectedUrl() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("WgetCmd.RedirectedUrl() = %v, want %v", got, tt.want)
			}
			if !c.HasServerError() {
				t.Errorf("WgetCmd.HasServerError() want %v, got %v", true, false)
			}
			if c.HasNetworkError() {
				t.Errorf("WgetCmd.HasNetworkError() want %v, got %v", false, true)
			}
			if !c.HasServerRedirectedError() {
				t.Errorf("WgetCmd.HasServerRedirectedError() want %v, got %v", true, false)
			}
		})
	}
}
