package collector

import "testing"

func Test_convertFiscalDate(t *testing.T) {
	type args struct {
		value string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "ValidQuarter",
			args: args{value: "Q1 2006"},
			want: "2006-01-01",
		},
		{
			name: "ValidQuarter",
			args: args{value: "Q3 2006"},
			want: "2006-07-01",
		},
		{
			name: "ValidHalf",
			args: args{value: "H1 2006"},
			want: "2006-01-01",
		},
		{
			name: "ValidHalf",
			args: args{value: "H2 2006"},
			want: "2006-07-01",
		},
		{
			name: "InvalidQuarter",
			args: args{value: "Q5 2006"},
			want: "Q5 2006",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := convertFiscalToDate(tt.args.value); got != tt.want {
				t.Errorf("convertFiscalDate() = %v, want %v", got, tt.want)
			}
		})
	}
}
