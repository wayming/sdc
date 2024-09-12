package collector

import (
	"reflect"
	"testing"
)

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
			want: "2005-09-30",
		},
		{
			name: "ValidQuarter",
			args: args{value: "Q3 2006"},
			want: "2006-03-31",
		},
		{
			name: "ValidHalf",
			args: args{value: "H1 2006"},
			want: "2006-06-30",
		},
		{
			name: "ValidHalf",
			args: args{value: "H2 2006"},
			want: "2006-12-31",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := convertFiscalToDate(tt.args.value)
			if err != nil {
				t.Errorf("convertFiscalDate() error: %s", err)
			}
			if got != tt.want {
				t.Errorf("convertFiscalDate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_convertFiscalDate_Invalid(t *testing.T) {
	type args struct {
		value string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "InvalidQuarter",
			args: args{value: "Q5 2006"},
			want: "Q5 2006",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := convertFiscalToDate(tt.args.value)
			if err == nil {
				t.Errorf("convertFiscalDate() expecting error, however convertFiscalToDate succeedes for string %s", tt.args.value)
			}
		})
	}
}

func Test_stringToDate(t *testing.T) {

	want := "2024-06-30 00:00:00"
	t.Run("stringToDate", func(t *testing.T) {
		got, err := stringToDate("2024-06-30")
		if err != nil {
			t.Errorf("stringToDate() error = %v", err)
			return
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("stringToDate() = %v, want %v", got, want)
		}
	})

}
