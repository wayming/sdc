package collector_test

import (
	"testing"

	. "github.com/wayming/sdc/collector"
)

func TestIsKeyField(t *testing.T) {
	type args struct {
		fieldsMetadata map[string]JsonFieldMetadata
		fieldName      string
	}
	metaData := AllSAMetricsFields()[SADataTypes[SA_FINANCIALSINCOME].Name()]
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "DBPrimaryKey",
			args: args{
				fieldsMetadata: metaData,
				fieldName:      "fiscal_quarter",
			},
			want: true,
		},
		{
			name: "NotDBPrimaryKey",
			args: args{
				fieldsMetadata: metaData,
				fieldName:      "period_ending",
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsKeyField(tt.args.fieldsMetadata, tt.args.fieldName); got != tt.want {
				t.Errorf("IsKeyField() = %v, want %v", got, tt.want)
			}
		})
	}
}
