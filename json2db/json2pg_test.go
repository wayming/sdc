package json2db

import "testing"

func TestDDLGenPG_Gen(t *testing.T) {
	type args struct {
		jsonText string
	}
	tests := []struct {
		name string
		d    *DDLGenPG
		args args
		want string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &DDLGenPG{}
			if got := d.Gen(tt.args.jsonText); got != tt.want {
				t.Errorf("DDLGenPG.Gen() = %v, want %v", got, tt.want)
			}
		})
	}
}
