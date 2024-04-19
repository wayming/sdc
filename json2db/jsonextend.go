package json2db

import (
	"time"
)

type Date struct {
	time.Time
}

func (ct *Date) UnmarshalJSON(b []byte) error {
	s := string(b)
	t, err := time.Parse("2006-01-02", s[1:len(s)-1]) // Assuming the JSON string is wrapped in double quotes
	if err != nil {
		return err
	}
	ct.Time = t
	return nil
}
