package common

import (
	"regexp"
	"strings"
)

func RemoveAllWhitespace(s string) string {
	re := regexp.MustCompile(`\s+`)
	return re.ReplaceAllString(s, "")
}

func RemoveCarriageReturn(str string) string {
	return strings.ReplaceAll(str, "\r", "")
}
