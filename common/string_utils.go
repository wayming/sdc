package common

import (
	"regexp"
	"strings"
	"unicode"
)

func RemoveAllWhitespace(s string) string {
	re := regexp.MustCompile(`\s+`)
	return re.ReplaceAllString(s, "")
}

func RemoveCarriageReturn(str string) string {
	return strings.ReplaceAll(str, "\r", "")
}

func ConvertToPascalCase(input string) string {
	// Split the input string at underscores
	parts := strings.Split(input, "_")

	// Create a string builder to construct the result
	var result string

	// Iterate through each part
	for _, part := range parts {
		if len(part) > 0 {
			// Capitalize the first letter of each part and make the rest lowercase
			result += string(unicode.ToUpper(rune(part[0]))) + strings.ToLower(part[1:])
		}
	}

	return result
}
