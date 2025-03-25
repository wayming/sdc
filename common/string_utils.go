package common

import (
	"regexp"
	"strings"

	"golang.org/x/text/cases"
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
		// Capitalize the first letter of each part and append it to the result
		result += cases.Title(part)
	}

	return result
}
