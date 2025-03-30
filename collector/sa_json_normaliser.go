package collector

import (
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/wayming/sdc/json2db"
)

type SAJsonNormaliser struct {
}

func (n SAJsonNormaliser) NormaliseJSONKey(key string) string {
	// lower case name
	key = strings.ToLower(key)

	// trim spaces
	key = strings.TrimSpace(key)

	// replace space with underscore
	key = strings.ReplaceAll(key, " ", "_")

	// replace ampersand with underscore
	key = strings.ReplaceAll(key, "&", "_")

	// replace slash with underscore
	key = strings.ReplaceAll(key, "/", "_")

	// replace dash with underscore
	key = strings.ReplaceAll(key, "-", "_")

	// Replace commas with underscore
	key = strings.ReplaceAll(key, ",", "_")

	// remove apostrophe
	key = strings.ReplaceAll(key, "'", "")

	// remove parenthesis
	key = strings.ReplaceAll(key, "(", "")
	key = strings.ReplaceAll(key, ")", "")

	// remove dot
	key = strings.ReplaceAll(key, ".", "")

	// remove consecutive underscore
	pattern := `_+`
	re := regexp.MustCompile(pattern)
	key = re.ReplaceAllString(key, "_")

	// Workaround. Symbols have different name for the same field.
	if key == "quarter_ending" {
		key = "period_ending"
	}

	return key
}

func (n SAJsonNormaliser) NormaliseJSONValue(value string, vType reflect.Type) (any, error) {
	var convertedValue any
	var err error

	value = strings.TrimSpace(value)

	switch vType.Kind() {
	case reflect.Float64:
		if convertedValue, err = n.stringToFloat64(value); err != nil {
			return nil, err
		}
	case reflect.Int64:
		if convertedValue, err = n.stringToInt64(value); err != nil {
			return nil, err
		}
	case reflect.String:
		convertedValue = value
	}

	if vType == reflect.TypeFor[time.Time]() ||
		vType == reflect.TypeFor[json2db.Date]() {
		if n.isFiscalQuarterFormat((value)) {
			if value, err = n.convertFiscalQuarterToDate(value); err != nil {
				return nil, err
			}
		}
		if convertedValue, err = n.stringToDate(value); err != nil {
			return convertedValue, err
		}
	}

	return convertedValue, nil
}

func (n SAJsonNormaliser) stringToFloat64(value string) (any, error) {
	baseNumber, sign, multi := n.normaliseValueToNumeric(value)
	valFloat, err := strconv.ParseFloat(baseNumber, 64)
	if err != nil {
		return nil, err
	}

	return float64(sign) * valFloat * float64(multi), nil
}

func (n SAJsonNormaliser) stringToInt64(value string) (any, error) {
	baseNumber, sign, multi := n.normaliseValueToNumeric(value)
	valInt, err := strconv.ParseInt(baseNumber, 10, 64)
	if err != nil {
		return nil, err
	}

	return int64(float64(sign) * float64(valInt) * multi), nil
}

func (n SAJsonNormaliser) stringToDate(value string) (any, error) {
	convertedValue, err := time.Parse("2006-01-02", value)
	if err == nil {
		return convertedValue.Format("2006-01-02"), nil
	}

	convertedValue, err = time.Parse("Jan 2, 2006", value)
	if err == nil {
		return convertedValue.Format("2006-01-02"), nil
	}

	format := "Jan '06"
	convertedValue, err = time.Parse(format, value)
	if err == nil {
		// Since the parsed time represents only the month and year, default to the first day of the month
		year := convertedValue.Year()
		month := convertedValue.Month()
		day := 1 // Defaulting to the first day of the month

		// Construct a new time.Time object representing January 1, 2006
		convertedValue = time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
		return convertedValue.Format("2006-01-02"), nil
	} else {
		return "", err
	}
}

func (n SAJsonNormaliser) addToYear(yearStr string, add int) (string, error) {
	if year, err := strconv.Atoi(yearStr); err != nil {
		return "", err
	} else {
		return strconv.Itoa(year + add), nil
	}
}
func (n SAJsonNormaliser) convertFiscalQuarterToDate(value string) (string, error) {
	pattern := `([QH])(\d) (\d{4})`
	re := regexp.MustCompile(pattern)
	matches := re.FindStringSubmatch(value)

	if len(matches) > 0 {
		var monthAndDay string
		var err error
		if matches[1] == "Q" {
			switch matches[2] {
			case "1":
				monthAndDay = "09-30"
				if matches[3], err = n.addToYear(matches[3], -1); err != nil {
					return "", err
				}
			case "2":
				monthAndDay = "12-31"
				if matches[3], err = n.addToYear(matches[3], -1); err != nil {
					return "", err
				}
			case "3":
				monthAndDay = "03-31"
			case "4":
				monthAndDay = "06-30"
			default:
				return "", fmt.Errorf("unsupported date format, %s", value)
			}
		} else if matches[1] == "H" {
			switch matches[2] {
			case "1":
				monthAndDay = "06-30"
			case "2":
				monthAndDay = "12-31"
			default:
				return "", fmt.Errorf("unsupported date format, %s", value)
			}
		}
		return matches[3] + "-" + monthAndDay, nil
	} else {
		return "", fmt.Errorf("unsupported date format, %s", value)
	}
}

// func (n SAJsonNormaliser) isValidDate(value string) bool {
// 	_, err := stringToDate(value)

// 	// If there is no error, the string is a valid date
// 	return err == nil
// }

// Pattern to match "Q1 2024"
func (n SAJsonNormaliser) isFiscalQuarterFormat(value string) bool {
	pattern := `[QH]\d \d{4}`
	re := regexp.MustCompile(pattern)
	matches := re.FindString(value)
	return len(matches) > 0
}

// Pattern to match "Sep '24" or "Mar '22"
func (n SAJsonNormaliser) isFiscalMonthYearFormat(value string) bool {
	pattern := `^[A-Za-z]{3} '\d{2}$`
	re := regexp.MustCompile(pattern)

	// Use FindString to find a match
	matches := re.FindString(value)

	// Check if matches is not empty, meaning we found a match
	return len(matches) > 0
}

// func (n SAJsonNormaliser) isValidValue(value string) bool {
// 	value = strings.TrimSpace(value)
// 	// Null Value

// 	if value == "-" {
// 		return false
// 	}

// 	pattern := `[a-zA-Z]+`
// 	re := regexp.MustCompile(pattern)

// 	matches := re.FindAllString(value, -1)
// 	// The value shold not contain characters except date
// 	if len(matches) > 0 && !n.isValidDate(value) && !n.isFiscalQuarterFormat(value) {
// 		return false
// 	}

// 	return true
// }

// Normalised input string value for numeric conversion
// Return normalised string, operator, multiplier
func (n SAJsonNormaliser) normaliseValueToNumeric(value string) (string, int, float64) {

	// Remove spaces
	value = strings.ReplaceAll(value, " ", "")

	// Handle n/a. Return 0.
	if strings.ToLower(value) == "n/a" {
		return "0", 0, 0
	}

	// Remove double quotes
	value = strings.ReplaceAll(value, "\"", "")

	// Remove commas
	value = strings.ReplaceAll(value, ",", "")

	// Remove dollors
	value = strings.ReplaceAll(value, "$", "")

	// Remove (.*)
	re := regexp.MustCompile(`\(.*\)`)
	value = re.ReplaceAllString(value, "")

	// Sign operator
	sign := 1
	if value[0] == '-' {
		if len(value) == 1 {
			return "0", 0, 0
		}
		sign = -1
		value = value[1:]
	}
	if value[0] == '+' {
		value = value[1:]
	}

	valLen := len(value)
	re = regexp.MustCompile(`^[.\d]+[KBMT%]?$`)
	multiplier := float64(1)
	baseNumber := value
	if re.Match([]byte(value)) {

		switch value[valLen-1] {
		case 'K':
			multiplier = multiplier * 1000
			baseNumber = value[:valLen-1]
		case 'M':
			multiplier = multiplier * 1000 * 1000
			baseNumber = value[:valLen-1]
		case 'B':
			multiplier = multiplier * 1000 * 1000 * 1000
			baseNumber = value[:valLen-1]
		case 'T':
			multiplier = multiplier * 1000 * 1000 * 1000 * 1000
			baseNumber = value[:valLen-1]
		case '%':
			multiplier = multiplier / 100
			baseNumber = value[:valLen-1]
		}
	}

	return baseNumber, sign, multiplier
}
