package collector

import (
	"errors"
	"fmt"
	"log"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/html"
)

type SAHTMLParser struct {
	logger        *log.Logger
	metricsFields map[string]map[string]JsonFieldMetadata
}

func NewSAHTMLParser(l *log.Logger) *SAHTMLParser {
	return &SAHTMLParser{
		logger:        l,
		metricsFields: AllSAMetricsFields(),
	}
}

func (p *SAHTMLParser) DecodeOverviewPages(node *html.Node, dataStructTypeName string) (map[string]interface{}, error) {
	var indicatorsMap map[string]interface{}
	var err error
	if node.Type == html.ElementNode {
		for _, attr := range node.Attr {
			if attr.Key == "data-test" && attr.Val == "overview-info" {
				indicatorsMap, err = p.decodeSimpleTable(node, dataStructTypeName)
				if err != nil {
					return nil, errors.New("Failed to decode html table overview-info. Error: " + err.Error())
				}
			}
			if attr.Key == "data-test" && attr.Val == "overview-quote" {
				indicatorsMap, err = p.decodeSimpleTable(node, dataStructTypeName)
				if err != nil {
					return nil, errors.New("Failed to decode html table overview-quote. Error: " + err.Error())
				}

			}
		}
	}

	for child := node.FirstChild; child != nil; child = child.NextSibling {
		var moreIndicators map[string]interface{}
		if moreIndicators, err = p.DecodeOverviewPages(child, dataStructTypeName); err != nil {
			return nil, err
		}
		if indicatorsMap, err = concatMaps(indicatorsMap, moreIndicators); err != nil {
			return nil, err
		}
	}

	return indicatorsMap, nil
}

func (p *SAHTMLParser) DecodeFinancialsPage(node *html.Node, dataStructTypeName string) ([]map[string]interface{}, error) {
	if node.Type == html.ElementNode {
		for _, attr := range node.Attr {
			if attr.Key == "data-test" && attr.Val == "financials" {
				indicatorMaps, err := p.decodeTimeSeriesTable(node, dataStructTypeName)
				if err != nil {
					return nil, errors.New("Failed to decode html table financials. Error: " + err.Error())
				}
				return indicatorMaps, nil
			}
		}
	}

	for child := node.FirstChild; child != nil; child = child.NextSibling {
		indicatorMaps, err := p.DecodeFinancialsPage(child, dataStructTypeName)
		if err != nil {
			return nil, err
		}
		if len(indicatorMaps) > 0 {
			return indicatorMaps, nil
		}
	}

	return nil, nil
}

func (p *SAHTMLParser) DecodeAnalystRatingsGrid(node *html.Node, dataStructTypeName string) (map[string]interface{}, error) {
	analystRatinMetrics := make(map[string]interface{})

	htmlFieldTexts := []string{
		"Total Analysts",
		"Consensus Rating",
		"Price Target",
		"Upside",
	}
	for _, fieldText := range htmlFieldTexts {
		if value := textOfAdjacentDiv(node, fieldText); len(value) > 0 {
			p.logger.Printf("Read %s", value)
			normKey := normaliseJSONKey(fieldText)
			fieldType := GetFieldTypeByTag(p.metricsFields[dataStructTypeName], normKey)
			if fieldType == nil {
				return nil, errors.New("Failed to get field type for tag " + normKey)
			}
			p.logger.Println("Normalise " + value + " to " + fieldType.Name() + " value")
			normVal, err := normaliseJSONValue(value, fieldType)
			if err != nil {
				return analystRatinMetrics, err
			}
			p.logger.Printf("Got %v", normVal)

			analystRatinMetrics[normKey] = normVal
		}
	}
	return analystRatinMetrics, nil
}

func (p *SAHTMLParser) decodeSimpleTable(node *html.Node, dataStructTypeName string) (map[string]interface{}, error) {
	simpleTableMetrics := make(map[string]interface{})
	// tbody
	tbody := node.FirstChild

	// For each tr
	for tr := tbody.FirstChild; tr != nil; tr = tr.NextSibling {
		td := tr.FirstChild
		if td != nil {
			text1 := firstTextNode(td)
			if text1 != nil {
				p.logger.Printf("Read %s", text1.Data)
			} else {
				// No text node for this sibling, try next one
				continue
			}

			for td2 := td.NextSibling; td2 != nil; td2 = td2.NextSibling {
				text2 := firstTextNode(td2)
				if text2 != nil {
					normKey := normaliseJSONKey(text1.Data)
					fieldType := GetFieldTypeByTag(p.metricsFields[dataStructTypeName], normKey)
					if fieldType == nil {
						return simpleTableMetrics, errors.New("Failed to get field type for tag " + normKey)
					}

					p.logger.Printf("Read %s", text2.Data)
					p.logger.Println("Normalise " + text2.Data + " to " + fieldType.Name() + " value")
					// TODO - remove n/a value from map
					normVal, err := normaliseJSONValue(text2.Data, fieldType)
					if err != nil {
						return simpleTableMetrics, err
					}
					p.logger.Printf("Got %v", normVal)

					simpleTableMetrics[normKey] = normVal
					continue
				}
			}
		}
	}

	// collector.PackSymbolField(simpleTableMetrics, dataStructTypeName)
	return simpleTableMetrics, nil

}

/*
	dataSeries: []map[string]interface{}{
	            {
	                "period_ending":  "Jan 2, 2006",
	                "Revenue":   10000,
	                "GrossProfit": 100,
	            },
	            {
					...
	            },
				...
			}
*/
func (p *SAHTMLParser) decodeTimeSeriesTable(node *html.Node, dataStructTypeName string) ([]map[string]interface{}, error) {
	var dataPoints []map[string]interface{}
	// thead
	thead := node.FirstChild

	// If skip the first value column
	skipFirstValue := false
	skipLastValue := false
	p.logger.Printf("Metadata for struct %s: %v", dataStructTypeName, p.metricsFields[dataStructTypeName])

	// For each tr
	for tr := thead.FirstChild; tr != nil; tr = tr.NextSibling {
		td := tr.FirstChild
		if td != nil {
			text1 := firstTextNode(td)
			if text1 != nil {
				p.logger.Printf("Read %s", text1.Data)
			}

			// Only process fields that mapps db primary key
			normKey := normaliseJSONKey(text1.Data)
			if !IsKeyField(p.metricsFields[dataStructTypeName], normKey) {
				p.logger.Printf("ignore table header key %s: not db primary key", normKey)
				continue
			}
			fieldType := GetFieldTypeByTag(p.metricsFields[dataStructTypeName], normKey)
			if fieldType == nil {
				p.logger.Printf("ignore table header key %s: no field type found", normKey)
				continue
			}

			var firstSibling *html.Node
			// For each td
			idx := 0
			for td2 := td.NextSibling; td2 != nil; td2 = td2.NextSibling {
				text2 := firstTextNode(td2)
				if text2 == nil {
					continue
				}

				if firstSibling == nil {
					// First td node with text data
					firstSibling = td2
				}

				p.logger.Printf("Read %s", text2.Data)
				if !isValidValue(text2.Data) {
					// Ignore the invalid values of the first or last td
					if td2 == firstSibling {
						// The first value field does not contain a valid value for a key row.
						// Skip this value field for all remaining tr(rows)
						// This is to skip the "Current" column of the table.
						skipFirstValue = true
						p.logger.Printf("ignore value %s for key field %s skipFirstValue=true", text2.Data, normKey)
						continue
					} else if td2.NextSibling == nil {
						skipLastValue = true
						p.logger.Printf("ignore value %s for key field %s skipLastValue=true", text2.Data, normKey)
						continue
					} else {
						return dataPoints, fmt.Errorf("invalid value %s for field %s", text2.Data, normKey)
					}
				}

				p.logger.Println("Normalise " + text2.Data + " to " + fieldType.Name() + " value")
				normVal, err := normaliseJSONValue(text2.Data, fieldType)
				if err != nil {
					return dataPoints, err
				}
				p.logger.Printf("Got %v", normVal)

				if idx == len(dataPoints) {
					// New data point
					dataPoints = append(dataPoints, make(map[string]interface{}))
				}
				dataPoints[idx][normKey] = normVal
				idx++
				continue

			}

			p.logger.Printf("Collect %d data points for key %s", idx, normKey)
		}
	}

	if len(dataPoints) <= 0 {
		return nil, errors.New("faild to get a valid header")
	}

	// tbody
	if thead.NextSibling == nil || thead.NextSibling.NextSibling == nil {
		return nil, errors.New("unexpected structure. Can not find the tbody element")
	}
	tbody := thead.NextSibling.NextSibling
	// For each tr(row)
	for tr := tbody.FirstChild; tr != nil; tr = tr.NextSibling {
		td := tr.FirstChild
		if td != nil {

			// First column is the key column
			text1 := firstTextNode(td)
			if text1 != nil {
				p.logger.Printf("Read %s", text1.Data)
			} else {
				// Skip row if the key is not valid
				continue
			}
			normKey := normaliseJSONKey(text1.Data)

			if td.NextSibling == nil {
				p.logger.Printf("Skip the values for key %s as it does not contain any data fields.", normKey)
			}

			// For each remaining td(column)
			var values []any
			var firstSibling *html.Node
			for td2 := td.NextSibling; td2 != nil; td2 = td2.NextSibling {
				if td2.Type == html.ElementNode && td2.Data == "td" {
					text2 := firstTextNode(td2)
					if text2 != nil {
						if firstSibling == nil {
							// First td node with text data
							firstSibling = td2
						}

						if td2 == firstSibling && skipFirstValue {
							p.logger.Printf("skip first column %s for field %s", text2.Data, normKey)
							continue
						}
						if td2.NextSibling == nil && skipLastValue {
							p.logger.Printf("skip last column %s for field %s", text2.Data, normKey)
							continue
						}

						p.logger.Printf("Read %s", text2.Data)
						if !isValidValue(text2.Data) {
							if newVal, ok := fillDefaultValue(text2.Data); ok {
								p.logger.Printf("Invalid value %s, fill in %s", text2.Data, newVal)
								text2.Data = newVal
							} else {
								p.logger.Printf("Ignore value %s", text2.Data)
								continue
							}
						}

						fieldType := GetFieldTypeByTag(p.metricsFields[dataStructTypeName], normKey)
						if fieldType == nil {
							return dataPoints, errors.New("Failed to get field type for tag " + normKey)
						}

						p.logger.Println("Normalise " + text2.Data + " to " + fieldType.Name() + " value")
						normVal, err := normaliseJSONValue(text2.Data, fieldType)
						if err != nil {
							return dataPoints, err
						}
						p.logger.Printf("Got %v", normVal)

						values = append(values, normVal)
						// dataSeries[idx][normKey] = normVal
						// idx++
					}
				}
			}

			if len(values) != len(dataPoints) {
				p.logger.Printf("The key field has %d data points, however, the field %s has %d data points. Ignore the field.",
					len(dataPoints), normKey, len(values))
				continue
			}

			// Fill value for each data point
			for idx, v := range values {
				dataPoints[idx][normKey] = v
			}

		}
	}

	// // Fill symbol name
	// for _, dataPoint := range dataSeries {
	// 	collector.PackSymbolField(dataPoint, dataStructTypeName)
	// }

	return dataPoints, nil

}

func firstTextNode(node *html.Node) *html.Node {

	if node.Type == html.TextNode && len(strings.TrimSpace(node.Data)) > 0 {
		// p.logger.Println(node.Data)
		return node
	}

	for child := node.FirstChild; child != nil; child = child.NextSibling {
		textNode := firstTextNode(child)
		if textNode != nil {
			return textNode
		}
	}

	return nil
}

func normaliseJSONKey(key string) string {
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

func stringToFloat64(value string) (any, error) {
	baseNumber, sign, multi := normaliseValueToNumeric(value)
	valFloat, err := strconv.ParseFloat(baseNumber, 64)
	if err != nil {
		return nil, err
	}

	return float64(sign) * valFloat * float64(multi), nil
}

func stringToInt64(value string) (any, error) {
	baseNumber, sign, multi := normaliseValueToNumeric(value)
	valInt, err := strconv.ParseInt(baseNumber, 10, 64)
	if err != nil {
		return nil, err
	}

	return int64(float64(sign) * float64(valInt) * multi), nil
}

func stringToDate(value string) (any, error) {
	convertedValue, err := time.Parse("2006-01-02", value)
	if err == nil {
		return convertedValue.Format("2006-01-02T15:04:05-07:00"), nil
	}

	convertedValue, err = time.Parse("Jan 2, 2006", value)
	if err == nil {
		return convertedValue.Format("2006-01-02T15:04:05-07:00"), nil
	}

	format := "Jan '06"
	convertedValue, err = time.Parse(format, value)
	if err == nil {
		// Since the parsed time represents only the month and year, default to the first day of the month
		year := convertedValue.Year()
		month := convertedValue.Month()
		day := 1 // Defaulting to the first day of the month

		// Construct a new time.Time object representing January 1, 2006
		convertedValue = time.Date(year, month, day, 0, 0, 0, 0, nil)
		return convertedValue.Format("2006-01-02T15:04:05-07:00"), nil
	} else {
		return "", err
	}
}

func addToYear(yearStr string, add int) (string, error) {
	if year, err := strconv.Atoi(yearStr); err != nil {
		return "", err
	} else {
		return strconv.Itoa(year + add), nil
	}
}
func convertFiscalToDate(value string) (string, error) {
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
				if matches[3], err = addToYear(matches[3], -1); err != nil {
					return "", err
				}
			case "2":
				monthAndDay = "12-31"
				if matches[3], err = addToYear(matches[3], -1); err != nil {
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
func isValidDate(value string) bool {
	_, err := stringToDate(value)

	// If there is no error, the string is a valid date
	return err == nil
}

func isFiscalDate(value string) bool {
	pattern := `[QH]\d \d{4}`
	re := regexp.MustCompile(pattern)
	matches := re.FindString(value)
	return len(matches) > 0
}

func isValidValue(value string) bool {
	value = strings.TrimSpace(value)
	// Null Value

	if value == "-" {
		return false
	}

	pattern := `[a-zA-Z]+`
	re := regexp.MustCompile(pattern)

	matches := re.FindAllString(value, -1)
	// The value shold not contain characters except date
	if len(matches) > 0 && !isValidDate(value) && !isFiscalDate(value) {
		return false
	}

	return true
}

func fillDefaultValue(value string) (string, bool) {
	value = strings.TrimSpace(value)
	// Null Value

	if len(value) == 0 {
		return "0", true
	}

	if value == "-" {
		return "0", true
	}

	return "", false
}

// Normalised input string value for numeric conversion
// Return normalised string, operator, multiplier
func normaliseValueToNumeric(value string) (string, int, float64) {

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

func normaliseJSONValue(value string, vType reflect.Type) (any, error) {
	var convertedValue any
	var err error

	value = strings.TrimSpace(value)

	switch vType.Kind() {
	case reflect.Float64:
		if convertedValue, err = stringToFloat64(value); err != nil {
			return nil, err
		}
	case reflect.Int64:
		if convertedValue, err = stringToInt64(value); err != nil {
			return nil, err
		}
	case reflect.String:
		convertedValue = value
	}

	if vType == reflect.TypeFor[time.Time]() {
		if isFiscalDate((value)) {
			if value, err = convertFiscalToDate(value); err != nil {
				return nil, err
			}
			if convertedValue, err = stringToDate(value); err != nil {
				return convertedValue, err
			}
		}
	}

	return convertedValue, nil
}

func textOfAdjacentDiv(node *html.Node, firstData string) string {
	if node.Type == html.ElementNode && node.Data == "div" {
		textNode := firstTextNode(node)
		if textNode != nil && strings.TrimSpace(textNode.Data) == firstData {
			if textNode.Parent != nil && textNode.Parent.NextSibling != nil && textNode.Parent.NextSibling.NextSibling != nil {
				if adjacentTextNode := firstTextNode(textNode.Parent.NextSibling.NextSibling); adjacentTextNode != nil {
					return strings.TrimSpace(adjacentTextNode.Data)
				}
			}
		}
	}

	for child := node.FirstChild; child != nil; child = child.NextSibling {

		if data := textOfAdjacentDiv(child, firstData); len(data) > 0 {
			return data
		}
	}

	return ""
}
