package collector

import (
	"errors"
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
				p.logger.Println(text1.Data)
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

					p.logger.Println("Normalise " + text2.Data + " to " + fieldType.Name() + " value")
					// TODO - remove n/a value from map
					normVal, err := normaliseJSONValue(text2.Data, fieldType)
					if err != nil {
						return simpleTableMetrics, err
					}

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
	dataSeries := make([]map[string]interface{}, 0)
	// thead
	thead := node.FirstChild

	// If skip the first value column
	skipFirstValue := false

	// Count of column for the key field
	keyFiledValueCnt := 0

	// For each tr
	dataPoints := make(map[string][]interface{})
	for tr := thead.FirstChild; tr != nil; tr = tr.NextSibling {
		td := tr.FirstChild
		if td != nil {
			text1 := firstTextNode(td)
			if text1 != nil {
				p.logger.Println(text1.Data)
			}

			normKey := normaliseJSONKey(text1.Data)
			fieldType := GetFieldTypeByTag(p.metricsFields[dataStructTypeName], normKey)
			if fieldType == nil {
				p.logger.Println("ignore key ", normKey)
				continue
			}

			dataPoints[normKey] = make([]interface{}, 0)
			firstSibling := td.NextSibling
			var values []any
			for td2 := td.NextSibling; td2 != nil; td2 = td2.NextSibling {
				text2 := firstTextNode(td2)
				if text2 != nil {
					p.logger.Println(text2.Data)
					if !isValidValue(text2.Data) {
						p.logger.Println("ignore value ", text2.Data)

						if IsKeyField(p.metricsFields[dataStructTypeName], normKey) && td2 == firstSibling {
							// The first value field does not contain a valid value for a key row.
							// Skip this value field for all remaining tr(rows)
							// This is to skip the "Current" column of the table.
							skipFirstValue = true
						}
						continue
					}

					p.logger.Println("Normalise " + text2.Data + " to " + fieldType.Name() + " value")
					normVal, err := normaliseJSONValue(text2.Data, fieldType)
					if err != nil {
						return dataSeries, err
					}
					values = append(values, normVal)

					continue
				}
			}

			for _, v := range values {
				dataPoints[normKey] = append(dataPoints[normKey], v)
			}

			if IsKeyField(p.metricsFields[dataStructTypeName], normKey) {
				keyFiledValueCnt = len(values)
			}
		}
	}
	for key, s := range dataPoints {
		dataPoint := make(map[string]interface{})
		for _, v := range s {
			dataPoint[key] = v
		}
		dataSeries = append(dataSeries, dataPoint)
	}

	if len(dataSeries) <= 0 {
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
				p.logger.Println(text1.Data)
			} else {
				// Skip row if the key is not valid
				continue
			}
			normKey := normaliseJSONKey(text1.Data)

			if td.NextSibling == nil {
				p.logger.Printf("Skip the values for key %s as it does not contain any data fields.", normKey)
			}

			// Skil the first value column ("current")
			if skipFirstValue {
				td = td.NextSibling
			}

			// For each remaining td(column)
			var values []any
			for td2 := td.NextSibling; td2 != nil; td2 = td2.NextSibling {
				if td2.Type == html.ElementNode && td2.Data == "td" {
					text2 := firstTextNode(td2)
					if text2 != nil {
						p.logger.Println(text2.Data)
						if !isValidValue(text2.Data) {
							p.logger.Println("ignore value ", text2.Data)
							continue
						}

						fieldType := GetFieldTypeByTag(p.metricsFields[dataStructTypeName], normKey)
						if fieldType == nil {
							return dataSeries, errors.New("Failed to get field type for tag " + normKey)
						}

						p.logger.Println("Normalise " + text2.Data + " to " + fieldType.Name() + " value")
						normVal, err := normaliseJSONValue(text2.Data, fieldType)
						if err != nil {
							return dataSeries, err
						}

						values = append(values, normVal)
						// dataSeries[idx][normKey] = normVal
						// idx++
					}
				}
			}

			if len(values) != keyFiledValueCnt {
				p.logger.Printf("The key field has %d data points, however, the field %s has %d data points. Ignore the field.",
					keyFiledValueCnt, normKey, len(values))
			}
			for _, v := range values {
				dataPoints[normKey] = append(dataPoints[normKey], v)
			}
		}
	}

	// // Fill symbol name
	// for _, dataPoint := range dataSeries {
	// 	collector.PackSymbolField(dataPoint, dataStructTypeName)
	// }

	return dataSeries, nil

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

	if key == "quarter_ending" {
		key = "period_ending"
	}

	return key
}

func stringToFloat64(value string) (any, error) {
	baseNumber, sign, multi := normaliseValueForNumeric(value)
	valFloat, err := strconv.ParseFloat(baseNumber, 64)
	if err != nil {
		return nil, err
	}

	return float64(sign) * valFloat * float64(multi), nil
}

func stringToInt64(value string) (any, error) {
	baseNumber, sign, multi := normaliseValueForNumeric(value)
	valInt, err := strconv.ParseInt(baseNumber, 10, 64)
	if err != nil {
		return nil, err
	}

	return int64(float64(sign) * float64(valInt) * multi), nil
}

func stringToDate(value string) (any, error) {
	convertedValue, err := time.Parse("2006-01-02", value)

	if err == nil {
		return convertedValue, err
	}

	convertedValue, err = time.Parse("Jan 2, 2006", value)
	if err == nil {
		return convertedValue, err
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
	}

	return convertedValue, err
}

func convertFiscalToDate(value string) string {
	pattern := `([QH])(\d) (\d{4})`
	re := regexp.MustCompile(pattern)
	matches := re.FindStringSubmatch(value)

	if len(matches) > 0 {
		var monthAndDay string
		if matches[1] == "Q" {
			switch matches[2] {
			case "1":
				monthAndDay = "01-01"
			case "2":
				monthAndDay = "04-01"
			case "3":
				monthAndDay = "07-01"
			case "4":
				monthAndDay = "01-01"
			default:
				return value
			}
		} else if matches[1] == "H" {
			switch matches[2] {
			case "1":
				monthAndDay = "01-01"
			case "2":
				monthAndDay = "07-01"
			default:
				return value
			}
		}
		return matches[3] + "-" + monthAndDay
	} else {
		return value
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

// Normalised input string value for numeric conversion
// Return normalised string, operator, multiplier
func normaliseValueForNumeric(value string) (string, int, float64) {

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
	re = regexp.MustCompile(`^[.\d]+[BMT%]?$`)
	multiplier := float64(1)
	baseNumber := value
	if re.Match([]byte(value)) {

		switch value[valLen-1] {
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
			value = convertFiscalToDate(value)
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
