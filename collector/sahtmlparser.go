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

func (p *SAHTMLParser) decodeTimeSeriesTable(node *html.Node, dataStructTypeName string) ([]map[string]interface{}, error) {
	completeSeries := make([]map[string]interface{}, 0)
	// thead
	thead := node.FirstChild

	pattern := `[a-zA-Z]+`
	re := regexp.MustCompile(pattern)

	// For each tr
	for tr := thead.FirstChild; tr != nil; tr = tr.NextSibling {
		td := tr.FirstChild
		if td != nil {
			text1 := firstTextNode(td)
			if text1 != nil {
				p.logger.Println(text1.Data)
			}

			for td2 := td.NextSibling; td2 != nil; td2 = td2.NextSibling {
				text2 := firstTextNode(td2)
				if text2 != nil {
					dataPoint := make(map[string]interface{})
					p.logger.Println(text2.Data)
					if matches := re.FindAllString(text2.Data, -1); len(matches) > 0 {
						p.logger.Println("ignore ", text2.Data)
						continue
					}

					normKey := normaliseJSONKey(text1.Data)
					fieldType := GetFieldTypeByTag(p.metricsFields[dataStructTypeName], normKey)
					if fieldType == nil {
						return completeSeries, errors.New("Failed to get field type for tag " + normKey)
					}

					p.logger.Println("Normalise " + text2.Data + " to " + fieldType.Name() + " value")
					normVal, err := normaliseJSONValue(text2.Data, fieldType)
					if err != nil {
						return completeSeries, err
					}

					dataPoint[normKey] = normVal
					completeSeries = append(completeSeries, dataPoint)
					continue
				}
			}
		}
	}

	// tbody
	if thead.NextSibling == nil || thead.NextSibling.NextSibling == nil {
		return nil, errors.New("unexpected structure. Can not find the tbody element")
	}
	tbody := thead.NextSibling.NextSibling
	// For each tr
	for tr := tbody.FirstChild; tr != nil; tr = tr.NextSibling {
		td := tr.FirstChild
		if td != nil {
			text1 := firstTextNode(td)
			if text1 != nil {
				p.logger.Println(text1.Data)
			} else {
				continue
			}

			idx := 0
			// Assumes the same amount of tds as the the thead
			for td2 := td.NextSibling; td2 != nil && idx < len(completeSeries); td2 = td2.NextSibling {
				if td2.Type == html.ElementNode && td2.Data == "td" {
					text2 := firstTextNode(td2)
					if text2 != nil {
						p.logger.Println(text2.Data)

						normKey := normaliseJSONKey(text1.Data)
						fieldType := GetFieldTypeByTag(p.metricsFields[dataStructTypeName], normKey)
						if fieldType == nil {
							return completeSeries, errors.New("Failed to get field type for tag " + normKey)
						}

						p.logger.Println("Normalise " + text2.Data + " to " + fieldType.Name() + " value")
						normVal, err := normaliseJSONValue(text2.Data, fieldType)
						if err != nil {
							return completeSeries, err
						}

						completeSeries[idx][normKey] = normVal
						idx++
					}
				}
			}

		}
	}

	// // Fill symbol name
	// for _, dataPoint := range completeSeries {
	// 	collector.PackSymbolField(dataPoint, dataStructTypeName)
	// }

	return completeSeries, nil

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

	// remove consecutive underscore
	pattern := `_+`
	re := regexp.MustCompile(pattern)
	key = re.ReplaceAllString(key, "_")

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
		if convertedValue, err = time.Parse("2006-01-02", value); err != nil {
			return convertedValue, err
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
