package collector

import (
	"encoding/json"
	"errors"
	"log"
	"reflect"
	"regexp"
	"strings"

	"github.com/wayming/sdc/dbloader"
	"golang.org/x/net/html"
)

type SACollector struct {
	dbLoader      dbloader.DBLoader
	logger        *log.Logger
	dbSchema      string
	metricsFields map[string]map[string]reflect.Type
	accessKey     string
}

func NewSACollector(loader dbloader.DBLoader, logger *log.Logger, schema string) *SACollector {
	collector := SACollector{
		dbLoader:      loader,
		logger:        logger,
		dbSchema:      schema,
		metricsFields: AllSAMetricsFields(),
		accessKey:     "",
	}
	return &collector
}

func (collector *SACollector) DecodeDualTableHTML(node *html.Node, dataStructTypeName string) (map[string]interface{}, error) {
	var indicatorsMap map[string]interface{}
	var err error
	if node.Type == html.ElementNode {
		for _, attr := range node.Attr {
			if attr.Key == "data-test" && attr.Val == "overview-info" {
				indicatorsMap, err = collector.DecodeSimpleTable(node, dataStructTypeName)
				if err != nil {
					return nil, errors.New("Faield to decode html table overview-info. Error: " + err.Error())
				}

			}
			if attr.Key == "data-test" && attr.Val == "overview-quote" {
				indicatorsMap, err = collector.DecodeSimpleTable(node, dataStructTypeName)
				if err != nil {
					return nil, errors.New("Faield to decode html table overview-quote. Error: " + err.Error())
				}

			}
		}
	}

	for child := node.FirstChild; child != nil; child = child.NextSibling {
		var moreIndicators map[string]interface{}
		if moreIndicators, err = collector.DecodeDualTableHTML(child, dataStructTypeName); err != nil {
			return nil, err
		}
		if indicatorsMap, err = concatMaps(indicatorsMap, moreIndicators); err != nil {
			return nil, err
		}
	}

	return indicatorsMap, nil
}

func (collector *SACollector) DecodeTimeSeriesTableHTML(node *html.Node) ([]map[string]string, error) {
	if node.Type == html.ElementNode {
		for _, attr := range node.Attr {
			if attr.Key == "data-test" && attr.Val == "financials" {
				indicatorMaps, err := collector.DecodeTimeSeriesTable(node)
				if err != nil {
					return nil, errors.New("Faield to decode html table overview-quote. Error: " + err.Error())
				}
				return indicatorMaps, nil
			}
		}
	}

	for child := node.FirstChild; child != nil; child = child.NextSibling {
		indicatorMaps, err := collector.DecodeTimeSeriesTableHTML(child)
		if err != nil {
			return nil, err
		}
		if len(indicatorMaps) > 0 {
			return indicatorMaps, nil
		}
	}

	return nil, nil
}

func (collector *SACollector) FirstTextNode(node *html.Node) *html.Node {

	if node.Type == html.TextNode && len(strings.TrimSpace(node.Data)) > 0 {
		// collector.logger.Println(node.Data)
		return node
	}

	for child := node.FirstChild; child != nil; child = child.NextSibling {
		textNode := collector.FirstTextNode(child)
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

func normaliseJSONValue(value string, vType reflect.Type) any {
	return value
}

func (collector *SACollector) DecodeSimpleTable(node *html.Node, dataStructTypeName string) (map[string]interface{}, error) {
	stockOverview := make(map[string]interface{})
	// tbody
	tbody := node.FirstChild

	// For each tr
	for tr := tbody.FirstChild; tr != nil; tr = tr.NextSibling {
		td := tr.FirstChild
		if td != nil {
			text1 := collector.FirstTextNode(td)
			if text1 != nil {
				collector.logger.Println(text1.Data)
			} else {
				// No text node for this sibling, try next one
				continue
			}

			for td2 := td.NextSibling; td2 != nil; td2 = td2.NextSibling {
				text2 := collector.FirstTextNode(td2)
				if text2 != nil {
					collector.logger.Println(text2.Data)
					stockOverview[normaliseJSONKey(text1.Data)] =
						normaliseJSONValue(text2.Data, collector.metricsFields[dataStructTypeName][normaliseJSONKey(text1.Data)])
					continue
				}
			}
		}
	}
	return stockOverview, nil

}

func (collector *SACollector) DecodeTimeSeriesTable(node *html.Node) ([]map[string]string, error) {
	completeSeries := make([]map[string]string, 0)
	// thead
	thead := node.FirstChild

	pattern := `[a-zA-Z]+`
	re := regexp.MustCompile(pattern)

	// For each tr
	for tr := thead.FirstChild; tr != nil; tr = tr.NextSibling {
		td := tr.FirstChild
		if td != nil {
			text1 := collector.FirstTextNode(td)
			if text1 != nil {
				collector.logger.Println(text1.Data)
			}

			for td2 := td.NextSibling; td2 != nil; td2 = td2.NextSibling {
				text2 := collector.FirstTextNode(td2)
				if text2 != nil {
					dataPoint := make(map[string]string)
					collector.logger.Println(text2.Data)
					if matches := re.FindAllString(text2.Data, -1); len(matches) > 0 {
						collector.logger.Println("ignore ", text2.Data)
						continue
					}
					dataPoint[normaliseJSONKey(text1.Data)] = text2.Data
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
			text1 := collector.FirstTextNode(td)
			if text1 != nil {
				collector.logger.Println(text1.Data)
			} else {
				continue
			}

			idx := 0
			// Assumes the same amount of tds as the the thead
			for td2 := td.NextSibling; td2 != nil && idx < len(completeSeries); td2 = td2.NextSibling {
				if td2.Type == html.ElementNode && td2.Data == "td" {
					text2 := collector.FirstTextNode(td2)
					if text2 != nil {
						collector.logger.Println(text2.Data)
						completeSeries[idx][normaliseJSONKey(text1.Data)] = text2.Data
						idx++
					}
				}
			}
		}
	}
	return completeSeries, nil

}

// Parse Stock Analysis page and return JSON text
func (collector *SACollector) ReadOverallPage(url string, params map[string]string) (string, error) {
	collector.logger.Println("Read " + url)

	htmlContent, err := ReadURL(url, params)
	if err != nil {
		return "", err
	}

	htmlDoc, err := html.Parse(strings.NewReader(htmlContent))
	if err != nil {
		return "", errors.New("Failed to parse the html page " + url + ". Error: " + err.Error())
	}

	indicatorsMap, err := collector.DecodeDualTableHTML(htmlDoc)
	if err != nil {
		return "", errors.New("Failed to parse " + url + ". Error: " + err.Error())
	}
	mapsArray := []map[string]string{indicatorsMap}
	jsonData, err := json.Marshal(mapsArray)
	if err != nil {
		return "", errors.New("Failed to marshal stock data to JSON text. Error: " + err.Error())
	} else {
		collector.logger.Println("JSON text generated - " + string(jsonData))

	}
	return string(jsonData), nil
}

func (collector *SACollector) LoadOverallPage(symbol string) (int64, error) {
	overallUrl := "https://stockanalysis.com/stocks/" + symbol
	overalTable := "sa_overall"
	jsonText, err := collector.ReadOverallPage(overallUrl, nil)
	if err != nil {
		return 0, errors.New("Failed to scrap data from url " + overallUrl + ". Error: " + err.Error())
	}

	numOfRows, err := collector.dbLoader.LoadByJsonText(jsonText, overalTable, reflect.TypeFor[StockOverview]())
	if err != nil {
		return 0, errors.New("Failed to load data into table " + overalTable + ". Error: " + err.Error())
	}

	collector.logger.Println(numOfRows, "rows have been loaded into", overalTable)
	return numOfRows, nil
}

// Parse Stock Analysis page and return JSON text
func (collector *SACollector) ReadTimeSeriesPage(url string, params map[string]string) (string, error) {
	htmlContent, err := ReadURL(url, params)
	if err != nil {
		return "", err
	}

	htmlDoc, err := html.Parse(strings.NewReader(htmlContent))
	if err != nil {
		return "", errors.New("Failed to parse the html page " + url + ". Error: " + err.Error())
	}

	indicatorsMap, err := collector.DecodeTimeSeriesTableHTML(htmlDoc)
	if err != nil {
		return "", errors.New("Failed to parse " + url + ". Error: " + err.Error())
	}

	jsonData, err := json.Marshal(indicatorsMap)
	if err != nil {
		return "", errors.New("Failed to marshal stock data to JSON text. Error: " + err.Error())
	} else {
		collector.logger.Println("JSON text generated - " + string(jsonData))

	}
	return string(jsonData), nil
}
