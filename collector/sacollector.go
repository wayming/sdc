package collector

import (
	"encoding/json"
	"errors"
	"log"
	"regexp"
	"strings"

	"github.com/wayming/sdc/dbloader"
	"golang.org/x/net/html"
)

type SACollector struct {
	dbLoader  dbloader.DBLoader
	logger    *log.Logger
	dbSchema  string
	accessKey string
}

func NewSACollector(loader dbloader.DBLoader, logger *log.Logger, schema string) *SACollector {
	collector := SACollector{
		dbLoader:  loader,
		logger:    logger,
		dbSchema:  schema,
		accessKey: "",
	}

	return &collector
}

func (collector *SACollector) DecodeDualTableHTML(node *html.Node) (map[string]string, error) {
	var indicatorsMap map[string]string
	var err error
	if node.Type == html.ElementNode {
		for _, attr := range node.Attr {
			if attr.Key == "data-test" && attr.Val == "overview-info" {
				indicatorsMap, err = collector.DecodeSimpleTable(node)
				if err != nil {
					return nil, errors.New("Faield to decode html table overview-info. Error: " + err.Error())
				}

			}
			if attr.Key == "data-test" && attr.Val == "overview-quote" {
				indicatorsMap, err = collector.DecodeSimpleTable(node)
				if err != nil {
					return nil, errors.New("Faield to decode html table overview-quote. Error: " + err.Error())
				}

			}
		}
	}

	for child := node.FirstChild; child != nil; child = child.NextSibling {
		var moreIndicators map[string]string
		if moreIndicators, err = collector.DecodeDualTableHTML(child); err != nil {
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
		return collector.FirstTextNode(child)
	}

	return nil
}

func (collector *SACollector) DecodeSimpleTable(node *html.Node) (map[string]string, error) {
	stockOverview := make(map[string]string)
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
					stockOverview[text1.Data] = text2.Data
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
					dataPoint[text1.Data] = text2.Data
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

						completeSeries[idx][text1.Data] = text2.Data
					}
					idx++
				}
			}
		}
	}
	return completeSeries, nil

}

// Parse Stock Analysis page and return JSON text
func (collector *SACollector) ReadStockAnalysisOverallPage(url string, params map[string]string) (string, error) {
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

	jsonData, err := json.Marshal(indicatorsMap)
	if err != nil {
		return "", errors.New("Failed to marshal stock data to JSON text. Error: " + err.Error())
	} else {
		collector.logger.Println("JSON text generated - " + string(jsonData))

	}
	return string(jsonData), nil
}

// Parse Stock Analysis page and return JSON text
func (collector *SACollector) ReadStockAnalysisTimeSeriesPage(url string, params map[string]string) (string, error) {
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
