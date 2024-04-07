package collector

import (
	"encoding/json"
	"errors"
	"log"
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

func (collector *SACollector) TraverseHTML(node *html.Node) (map[string]string, error) {
	var indicatorsMap map[string]string
	var err error
	if node.Type == html.ElementNode {
		for _, attr := range node.Attr {
			if attr.Key == "data-test" && attr.Val == "overview-info" {
				indicatorsMap, err = collector.ConvetHTMLTableToMap(node)
				if err != nil {
					return nil, errors.New("Faield to decode html table overview-info. Error: " + err.Error())
				}

			}
			if attr.Key == "data-test" && attr.Val == "overview-quote" {
				indicatorsMap, err = collector.ConvetHTMLTableToMap(node)
				if err != nil {
					return nil, errors.New("Faield to decode html table overview-quote. Error: " + err.Error())
				}

			}
			// if attr.Key == "data-test" && attr.Val == "financials" {
			// 	collector.TraverseTable(node)
			// }
		}
	}

	for child := node.FirstChild; child != nil; child = child.NextSibling {
		var moreIndicators map[string]string
		if moreIndicators, err = collector.TraverseHTML(child); err != nil {
			return nil, err
		}
		if indicatorsMap, err = concatMaps(indicatorsMap, moreIndicators); err != nil {
			return nil, err
		}
	}

	return indicatorsMap, nil
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

func (collector *SACollector) ConvetHTMLTableToMap(node *html.Node) (map[string]string, error) {
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

// Parse Stock Analysis page and return JSON text
func (collector *SACollector) ReadStockAnalysisPage(url string, params map[string]string) (string, error) {
	htmlContent, err := ReadURL(url, params)
	if err != nil {
		return "", err
	}

	htmlDoc, err := html.Parse(strings.NewReader(htmlContent))
	if err != nil {
		return "", errors.New("Failed to parse the html page " + url + ". Error: " + err.Error())
	}

	indicatorsMap, err := collector.TraverseHTML(htmlDoc)
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
