package dbloader

import (
	"database/sql"
	"encoding/json"
	"log"
	"net/http"

	_ "github.com/lib/pq"
	"github.com/wayming/sdc/json2db"
)

type PGLoader struct {
	db *sql.DB
}

type Response struct {
	Data []json2db.JsonObject `json:"data"`
}

func NewPGLoader() *PGLoader {
	log.SetFlags(log.Ldate | log.Ltime)
	return &PGLoader{db: nil}
}

func (loader *PGLoader) Connect(host string, port string, user string, password string, dbname string) {
	var err error
	connectonString := "host=" + host + " port=" + port + " user=" + user + " password=" + password + " dbname=" + dbname
	if loader.db, err = sql.Open("postgres", connectonString); err != nil {
		log.Fatal("Failed to connect to database ", dbname, " with user ", user, ", error ", err)
	}
}

func (loader *PGLoader) Disconnect() {
	loader.db.Close()
}

func (loader PGLoader) Load(url string) int {
	resp, err := http.Get(url)
	if err != nil {
		log.Fatal("Failed to access the url, Error: ", err)
	}
	defer resp.Body.Close()

	var jsonResponse Response
	err = json.NewDecoder(resp.Body).Decode(&jsonResponse)
	if err != nil {
		log.Fatal("Failed to decode response. Error: ", err)
	}

	converter := json2db.NewJsonToPGSQLConverter()
	allObjs := converter.FlattenJsonArrayObjs(jsonResponse.Data, "sdc_tickers")
	numAllObjs := 0
	for tbl, objs := range allObjs {
		tableCreateSQL := converter.GenCreateTableSQLByObj(objs[0], tbl)
		if _, err := loader.db.Exec(tableCreateSQL); err != nil {
			log.Fatal("Failed to execute SQL ", tableCreateSQL, ". Error ", err)
		}
		numAllObjs += len(objs)
	}
	for tbl, objs := range allObjs {
		insertSQL, allRows := converter.GenBulkInsertRowsSQLByObjs(objs, tbl)

		var bindParams []interface{}
		for _, row := range allRows {
			bindParams = append(bindParams, row...)
		}

		if _, err := loader.db.Exec(insertSQL); err != nil {
			log.Fatal("Failed to execute SQL ", insertSQL, ". Bind parameters ", bindParams, ". Error ", err)
		}
	}

	return numAllObjs
}
