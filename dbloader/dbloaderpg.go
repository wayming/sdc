package dbloader

import (
	"database/sql"
	"encoding/json"
	"log"
	"net/http"

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

func (loader *PGLoader) Connect(user string, password string, dbname string) {
	var err error
	connectonString := "user=" + user + " password=" + password + " dbname=" + dbname
	if loader.db, err = sql.Open("postgres", connectonString); err != nil {
		log.Fatal("Failed to connect to database ", dbname, " with user ", user)
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
	for tbl, objs := range allObjs {
		log.Println(converter.GenCreateTableSQLByObj(objs[0], tbl))
	}
	for tbl, objs := range allObjs {
		sql, bindVars := converter.GenBulkInsertRowsSQLByObjs(objs, tbl)
		log.Print(sql, " Bind variables ", bindVars)
	}

	return len(allObjs)
}
