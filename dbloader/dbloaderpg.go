package dbloader

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	_ "github.com/lib/pq"
	"github.com/wayming/sdc/json2db"
)

type PGLoader struct {
	db           *sql.DB
	schema       string
	sqlConverter json2db.JsonToSQLConverter
	logger       *log.Logger
	apiKey       string
}

type Response struct {
	Data []json2db.JsonObject `json:"data"`
}

func NewPGLoader(logger *log.Logger) *PGLoader {
	// log.SetFlags(log.Ldate | log.Ltime)

	loader := PGLoader{
		db: nil, schema: "", logger: logger,
		sqlConverter: json2db.NewJsonToPGSQLConverter()}

	return &loader
}

func (loader *PGLoader) Connect(host string, port string, user string, password string, dbname string) {
	var err error
	connectonString := "host=" + host
	connectonString += " port=" + port
	connectonString += " user=" + user
	connectonString += " password=" + password
	connectonString += " dbname=" + dbname
	connectonString += " sslmode=disable"
	if loader.db, err = sql.Open("postgres", connectonString); err != nil {
		loader.logger.Fatal("Failed to connect to database ", dbname, " with user ", user, ", error ", err)
	} else {
		loader.logger.Println("Connect to database host=", host, "port=", port, "user=", user, "dbname=", dbname)
	}
}

func (loader *PGLoader) Disconnect() {
	loader.db.Close()
}

func (loader *PGLoader) CreateSchema(schema string) {
	loader.schema = schema
	createSchemaSQL := loader.sqlConverter.GenCreateSchema(schema)
	if _, err := loader.db.Exec(createSchemaSQL); err != nil {
		loader.logger.Fatal("Failed to execute SQL ", createSchemaSQL, ". Error ", err)
	} else {
		loader.logger.Println("Execute SQL: ", createSchemaSQL)
	}
}

func (loader *PGLoader) DropSchema(schema string) {
	loader.schema = schema
	DropSchemaSQL := loader.sqlConverter.GenDropSchema(schema)
	if _, err := loader.db.Exec(DropSchemaSQL); err != nil {
		loader.logger.Fatal("Failed to execute SQL ", DropSchemaSQL, ". Error ", err)
	} else {
		loader.logger.Println("Execute SQL: ", DropSchemaSQL)
	}
}

func (loader *PGLoader) LoadByURL(url string, tableName string) int {
	if loader.schema == "" {
		loader.logger.Fatal("Schema must be created first")
	}
	resp, err := http.Get(url)
	if err != nil {
		loader.logger.Fatal("Failed to access the url, Error: ", err)
	}
	defer resp.Body.Close()

	var jsonResponse Response
	err = json.NewDecoder(resp.Body).Decode(&jsonResponse)
	if err != nil {
		loader.logger.Fatal("Failed to decode response. Error: ", err)
	}

	tableName = loader.schema + "." + tableName
	return loader.loadJsonResponseObj(jsonResponse, tableName)
}

func (loader *PGLoader) LoadByJsonResponse(JsonResponse string, tableName string) int {
	if loader.schema == "" {
		loader.logger.Fatal("Schema must be created first")
	}
	var jsonResponse Response
	err := json.Unmarshal([]byte(JsonResponse), &jsonResponse)
	if err != nil {
		loader.logger.Fatal("Failed to decode response. Error: ", err)
	}
	tableName = loader.schema + "." + tableName
	return loader.loadJsonResponseObj(jsonResponse, tableName)
}

func (loader *PGLoader) loadJsonResponseObj(resp Response, tableName string) int {
	converter := json2db.NewJsonToPGSQLConverter()
	allObjs := converter.FlattenJsonArrayObjs(resp.Data, tableName)
	numAllObjs := 0
	for tbl, objs := range allObjs {
		tableCreateSQL := converter.GenCreateTableSQLByObj(objs[0], tbl)
		if _, err := loader.db.Exec(tableCreateSQL); err != nil {
			loader.logger.Fatal("Failed to execute SQL ", tableCreateSQL, ". Error ", err)
		} else {
			loader.logger.Println("Execute SQL: ", tableCreateSQL)
		}
		numAllObjs += len(objs)
	}
	for tbl, objs := range allObjs {
		insertSQL, allRows := converter.GenInsertSQLByJsonObjs(objs, tbl)

		for _, bindRow := range allRows {

			if _, err := loader.db.Exec(insertSQL, bindRow...); err != nil {
				loader.logger.Fatal("Failed to execute SQL ", insertSQL, ". Bind parameters ", bindParamsAsString(bindRow), ". Error ", err)
			} else {
				loader.logger.Println("Execute SQL: ", insertSQL)
			}
		}
	}

	return numAllObjs
}

func bindParamsAsString(binds []interface{}) string {
	bindString := "["
	for idx, bind := range binds {
		bindString += fmt.Sprintf("%v", bind)
		if idx < len(binds)-1 {
			bindString += ", "
		}
	}
	return bindString + "]"
}
