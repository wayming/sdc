package dbloader

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"reflect"

	"github.com/lib/pq"
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

func (loader *PGLoader) LoadByURL(url string, tableName string, jsonStructType reflect.Type) (int64, error) {
	var ret int64

	if loader.schema == "" {
		return ret, errors.New("schema must be created first")
	}

	resp, err := http.Get(url)
	if err != nil {
		return ret, errors.New("Failed to access the url, Error: " + err.Error())
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return ret, err
	}

	tableName = loader.schema + "." + tableName
	return loader.LoadByJsonText(string(body), tableName, jsonStructType)
}

func NVL(val interface{}, defaultVal interface{}) interface{} {
	if val == nil {
		return defaultVal
	}
	return val
}

func interfaceSliceToStringSlice(input []interface{}) []string {
	var result []string
	for _, v := range input {
		// Convert each interface{} value to string
		strVal := fmt.Sprintf("%v", NVL(v, ""))
		result = append(result, strVal)
	}
	return result
}

func (loader *PGLoader) LoadByJsonText(jsonText string, tableName string, jsonStructType reflect.Type) (int64, error) {
	var rowsInserted int64

	if loader.schema == "" {
		loader.logger.Fatal("Schema must be created first")
	}

	converter := json2db.NewJsonToPGSQLConverter()

	// Create table
	tableCreateSQL, err := converter.GenCreateTableSQLByJson2(jsonText, loader.schema+"."+tableName, jsonStructType)
	if err != nil {
		return rowsInserted, err
	}
	log.Println("SQL=", tableCreateSQL)

	// _, err = loader.db.Exec(createSQL)
	// if err != nil {
	// 	return 0, errors.New("Failed to execute SQL " + createSQL + ". Error: " + err.Error())
	// }

	// // Insert
	// fields, rows, err := converter.GenBulkInsert(jsonText, tableName, jsonStructType)
	// if err != nil {
	// 	return 0, err
	// }

	// // Generate SQL
	// sql := "COPY " + tableName + " ("
	// for idx, field := range fields {
	// 	sql += field
	// 	if idx < len(fields)-1 {
	// 		sql += ","
	// 	}
	// }
	// sql += ") FROM STDIN WITH CSV"
	// // Prepare the COPY data as CSV format
	// copyDataCSV := ""
	// for _, row := range rows {
	// 	copyDataCSV += strings.Join(interfaceSliceToStringSlice(row), ",")
	// 	copyDataCSV += "\n"
	// }

	// log.Println("SQL=", sql)
	// log.Println("CSV", copyDataCSV)

	// result, err := loader.db.Exec(sql, copyDataCSV)
	// if err != nil {
	// 	return 0, errors.New("Failed to execute SQL " + sql +
	// 		". CSV: " + copyDataCSV + ". Error: " + err.Error())
	// }
	// rowsInserted, _ = result.RowsAffected()
	// loader.logger.Println("Execute SQL: ", sql, ". Inserted rows ", rowsInserted)
	// return rowsInserted, nil

	tx, _ := loader.db.Begin()
	if _, err := tx.Exec(tableCreateSQL); err != nil {
		return 0, errors.New("Failed to execute SQL " + tableCreateSQL + ". Error: " + err.Error())
	} else {
		loader.logger.Println("Execute SQL: ", tableCreateSQL)
	}
	tx.Commit()

	// Insert
	fields, rows, err := converter.GenBulkInsert(jsonText, loader.schema+"."+tableName, jsonStructType)
	if err != nil {
		return 0, err
	}

	// Start a transaction
	tx, err = loader.db.Begin()
	if err != nil {
		return 0, errors.New("Failed to start transaction . Error: " + err.Error())
	}

	stmt, err := tx.Prepare(pq.CopyInSchema(loader.schema, tableName, fields...))
	if err != nil {
		tx.Rollback()
		return 0, errors.New("Failed to prepare CopyIn statement. Error: " + err.Error())
	}

	for _, row := range rows {
		_, err := stmt.Exec(row...)
		if err != nil {
			tx.Rollback()
			return 0, errors.New("Failed to Exec row " + ". Error: " + err.Error())
		}
	}

	// Flush
	_, err = stmt.Exec()
	if err != nil {
		log.Fatal(err)
	}

	err = stmt.Close()
	if err != nil {
		log.Fatal(err)
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		return 0, errors.New("Failed to commit CopyIn statement. Error: " + err.Error())
	}
	return int64(len(rows)), nil
}

// func (loader *PGLoader) loadJsonResponseObj(resp Response, tableName string) int {
// 	converter := json2db.NewJsonToPGSQLConverter()
// 	allObjs := converter.FlattenJsonArrayObjs(resp.Data, tableName)
// 	numAllObjs := 0
// 	for tbl, objs := range allObjs {
// 		tableCreateSQL := converter.GenCreateTableSQLByObj(objs[0], tbl)
// 		if _, err := loader.db.Exec(tableCreateSQL); err != nil {
// 			loader.logger.Fatal("Failed to execute SQL ", tableCreateSQL, ". Error ", err)
// 		} else {
// 			loader.logger.Println("Execute SQL: ", tableCreateSQL)
// 		}
// 		numAllObjs += len(objs)
// 	}
// 	for tbl, objs := range allObjs {
// 		insertSQL, allRows := converter.GenInsertSQLByJsonObjs(objs, tbl)

// 		for _, bindRow := range allRows {

// 			if _, err := loader.db.Exec(insertSQL, bindRow...); err != nil {
// 				loader.logger.Fatal("Failed to execute SQL ", insertSQL, ". Bind parameters ", bindParamsAsString(bindRow), ". Error ", err)
// 			} else {
// 				loader.logger.Println("Execute SQL: ", insertSQL)
// 			}
// 		}
// 	}

// 	return numAllObjs
// }

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
