package dbloader

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
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

func NewPGLoader(logger *log.Logger, dbSchema string) *PGLoader {
	// log.SetFlags(log.Ldate | log.Ltime)

	loader := PGLoader{
		db: nil, schema: dbSchema, logger: logger,
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

func (loader *PGLoader) RunQuery(sql string, structType reflect.Type, args ...any) (interface{}, error) {
	rows, err := loader.db.Query(sql)
	if err != nil {
		argsStr := fmt.Sprintf("%v", args)
		return nil, errors.New("Failed to run query [" + sql + "] with parameters " + argsStr + ". Error: " + err.Error())
	}
	defer rows.Close()

	sliceType := reflect.SliceOf(structType)
	sliceValue := reflect.MakeSlice(sliceType, 0, 0)
	for rows.Next() {
		rowStruct := reflect.New(structType).Elem()
		fields := make([]interface{}, rowStruct.NumField())
		for i := 0; i < len(fields); i++ {
			fields[i] = rowStruct.Field(i).Addr().Interface()
		}

		if err := rows.Scan(fields...); err != nil {
			return nil, errors.New("Failed to extract fields from the query result. Error: " + err.Error())
		}
		sliceValue = reflect.Append(sliceValue, rowStruct)
	}
	return sliceValue.Interface(), nil
}

func (loader *PGLoader) LoadByJsonText(jsonText string, tableName string, jsonStructType reflect.Type) (int64, error) {
	var rowsInserted int64

	loader.logger.Println("Load JSON text:", jsonText)

	if loader.schema == "" {
		loader.logger.Fatal("Schema must be created first")
	}

	converter := json2db.NewJsonToPGSQLConverter()

	// Create table
	tableCreateSQL, err := converter.GenCreateTable(jsonText, loader.schema+"."+tableName, jsonStructType)
	if err != nil {
		return rowsInserted, err
	}
	log.Println("SQL=", tableCreateSQL)

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
