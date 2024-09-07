package dbloader

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strings"

	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"github.com/wayming/sdc/json2db"
)

type PGLoader struct {
	db           *sql.DB
	schema       string
	sqlConverter json2db.JsonToSQLConverter
	logger       *log.Logger
}

func NewPGLoader(dbSchema string, logger *log.Logger) *PGLoader {
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

	loader.CreateSchema(loader.schema)
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

	loader.Exec("SET search_path TO " + schema)
}

func (loader *PGLoader) DropSchema(schema string) error {
	loader.schema = schema
	DropSchemaSQL := loader.sqlConverter.GenDropSchema(schema)
	_, err := loader.db.Exec(DropSchemaSQL)
	if err != nil {
		loader.logger.Printf("Failed to execute SQL %s. Error %v", DropSchemaSQL, err)
	}
	loader.logger.Println("Execute SQL: ", DropSchemaSQL)
	return err
}

func ExistsInSlice(s []string, e string) bool {
	for _, one := range s {
		if e == one {
			return true
		}
	}
	return false
}

func (loader *PGLoader) Exec(sql string) error {
	if _, err := loader.db.Exec(sql); err != nil {
		loader.logger.Fatal("Failed to execute SQL ", sql, ". Error ", err)
		return err
	} else {
		loader.logger.Println("Execute SQL: ", sql)
	}
	return nil
}

func (loader *PGLoader) RunQuery(sql string, structType reflect.Type, args ...any) (interface{}, error) {
	stmt, err := loader.db.Prepare(sql)
	if err != nil {
		return nil, errors.New("Failed to run prepare [" + sql + "]. Error: " + err.Error())
	}
	defer stmt.Close()

	sliceType := reflect.SliceOf(structType)
	sliceValue := reflect.MakeSlice(sliceType, 0, 0)

	if args != nil {
		rows, err := stmt.Query(args...)
		if err != nil {
			argsStr := fmt.Sprintf("%v", args)
			return nil, errors.New("Failed to run bind parameters " + argsStr + ". Error: " + err.Error())
		}

		columns, _ := rows.Columns()

		for rows.Next() {
			rowValue := reflect.New(structType).Elem()
			fields := make([]interface{}, 0)
			for i := 0; i < structType.NumField(); i++ {
				if ExistsInSlice(columns, strings.ToLower(structType.Field(i).Name)) {
					fields = append(fields, rowValue.Field(i).Addr().Interface())
				}
			}

			if err := rows.Scan(fields...); err != nil {
				return nil, errors.New("Failed to extract fields from the query result. Error: " + err.Error())
			}
			sliceValue = reflect.Append(sliceValue, rowValue)
		}
	} else {
		rows, err := stmt.Query()
		if err != nil {
			return nil, errors.New("Failed to run without bind parameter. Error: " + err.Error())
		}

		columns, _ := rows.Columns()

		for rows.Next() {
			rowValue := reflect.New(structType).Elem()
			fields := make([]interface{}, 0)
			for i := 0; i < structType.NumField(); i++ {
				if ExistsInSlice(columns, strings.ToLower(structType.Field(i).Name)) {
					fields = append(fields, rowValue.Field(i).Addr().Interface())
				}
			}

			if err := rows.Scan(fields...); err != nil {
				return nil, errors.New("Failed to extract fields from the query result. Error: " + err.Error())
			}
			sliceValue = reflect.Append(sliceValue, rowValue)
		}
	}

	return sliceValue.Interface(), nil
}

func joinInterfaceSlice(slice []interface{}, sep string) string {
	// Convert each element to string and append to a slice of strings
	var strSlice []string
	for _, v := range slice {
		strSlice = append(strSlice, fmt.Sprintf("%v", v))
	}

	// Join the slice of strings with the separator
	return strings.Join(strSlice, sep)
}

func (loader *PGLoader) CreateTableByJsonStruct(tableName string, jsonStructType reflect.Type) error {
	converter := json2db.NewJsonToPGSQLConverter()

	// Create table
	tableCreateSQL, err := converter.GenCreateTable(tableName, jsonStructType)
	if err != nil {
		return err
	}
	loader.logger.Println("SQL=", tableCreateSQL)

	tx, _ := loader.db.Begin()
	if _, err := tx.Exec(tableCreateSQL); err != nil {
		tx.Rollback()
		return errors.New("Failed to execute SQL " + tableCreateSQL + ". Error: " + err.Error())
	} else {
		loader.logger.Println("Execute SQL: ", tableCreateSQL)
	}
	tx.Commit()
	return nil
}

func (loader *PGLoader) LoadByJsonText(jsonText string, tableName string, jsonStructType reflect.Type) (int64, error) {
	loader.logger.Println("Load JSON text:", jsonText)

	if loader.schema == "" {
		loader.logger.Fatal("Schema must be created first")
	}

	// Query to get the current search_path
	var searchPath string
	err := loader.db.QueryRow("SHOW search_path").Scan(&searchPath)
	if err != nil {
		loader.logger.Fatalf("database QueryRow failed: %v", err)
	}
	loader.logger.Printf("Current search_path: %s, loader database schema: %s\n", searchPath, loader.schema)

	converter := json2db.NewJsonToPGSQLConverter()

	// Insert
	fields, rows, err := converter.ExtractSQLData(jsonText, tableName, jsonStructType)
	if err != nil || len(rows) == 0 {
		loader.logger.Println("Failed to generate bulk insert SQL. Error: " + err.Error())
		return 0, err
	}

	// Start a transaction
	tx, err := loader.db.Begin()
	if err != nil {
		tx.Rollback()
		return 0, errors.New("Failed to start transaction . Error: " + err.Error())
	}

	// conflictResolution := " ON CONFLICT DO UPDATE SET "
	// for idx, field := range fields {
	// 	conflictResolution += field + " = EXCLUDED." + field
	// 	if idx < len(fields)-1 {
	// 		conflictResolution += ","
	// 	}
	// }

	prepareSQL := pq.CopyIn(tableName, fields...)
	stmt, err := tx.Prepare(prepareSQL)
	if err != nil {
		tx.Rollback()
		return 0, fmt.Errorf("failed to prepare CopyIn statement. SQL: %s Error: %s", prepareSQL, err.Error())
	}

	for _, row := range rows {
		loader.logger.Printf("Execute INSERT: fields[%s], row[%s]", strings.Join(fields, ","), joinInterfaceSlice(row, ","))
		_, err := stmt.Exec(row...)
		if err != nil {
			tx.Rollback()
			return 0, errors.New("Failed to Exec row " + ". Error: " + err.Error())
		}
	}

	// Flush
	_, err = stmt.Exec()
	if err != nil {
		errMsg := fmt.Sprintf("failed to execute CopyIn statement. Error: %s", err.Error())
		loader.logger.Printf(errMsg)
		tx.Rollback()
		return 0, errors.New(errMsg)

	}

	err = stmt.Close()
	if err != nil {
		loader.logger.Printf("Close error %s\n", err.Error())
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		errMsg := fmt.Sprintf("failed to commit CopyIn statement. Error: %s", err.Error())
		loader.logger.Printf(errMsg)
		return 0, errors.New(errMsg)
	}
	return int64(len(rows)), nil
}
