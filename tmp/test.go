package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	_ "github.com/lib/pq"
)

func main() {
    // Connection parameters
    connStr := "user=" + os.Getenv("PGUSER")" dbname=mydb sslmode=disable"
    db, err := sql.Open("postgres", connStr)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Create temporary table for bulk insert
    _, err = db.Exec(`
        CREATE TEMP TABLE temp_employees (
            employee_id INT PRIMARY KEY,
            name TEXT,
            position TEXT,
            salary INT
        )
    `)
    if err != nil {
        log.Fatal(err)
    }

    // Prepare COPY command
    stmt, err := db.Prepare(pq.CopyIn("temp_employees", "employee_id", "name", "position", "salary"))
    if err != nil {
        log.Fatal(err)
    }
    defer stmt.Close()

    // Bulk insert data into the temporary table
    _, err = stmt.Exec(
        1, "John Doe", "Software Engineer", 70000,
        2, "Jane Smith", "Data Scientist", 75000,
        3, "Emily Davis", "Product Manager", 80000,
    )
    if err != nil {
        log.Fatal(err)
    }

    // End COPY operation
    _, err = stmt.Exec()
    if err != nil {
        log.Fatal(err)
    }

    // Upsert from temporary table into the main table
    _, err = db.Exec(`
        INSERT INTO employees (employee_id, name, position, salary)
        SELECT employee_id, name, position, salary
        FROM temp_employees
        ON CONFLICT (employee_id)
        DO UPDATE SET
            name = EXCLUDED.name,
            position = EXCLUDED.position,
            salary = EXCLUDED.salary
    `)
    if err != nil {
        log.Fatal(err)
    }

    fmt.Println("Bulk insert and upsert operation completed successfully")
}

