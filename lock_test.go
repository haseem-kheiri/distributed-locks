package lock

import (
	"database/sql"
	"fmt"
	"log"
	"testing"
	"time"

	postgres "github.com/haseem-kheiri/postgres-tests"
	_ "github.com/lib/pq"
)

func TestPostgreIntegration(t *testing.T) {
	// 1. Setup the container using your shared helper
	// This handles the image, the wait strategy, and t.Cleanup automatically.
	connStr := postgres.SetupPostgres(t, postgres.PostgresConfig{
		DB:       "lockdb",
		User:     "postgres",
		Password: "password",
	})

	// 2. Use the connection string for your actual library logic
	t.Logf("Successfully started Postgres for locking tests at: %s", connStr)

	db, err := sql.Open("postgres", connStr)

	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	var currentDate time.Time
	err = db.QueryRow("Select current_date").Scan(&currentDate)
	if err != nil {
		log.Fatalf("Error running query %s\n", err)
	}

	fmt.Printf("Current Date from PostgresSQL: %s\n", currentDate)
}
