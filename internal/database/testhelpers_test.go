package database

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "github.com/lib/pq"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// TestDB wraps a test database connection with cleanup
type TestDB struct {
	*DB
	container testcontainers.Container
	connStr   string
}

// SetupTestDB creates a new PostgreSQL container and returns a connected DB
func SetupTestDB(t *testing.T) *TestDB {
	t.Helper()
	ctx := context.Background()

	// Start PostgreSQL container
	pgContainer, err := tcpostgres.Run(ctx,
		"postgres:15-alpine",
		tcpostgres.WithDatabase("testdb"),
		tcpostgres.WithUsername("testuser"),
		tcpostgres.WithPassword("testpass"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("failed to start postgres container: %v", err)
	}

	// Get connection string
	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("failed to get connection string: %v", err)
	}

	// Connect to database
	db, err := New(connStr)
	if err != nil {
		t.Fatalf("failed to connect to test database: %v", err)
	}

	testDB := &TestDB{
		DB:        db,
		container: pgContainer,
		connStr:   connStr,
	}

	// Run migrations
	if err := testDB.RunMigrations(); err != nil {
		testDB.Cleanup(t)
		t.Fatalf("failed to run migrations: %v", err)
	}

	return testDB
}

// RunMigrations applies all database migrations
func (tdb *TestDB) RunMigrations() error {
	driver, err := postgres.WithInstance(tdb.conn, &postgres.Config{})
	if err != nil {
		return fmt.Errorf("failed to create migration driver: %w", err)
	}

	// Get the migrations path relative to this file
	_, filename, _, _ := runtime.Caller(0)
	migrationsPath := filepath.Join(filepath.Dir(filename), "..", "..", "db", "migrations")

	m, err := migrate.NewWithDatabaseInstance(
		"file://"+migrationsPath,
		"postgres",
		driver,
	)
	if err != nil {
		return fmt.Errorf("failed to create migrate instance: %w", err)
	}

	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return fmt.Errorf("failed to run migrations: %w", err)
	}

	return nil
}

// Cleanup closes the database connection and terminates the container
func (tdb *TestDB) Cleanup(t *testing.T) {
	t.Helper()
	ctx := context.Background()

	if tdb.DB != nil {
		tdb.DB.Close()
	}

	if tdb.container != nil {
		if err := tdb.container.Terminate(ctx); err != nil {
			t.Errorf("failed to terminate container: %v", err)
		}
	}
}

// TruncateAll truncates all tables for test isolation
func (tdb *TestDB) TruncateAll(t *testing.T) {
	t.Helper()

	tables := []string{
		"alert_history",
		"alert_rules",
		"trades_history",
		"technical_indicators",
		"price_data_daily",
		"positions",
		"monitored_stocks",
		"stocks",
	}

	for _, table := range tables {
		_, err := tdb.conn.Exec(fmt.Sprintf("TRUNCATE TABLE %s CASCADE", table))
		if err != nil {
			t.Fatalf("failed to truncate table %s: %v", table, err)
		}
	}
}

// GetRawConn returns the underlying sql.DB for direct queries in tests
func (tdb *TestDB) GetRawConn() *sql.DB {
	return tdb.conn
}

// ConnectionString returns the database connection string
func (tdb *TestDB) ConnectionString() string {
	return tdb.connStr
}
