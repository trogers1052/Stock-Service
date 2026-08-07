package database

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "github.com/lib/pq"
)

// ErrNotFound is returned when a requested row does not exist. Handlers use
// errors.Is(err, ErrNotFound) to map it to 404 instead of 500.
var ErrNotFound = errors.New("not found")

// DB wraps the database connection
type DB struct {
	conn *sql.DB
}

// New creates a new database connection
func New(connectionString string) (*DB, error) {
	conn, err := sql.Open("postgres", connectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}

	conn.SetMaxOpenConns(10)
	conn.SetMaxIdleConns(5)
	conn.SetConnMaxLifetime(5 * time.Minute)

	if err := conn.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &DB{conn: conn}, nil
}

// Close closes the database connection
func (db *DB) Close() error {
	return db.conn.Close()
}

// Conn returns the underlying database connection
func (db *DB) Conn() *sql.DB {
	return db.conn
}

// Ping checks if the database is reachable
func (db *DB) Ping() error {
	return db.conn.Ping()
}
