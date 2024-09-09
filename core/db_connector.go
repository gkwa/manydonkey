package core

import (
	"database/sql"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
)

type DBConnector interface {
	Connect(dbPath string) (*sql.DB, error)
}

type SQLiteConnector struct{}

func NewSQLiteConnector() *SQLiteConnector {
	return &SQLiteConnector{}
}

func (c *SQLiteConnector) Connect(dbPath string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	return db, nil
}
