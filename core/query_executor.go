package core

import (
	"database/sql"
	"fmt"
)

type QueryExecutor interface {
	Query(db *sql.DB, query string) (*sql.Rows, error)
	ExecuteInserts(rows *sql.Rows, stmt *sql.Stmt) (int, error)
}

type SQLiteQueryExecutor struct{}

func NewSQLiteQueryExecutor() *SQLiteQueryExecutor {
	return &SQLiteQueryExecutor{}
}

func (e *SQLiteQueryExecutor) Query(db *sql.DB, query string) (*sql.Rows, error) {
	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	return rows, nil
}

func (e *SQLiteQueryExecutor) ExecuteInserts(rows *sql.Rows, stmt *sql.Stmt) (int, error) {
	var count int
	for rows.Next() {
		var url, title, createdAt string
		if err := rows.Scan(&url, &title, &createdAt); err != nil {
			return 0, fmt.Errorf("failed to scan row: %w", err)
		}

		_, err := stmt.Exec(url, title, createdAt)
		if err != nil {
			return 0, fmt.Errorf("failed to insert row: %w", err)
		}
		count++
	}
	return count, nil
}
