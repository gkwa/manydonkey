package core

import (
	"database/sql"
	"fmt"

	"github.com/go-logr/logr"
	_ "github.com/mattn/go-sqlite3"
)

func Hello(logger logr.Logger) {
	logger.V(1).Info("Debug: Entering Hello function")
	logger.Info("Hello, World!")
	logger.V(1).Info("Debug: Exiting Hello function")
}

func TransferData(sourceDB, destDB string, logger logr.Logger) error {
	logger.Info("Starting data transfer", "from", sourceDB, "to", destDB)

	source, err := sql.Open("sqlite3", sourceDB)
	if err != nil {
		return fmt.Errorf("failed to open source database: %w", err)
	}
	defer source.Close()

	dest, err := sql.Open("sqlite3", destDB)
	if err != nil {
		return fmt.Errorf("failed to open destination database: %w", err)
	}
	defer dest.Close()

	rows, err := source.Query("SELECT url, title, created_at FROM links")
	if err != nil {
		return fmt.Errorf("failed to query source database: %w", err)
	}
	defer rows.Close()

	tx, err := dest.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && err != sql.ErrTxDone {
			logger.Error(err, "Failed to rollback transaction")
		}
	}()

	stmt, err := tx.Prepare("INSERT OR IGNORE INTO links (url, title, created_at) VALUES (?, ?, ?)")
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	var count int
	for rows.Next() {
		var url, title, createdAt string
		if err := rows.Scan(&url, &title, &createdAt); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		_, err := stmt.Exec(url, title, createdAt)
		if err != nil {
			return fmt.Errorf("failed to insert row: %w", err)
		}
		count++
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	logger.Info("Data transfer completed", "records_transferred", count)
	return nil
}
