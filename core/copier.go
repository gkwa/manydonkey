package core

import (
	"database/sql"
	"fmt"
	"github.com/go-logr/logr"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
	"os"
	"text/tabwriter"
)

type Copier interface {
	CopyData(sourceDB, destDB string) error
}

type SQLiteCopier struct {
	logger        logr.Logger
	dbConnector   DBConnector
	queryExecutor QueryExecutor
}

func NewSQLiteCopier(logger logr.Logger) *SQLiteCopier {
	return &SQLiteCopier{
		logger:        logger,
		dbConnector:   NewSQLiteConnector(),
		queryExecutor: NewSQLiteQueryExecutor(),
	}
}

func (c *SQLiteCopier) CopyData(sourceDB, destDB string) error {
	c.logger.Info("Starting data copy", "from", sourceDB, "to", destDB)

	p := message.NewPrinter(language.English)

	source, err := c.dbConnector.Connect(sourceDB)
	if err != nil {
		return err
	}
	defer source.Close()

	dest, err := c.dbConnector.Connect(destDB)
	if err != nil {
		return err
	}
	defer dest.Close()

	sourceCount, err := c.getRecordCount(source)
	if err != nil {
		return err
	}

	destCountBefore, err := c.getRecordCount(dest)
	if err != nil {
		return err
	}

	sourceSize, err := c.getFileSize(sourceDB)
	if err != nil {
		return err
	}

	destSizeBefore, err := c.getFileSize(destDB)
	if err != nil {
		return err
	}

	rows, err := c.queryExecutor.Query(source, "SELECT url, title, created_at FROM links")
	if err != nil {
		return err
	}
	defer rows.Close()

	tx, err := dest.Begin()
	if err != nil {
		return err
	}

	committed := false
	defer func() {
		if !committed {
			if err := tx.Rollback(); err != nil && err != sql.ErrTxDone {
				c.logger.Error(err, "Failed to rollback transaction")
			}
		}
	}()

	stmt, err := tx.Prepare("INSERT OR IGNORE INTO links (url, title, created_at) VALUES (?, ?, ?)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	added, ignored, err := c.executeInserts(rows, stmt)
	if err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	committed = true

	destCountAfter, err := c.getRecordCount(dest)
	if err != nil {
		return err
	}

	destSizeAfter, err := c.getFileSize(destDB)
	if err != nil {
		return err
	}

	// Print the report
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "Metric\tBefore\tAfter\tChange")
	fmt.Fprintln(w, "------\t------\t-----\t------")
	fmt.Fprintf(w, "Source Count\t%s\t%s\t-\n", p.Sprintf("%d", sourceCount), p.Sprintf("%d", sourceCount))
	fmt.Fprintf(w, "Destination Count\t%s\t%s\t%s\n",
		p.Sprintf("%d", destCountBefore),
		p.Sprintf("%d", destCountAfter),
		p.Sprintf("%d", destCountAfter-destCountBefore))
	fmt.Fprintf(w, "Source Size\t%s\t%s\t-\n", formatSize(sourceSize), formatSize(sourceSize))
	fmt.Fprintf(w, "Destination Size\t%s\t%s\t%s\n",
		formatSize(destSizeBefore),
		formatSize(destSizeAfter),
		formatSize(destSizeAfter-destSizeBefore))
	fmt.Fprintf(w, "Records Processed\t-\t%s\t-\n", p.Sprintf("%d", added+ignored))
	fmt.Fprintf(w, "Records Added\t-\t%s\t-\n", p.Sprintf("%d", added))
	fmt.Fprintf(w, "Records Ignored\t-\t%s\t-\n", p.Sprintf("%d", ignored))
	w.Flush()

	return nil
}

func (c *SQLiteCopier) getRecordCount(db *sql.DB) (int, error) {
	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM links").Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get record count: %w", err)
	}
	return count, nil
}

func (c *SQLiteCopier) getFileSize(path string) (int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, fmt.Errorf("failed to get file info: %w", err)
	}
	return info.Size(), nil
}

func (c *SQLiteCopier) executeInserts(rows *sql.Rows, stmt *sql.Stmt) (added int, ignored int, err error) {
	for rows.Next() {
		var url, title, createdAt string
		if err := rows.Scan(&url, &title, &createdAt); err != nil {
			return added, ignored, fmt.Errorf("failed to scan row: %w", err)
		}

		result, err := stmt.Exec(url, title, createdAt)
		if err != nil {
			return added, ignored, fmt.Errorf("failed to insert row: %w", err)
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return added, ignored, fmt.Errorf("failed to get rows affected: %w", err)
		}

		if rowsAffected == 0 {
			ignored++
		} else {
			added++
		}
	}
	return added, ignored, nil
}

func formatSize(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
