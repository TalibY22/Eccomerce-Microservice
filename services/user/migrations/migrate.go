package migrations

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strings"
	"time"
)

//go:embed *.sql
var files embed.FS

func Apply(ctx context.Context, db *sql.DB) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	lockName := "user_service_migrations"
	var locked int
	if err := db.QueryRowContext(ctx, "SELECT GET_LOCK(?, 30)", lockName).Scan(&locked); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	if locked != 1 {
		return fmt.Errorf("could not acquire migration lock %q", lockName)
	}
	defer db.ExecContext(context.Background(), "SELECT RELEASE_LOCK(?)", lockName)

	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version VARCHAR(255) PRIMARY KEY,
		checksum CHAR(64) NOT NULL,
		statement_index INT UNSIGNED NOT NULL DEFAULT 0,
		complete BOOLEAN NOT NULL DEFAULT FALSE,
		started_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
		completed_at TIMESTAMP(6) NULL
	) ENGINE=InnoDB`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	entries, err := fs.Glob(files, "*.sql")
	if err != nil {
		return err
	}
	sort.Strings(entries)
	for _, name := range entries {
		body, err := files.ReadFile(name)
		if err != nil {
			return err
		}
		checksum := fmt.Sprintf("%x", sha256.Sum256(body))
		var appliedChecksum string
		var statementIndex int
		var complete bool
		err = db.QueryRowContext(ctx, "SELECT checksum, statement_index, complete FROM schema_migrations WHERE version = ?", name).Scan(&appliedChecksum, &statementIndex, &complete)
		if err == sql.ErrNoRows {
			if _, err := db.ExecContext(ctx, "INSERT INTO schema_migrations (version, checksum) VALUES (?, ?)", name, checksum); err != nil {
				return fmt.Errorf("start migration %s: %w", name, err)
			}
			statementIndex = 0
		} else if err != nil {
			return fmt.Errorf("check migration %s: %w", name, err)
		} else if appliedChecksum != checksum {
			return fmt.Errorf("applied migration %s was modified", name)
		}
		if complete {
			continue
		}
		statements := strings.Split(string(body), ";")
		for i, statement := range statements {
			statement = strings.TrimSpace(statement)
			if statement == "" || i < statementIndex {
				continue
			}
			if _, err := db.ExecContext(ctx, statement); err != nil {
				return fmt.Errorf("run migration %s statement %d: %w", name, i+1, err)
			}
			if _, err := db.ExecContext(ctx, "UPDATE schema_migrations SET statement_index = ? WHERE version = ?", i+1, name); err != nil {
				return fmt.Errorf("record migration %s statement %d: %w", name, i+1, err)
			}
		}
		if _, err := db.ExecContext(ctx, "UPDATE schema_migrations SET complete = TRUE, completed_at = CURRENT_TIMESTAMP(6) WHERE version = ?", name); err != nil {
			return fmt.Errorf("complete migration %s: %w", name, err)
		}
	}
	return nil
}
