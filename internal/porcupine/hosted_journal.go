package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/olup/porcupine/internal/syncx"
)

func openHostedJournal(ctx context.Context, path, source string) (_ *sql.DB, _ func(), err error) {
	path, err = filepath.Abs(path)
	if err != nil {
		return nil, nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, nil, err
	}
	filename := filepath.Base(path)
	parent, err := filepath.EvalSymlinks(filepath.Dir(path))
	if err != nil {
		return nil, nil, err
	}
	path = filepath.Join(parent, filename)
	if info, err := os.Lstat(path); err == nil && !info.Mode().IsRegular() {
		return nil, nil, errors.New("hosted journal must be a regular SQLite file")
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, nil, err
	}
	lock, err := os.OpenFile(path+".writer.lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		if err != nil {
			_ = lock.Close()
		}
	}()
	if err := syncx.FileLock(lock, true); err != nil {
		return nil, nil, fmt.Errorf("lock hosted ingestion journal: %w", err)
	}
	db, err := openLocalSQLite(path)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		if err != nil {
			_ = db.Close()
		}
	}()
	if err := migrateSQLite(ctx, db, hostedJournalSchema); err != nil {
		return nil, nil, err
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO worker_journal(singleton, source_id) VALUES (1, ?) ON CONFLICT DO NOTHING`, source); err != nil {
		return nil, nil, err
	}
	var owner string
	if err := db.QueryRowContext(ctx, `SELECT source_id FROM worker_journal WHERE singleton=1`).Scan(&owner); err != nil {
		return nil, nil, err
	}
	if owner != source {
		return nil, nil, errors.New("hosted ingestion journal belongs to a different shared source")
	}
	return db, func() { _ = lock.Close() }, nil
}
