package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"os"
	"time"

	"modernc.org/sqlite"
)

// snapshotSQLite uses a dedicated source connection when concurrent acceptance
// must continue. The caller owns the private destination directory.
func snapshotSQLite(ctx context.Context, source *sql.DB, destination string) error {
	file, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	conn, err := source.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	err = conn.Raw(func(raw any) (result error) {
		backup, err := raw.(interface {
			NewBackup(string) (*sqlite.Backup, error)
		}).NewBackup((&url.URL{Scheme: "file", Path: destination}).String())
		if err != nil {
			return err
		}
		defer func() { result = errors.Join(result, backup.Finish()) }()
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			more, err := backup.Step(256)
			if err == nil && !more {
				return nil
			}
			if err != nil {
				var native *sqlite.Error
				if !errors.As(err, &native) || (native.Code()&255 != 5 && native.Code()&255 != 6) {
					return err
				}
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Millisecond):
			}
		}
	})
	if err != nil {
		return err
	}
	// Publish a self-contained rollback-journal database. Read-only validation
	// of a WAL-mode snapshot can otherwise create untracked -wal/-shm files.
	target, err := sql.Open("sqlite", (&url.URL{Scheme: "file", Path: destination}).String())
	if err != nil {
		return err
	}
	defer target.Close()
	var mode string
	if err := target.QueryRowContext(ctx, "PRAGMA journal_mode=DELETE").Scan(&mode); err != nil {
		return err
	}
	if mode != "delete" {
		return errors.New("cannot make SQLite snapshot self-contained")
	}
	return nil
}
