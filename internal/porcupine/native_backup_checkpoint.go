package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"path/filepath"
)

// The caller holds the projection guard. Freezing control writes keeps project
// lifecycle and access metadata stable while the journal and catalogs are copied.
func (s *Service) stageNativeCheckpoint(ctx context.Context) (checkpoint *nativeCheckpoint, result error) {
	defer func() {
		if result != nil && checkpoint != nil {
			result = errors.Join(result, checkpoint.Close())
			checkpoint = nil
		}
	}()
	db, err := sql.Open("sqlite", (&url.URL{Scheme: "file", Path: s.backups.controlDatabasePath}).String()+"?_pragma=busy_timeout(100)")
	if err != nil {
		return nil, err
	}
	defer func() { result = errors.Join(result, db.Close()) }()
	control, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := control.Rollback(); !errors.Is(err, sql.ErrTxDone) {
			result = errors.Join(result, err)
		}
	}()
	if _, err := control.ExecContext(ctx, `UPDATE metadata SET value=value WHERE key='initialized'`); err != nil {
		return nil, err
	}
	var capturedAt string
	checkpoint, result = s.lake.stageCheckpoint(ctx, func(ctx context.Context, destination string) error {
		for _, item := range []struct{ source, destination string }{
			{s.backups.databasePath, destination},
			{s.backups.controlDatabasePath, filepath.Join(filepath.Dir(destination), "control.sqlite")},
		} {
			source, err := sql.Open("sqlite", (&url.URL{Scheme: "file", Path: item.source}).String()+"?mode=ro&_pragma=busy_timeout(100)")
			if err != nil {
				return err
			}
			err = errors.Join(snapshotSQLite(ctx, source, item.destination), source.Close())
			if err != nil {
				return err
			}
		}
		capturedAt = now()
		return nil
	})
	if result == nil {
		checkpoint.CapturedAt = capturedAt
	}
	return checkpoint, result
}
