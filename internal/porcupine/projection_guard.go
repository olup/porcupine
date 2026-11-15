package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"time"
)

// Lifetime service locks exclude an offline restore. Shared projection locks
// cover native analytical writes and their SQLite acknowledgment as one checkpoint
// unit. Native backups take the same projection file's exclusive lock.
type projectionGuard struct {
	path   string
	writer *os.File
}

type projectionGuardContextKey struct{}

func openProjectionGuard(dbPath string) (*projectionGuard, error) {
	if dbPath == ":memory:" || !projectionLockSupported() {
		return nil, nil
	}
	path, err := filepath.Abs(dbPath)
	if err != nil {
		return nil, err
	}
	if resolved, resolveErr := filepath.EvalSymlinks(path); resolveErr == nil {
		path = resolved
	} else if !errors.Is(resolveErr, os.ErrNotExist) {
		return nil, resolveErr
	} else if parent, parentErr := filepath.EvalSymlinks(filepath.Dir(path)); parentErr == nil {
		path = filepath.Join(parent, filepath.Base(path))
	} else {
		return nil, parentErr
	}
	writer, err := os.OpenFile(path+".writer.lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = projectionFileLock(writer, false); err != nil {
		writer.Close()
		return nil, errors.New("SQLite has an operator restore in progress")
	}
	if _, err = os.Stat(path + ".restore-incomplete"); err == nil {
		writer.Close()
		return nil, errors.New("operator restore is incomplete")
	} else if !errors.Is(err, os.ErrNotExist) {
		writer.Close()
		return nil, err
	}
	return &projectionGuard{path: path + ".projection.lock", writer: writer}, nil
}

func (g *projectionGuard) close() error {
	if g == nil {
		return nil
	}
	return g.writer.Close()
}

func (g *projectionGuard) shared(ctx context.Context) (context.Context, func(), error) {
	if g == nil || ctx.Value(projectionGuardContextKey{}) == g {
		return ctx, func() {}, nil
	}
	release, err := lockProjectionFile(ctx, g.path, false)
	if err != nil {
		return ctx, nil, err
	}
	return context.WithValue(ctx, projectionGuardContextKey{}, g), release, nil
}

func (g *projectionGuard) exclusive(ctx context.Context) (func(), error) {
	if g == nil {
		return nil, errors.New("native checkpoints require a persistent projection guard")
	}
	return lockProjectionFile(ctx, g.path, true)
}

func lockProjectionFile(ctx context.Context, path string, exclusive bool) (func(), error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	for {
		if err := ctx.Err(); err != nil {
			file.Close()
			return nil, err
		}
		err = projectionFileLock(file, exclusive)
		if err == nil {
			return func() { file.Close() }, nil
		}
		if !projectionLockBusy(err) {
			file.Close()
			return nil, err
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}
}

func (s *Service) projectionContext(ctx context.Context) (context.Context, func(), error) {
	return s.projectionGuard.shared(ctx)
}

func (s *Service) initializeBackupGuard(ctx context.Context) error {
	var incomplete string
	err := s.db.QueryRowContext(ctx, `SELECT value FROM metadata WHERE key='online_restore_incomplete'`).Scan(&incomplete)
	if err == nil {
		return errors.New("operator restore is incomplete; finish or discard the isolated restore before starting")
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if s.projectionGuard == nil {
		return nil
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO metadata(key,value) VALUES('online_backup_guard','flock-v1'),('online_backup_lock_path',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, s.projectionGuard.path)
	return err
}

func (s *Service) initializeDelivery(ctx context.Context) error {
	return s.initializeLakeSource(ctx)
}
