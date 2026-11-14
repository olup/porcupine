package porcupine

import (
	"context"
	"errors"
	"os"
	"path/filepath"
)

func checkNativeRestoreMarker(database string) error {
	if database == ":memory:" {
		return nil
	}
	paths := []string{database}
	if resolved, err := filepath.EvalSymlinks(database); err == nil && resolved != database {
		paths = append(paths, resolved)
	}
	for _, path := range paths {
		_, err := os.Lstat(path + ".native-restore-incomplete")
		if err == nil {
			return errors.New("native restore is incomplete; refusing to open recovered storage")
		}
		if !errors.Is(err, os.ErrNotExist) {
			return errors.New("cannot check native restore completion marker")
		}
	}
	return nil
}

func (s *Service) restoredIdentityBootstrapBlocked(ctx context.Context) (bool, error) {
	var restored bool
	err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM metadata WHERE key='native_restore_credentials_reset' AND value='1')`).Scan(&restored)
	if err != nil {
		return false, err
	}
	return restored && (s.oauth == nil || !s.oauth.config.AllowRestoredIdentityBootstrap), nil
}
