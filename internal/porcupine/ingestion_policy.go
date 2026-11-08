package porcupine

import (
	"context"
	"errors"
)

const (
	ingestionMixed      = "mixed"
	ingestionServerOnly = "server_only"
)

func (s *Service) initializeProjectIngestion(ctx context.Context) error {
	var exists int
	if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM pragma_table_info('projects') WHERE name='ingestion_mode'`).Scan(&exists); err != nil {
		return errors.New("cannot inspect project ingestion policy")
	}
	if exists == 0 {
		_, err := s.db.ExecContext(ctx, `ALTER TABLE projects ADD COLUMN ingestion_mode TEXT NOT NULL DEFAULT 'mixed' CHECK(ingestion_mode IN ('mixed','server_only'))`)
		if err != nil {
			return errors.New("cannot migrate project ingestion policy")
		}
	}
	return nil
}

func (s *Service) projectIngestionMode(ctx context.Context, projectID string) (string, error) {
	var mode string
	if err := s.db.QueryRowContext(ctx, `SELECT ingestion_mode FROM projects WHERE id=?`, projectID).Scan(&mode); err != nil {
		return "", errors.New("project ingestion policy unavailable")
	}
	if mode != ingestionMixed && mode != ingestionServerOnly {
		return "", errors.New("invalid project ingestion policy")
	}
	return mode, nil
}
