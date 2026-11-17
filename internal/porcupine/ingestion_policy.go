package porcupine

import (
	"context"
	"errors"
)

const (
	ingestionMixed      = "mixed"
	ingestionServerOnly = "server_only"
)

func (s *Service) projectIngestionMode(ctx context.Context, projectID string) (string, error) {
	var mode string
	if err := s.control.QueryRowContext(ctx, `SELECT ingestion_mode FROM projects WHERE id=$1`, projectID).Scan(&mode); err != nil {
		return "", errors.New("project ingestion policy unavailable")
	}
	if mode != ingestionMixed && mode != ingestionServerOnly {
		return "", errors.New("invalid project ingestion policy")
	}
	return mode, nil
}
