package porcupine

import (
	"context"
	"errors"
)

// deleteProjectArchive runs after admission is fenced and project work drains.
// Native storage owns its durable deletion fence and resumable object cleanup.
func (s *Service) deleteProjectArchive(ctx context.Context, projectID string) error {
	if s.lake == nil {
		var bound bool
		if err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM metadata WHERE key='ducklake_source_id')`).Scan(&bound); err != nil {
			return err
		}
		if bound {
			return errors.New("project cleanup requires its configured native archive")
		}
		return nil
	}
	return s.lake.DeleteProject(ctx, projectID)
}
