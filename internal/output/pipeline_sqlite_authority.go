package output

import (
	"context"
	"database/sql"
	"errors"

	"github.com/olup/porcupine/internal/auth"
)

func (s *PipelineJobs) AuthorizeCreator(ctx context.Context, id, project string) (bool, error) {
	if err := s.requireActive(ctx, project); err != nil {
		return false, err
	}
	if err := s.requireOwner(ctx, project); err != nil {
		return false, err
	}
	a, err := s.loadActor(ctx, id)
	if errors.Is(err, sql.ErrNoRows) || s.credentialMissing(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return auth.Authorize(a, "pipelines:write", project) && auth.Authorize(a, "events:read", project), nil
}
