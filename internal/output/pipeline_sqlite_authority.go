package output

import (
	"context"
	"database/sql"
	"errors"

	"github.com/olup/porcupine/internal/auth"
)

func (s *PipelineJobs) AuthorizeCreator(ctx context.Context, id, project string) (bool, error) {
	if s.pg != nil {
		var allowed bool
		err := s.pg.transaction(ctx, project, func(ctx context.Context, tx *sql.Tx) error {
			var tenant string
			err := tx.QueryRowContext(ctx, `SELECT tenant_id FROM projects WHERE id=$1`, project).Scan(&tenant)
			if errors.Is(err, sql.ErrNoRows) {
				return nil
			}
			if err != nil {
				return err
			}
			allowed, err = s.pg.effects.authority.ResolveCreatorTx(ctx, tx, tenant, project, id)
			return err
		})
		return allowed, err
	}

	if err := s.requireActive(ctx, project); err != nil {
		return false, err
	}
	a, err := s.loadActor(ctx, id)
	if errors.Is(err, sql.ErrNoRows) || s.credentialMissing(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return auth.CanExecutePipeline(a, project), nil
}
