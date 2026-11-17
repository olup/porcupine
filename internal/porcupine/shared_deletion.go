package porcupine

import (
	"context"
	"database/sql"
	"errors"
)

// The project-row lock is also taken by shared query admission. Once this fence
// commits, no worker can start another project query or owner operation.
func (s *Service) beginControlProjectDeletion(ctx context.Context, id string) error {
	tx, err := s.control.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var project, tenant string
	lock := ""
	if s.shared != nil {
		lock = " FOR UPDATE"
	}
	err = tx.QueryRowContext(ctx, `SELECT id,tenant_id FROM projects WHERE id=$1`+lock, id).Scan(&project, &tenant)
	if errors.Is(err, sql.ErrNoRows) {
		var completed bool
		if err := tx.QueryRowContext(ctx, `SELECT completed FROM project_deletions WHERE project_id=$1`, id).Scan(&completed); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return apiError(404, "project not found")
			}
			return err
		}
		return tx.Commit()
	}
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO project_deletions(project_id,tenant_id,completed) VALUES($1,$2,0)
ON CONFLICT(project_id) DO UPDATE SET tenant_id=excluded.tenant_id WHERE project_deletions.tenant_id=''`, id, tenant); err != nil {
		return err
	}
	var recordedTenant string
	if err := tx.QueryRowContext(ctx, `SELECT tenant_id FROM project_deletions WHERE project_id=$1`, id).Scan(&recordedTenant); err != nil {
		return err
	}
	if recordedTenant != tenant {
		return apiError(409, "project deletion tenant ownership is inconsistent")
	}
	if err := purgeProjectGrants(ctx, tx, id); err != nil {
		return err
	}
	return tx.Commit()
}

// Finalization removes active ownership only after the owner has drained and
// cleaned its payloads and external objects. The deletion tombstone is permanent.
func (s *Service) completeControlProjectDeletion(ctx context.Context, id string) error {
	tx, err := s.control.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var project string
	lock := ""
	if s.shared != nil {
		lock = " FOR UPDATE"
	}
	err = tx.QueryRowContext(ctx, `SELECT id FROM projects WHERE id=$1`+lock, id).Scan(&project)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	var completed bool
	if err := tx.QueryRowContext(ctx, `SELECT completed FROM project_deletions WHERE project_id=$1`+lock, id).Scan(&completed); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return apiError(409, "project deletion has not started")
		}
		return err
	}
	if completed {
		return tx.Commit()
	}
	if err := purgeProjectGrants(ctx, tx, id); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM projects WHERE id=$1`, id); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE project_deletions SET completed=1 WHERE project_id=$1`, id); err != nil {
		return err
	}
	return tx.Commit()
}
