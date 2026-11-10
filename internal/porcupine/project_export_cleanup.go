package porcupine

import (
	"context"
	"database/sql"
	"errors"
)

// stageProjectExportCleanup runs in the project-removal transaction after the
// writers drain. The checkpoints must survive deletion of export_jobs/files.
func (s *Service) stageProjectExportCleanup(ctx context.Context, tx *sql.Tx, projectID string) error {
	_, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS project_deletion_files (
		project_id TEXT NOT NULL, object_key TEXT NOT NULL, upload_id TEXT NOT NULL,
		PRIMARY KEY(project_id, object_key, upload_id)) WITHOUT ROWID`)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO project_deletion_files(project_id,object_key,upload_id)
		SELECT j.project_id,f.object_key,f.upload_id FROM export_jobs j
		JOIN export_files f ON f.job_id=j.id WHERE j.project_id=?
		ON CONFLICT(project_id,object_key,upload_id) DO NOTHING`, projectID)
	return err
}

func (s *Service) purgeProjectExports(ctx context.Context, projectID string) error {
	for {
		var key, uploadID string
		err := s.db.QueryRowContext(ctx, `SELECT object_key,upload_id FROM project_deletion_files
			WHERE project_id=? ORDER BY object_key,upload_id LIMIT 1`, projectID).Scan(&key, &uploadID)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if err = s.removeExportArtifact(ctx, key, uploadID); err != nil {
			return err
		}
		if _, err = s.db.ExecContext(ctx, `DELETE FROM project_deletion_files
			WHERE project_id=? AND object_key=? AND upload_id=?`, projectID, key, uploadID); err != nil {
			return err
		}
	}
}

func (s *Service) removeExportArtifact(ctx context.Context, key, uploadID string) error {
	if s.exports == nil || s.exports.store == nil {
		return errExportSink
	}
	if uploadID != "" {
		if err := s.exports.store.Abort(ctx, key, uploadID); err != nil {
			return err
		}
	}
	// Complete may have committed despite a lost acknowledgment; always delete.
	return s.exports.store.Delete(ctx, key)
}
