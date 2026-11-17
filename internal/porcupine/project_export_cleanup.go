package porcupine

import (
	"context"
	"database/sql"
	"errors"
)

// stageProjectExportCleanup runs in the project-removal transaction after the
// writers drain. The checkpoints must survive deletion of export_jobs/files.
func (s *Service) stageProjectExportCleanup(ctx context.Context, tx *sql.Tx, projectID string) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO project_deletion_files(project_id,object_key,upload_id)
		SELECT j.project_id,f.object_key,f.upload_id FROM export_jobs j
		JOIN export_files f ON f.job_id=j.id WHERE j.project_id=$1
		ON CONFLICT(project_id,object_key,upload_id) DO NOTHING`, projectID)
	return err
}

func (s *Service) purgeProjectExports(ctx context.Context, projectID string) error {
	for {
		var key, uploadID string
		err := s.db.QueryRowContext(ctx, `SELECT object_key,upload_id FROM project_deletion_files
			WHERE project_id=$1 ORDER BY object_key,upload_id LIMIT 1`, projectID).Scan(&key, &uploadID)
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
			WHERE project_id=$1 AND object_key=$2 AND upload_id=$3`, projectID, key, uploadID); err != nil {
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
