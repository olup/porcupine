package output

import (
	"context"
	"database/sql"
	"errors"
)

// StageExportProjectCleanupTx runs in the project-removal transaction after the
// writers drain. The checkpoints must survive deletion of export_jobs/files.
func StageExportProjectCleanupTx(ctx context.Context, tx *sql.Tx, projectID string) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO project_deletion_files(project_id,object_key,upload_id,storage_id)
		SELECT j.project_id,f.object_key,f.upload_id,f.storage_id FROM export_jobs j
		JOIN export_files f ON f.job_id=j.id WHERE j.project_id=$1
		ON CONFLICT(project_id,object_key,upload_id) DO NOTHING`, projectID)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM export_files WHERE job_id IN (SELECT id FROM export_jobs WHERE project_id=$1)`, projectID); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `DELETE FROM export_jobs WHERE project_id=$1`, projectID)
	return err
}

func (s *ExportJobs) CleanupProject(ctx context.Context, projectID string) error {
	for {
		var key, uploadID, storageID string
		err := s.db.QueryRowContext(ctx, `SELECT object_key,upload_id,storage_id FROM project_deletion_files
			WHERE project_id=$1 ORDER BY object_key,upload_id LIMIT 1`, projectID).Scan(&key, &uploadID, &storageID)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if s == nil || s.store == nil {
			return errExportSink
		}
		if storageID == "" || storageID != s.store.OwnershipID() {
			return errors.New("export cleanup requires its original bound storage namespace")
		}
		if err := retainArtifactOwnership(ctx, s.control, projectID, "export", s.store, key, uploadID); err != nil {
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

func (s *ExportJobs) removeExportArtifact(ctx context.Context, key, uploadID string) error {
	if s == nil || s.store == nil {
		return errExportSink
	}
	if uploadID != "" {
		if err := s.store.Abort(ctx, key, uploadID); err != nil {
			return err
		}
	} else if s.retained != nil {
		// The owned key was checkpointed before Begin could reach S3.
		if err := s.store.AbortKeyUploads(ctx, key); err != nil {
			return err
		}
	}
	// Complete may have committed despite a lost acknowledgment; always delete.
	return s.store.Delete(ctx, key)
}
