package output

import (
	"context"
	"database/sql"
	"errors"
	"github.com/olup/porcupine/internal/exportstore"
)

// RestoreArtifact carries opaque ownership into restore holds. Enumerating it
// grants no authority to use the source destination.
type RestoreArtifact struct{ Project, Storage, Key, Upload string }

func ExportRestoreArtifacts(ctx context.Context, journal *sql.DB) ([]RestoreArtifact, error) {
	rows, err := journal.QueryContext(ctx, `SELECT j.project_id,f.storage_id,f.object_key,f.upload_id FROM export_files f JOIN export_jobs j ON j.id=f.job_id
UNION SELECT project_id,storage_id,object_key,upload_id FROM project_deletion_files`)
	if err != nil {
		return nil, err
	}
	var artifacts []RestoreArtifact
	for rows.Next() {
		var a RestoreArtifact
		if err := rows.Scan(&a.Project, &a.Storage, &a.Key, &a.Upload); err != nil {
			rows.Close()
			return nil, err
		}
		artifacts = append(artifacts, a)
	}
	return artifacts, errors.Join(rows.Err(), rows.Close())
}

// Preserve inventory before removing the final checkpoint. DeleteObject can
// leave noncurrent object versions, so successful deletion is not purge proof.
func (s *ExportJobs) retainArtifactOwnership(ctx context.Context, project, kind string, store *exportstore.Store, key, upload string) error {
	var tenant string
	err := s.control.QueryRowContext(ctx, `SELECT tenant_id FROM projects WHERE id=$1 UNION ALL SELECT tenant_id FROM project_deletions WHERE project_id=$1 AND tenant_id<>'' LIMIT 1`, project).Scan(&tenant)
	if err != nil || tenant == "" {
		return errors.New("managed artifact ownership unavailable")
	}
	_, err = s.control.ExecContext(ctx, `INSERT INTO managed_artifact_ownership(tenant_id,project_id,kind,storage_id,object_key,upload_id) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING`, tenant, project, kind, store.OwnershipID(), key, upload)
	return err
}

// ResetRestoredExportsTx follows preservation of opaque source artifact holds.
// It clears execution authority and checkpoints without contacting that source.
func ResetRestoredExportsTx(ctx context.Context, tx *sql.Tx) error {
	for _, table := range []string{"export_files", "export_external_authority", "project_deletion_files"} {
		if table == "project_deletion_files" {
			var exists bool
			if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?)`, table).Scan(&exists); err != nil {
				return err
			}
			if !exists {
				continue
			}
		}
		if _, err := tx.ExecContext(ctx, "DELETE FROM "+table); err != nil {
			return err
		}
	}
	_, err := tx.ExecContext(ctx, `UPDATE export_jobs SET state='canceled',cancel_requested=1,attempt_id='',cleanup_pending=0,file_count=0,error='cancelled by offline credential reset'`)
	return err
}
