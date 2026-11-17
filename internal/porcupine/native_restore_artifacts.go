package porcupine

import (
	"context"
	"database/sql"
	"errors"

	"github.com/olup/porcupine/internal/output"
)

// The reset deliberately cannot act on source destinations. Preserve only
// opaque references, and require reconciliation before tenant completion.
func preserveNativeRestoreArtifactHolds(ctx context.Context, journal *sql.DB, control *sql.Tx) error {
	type artifact struct{ project, kind, storage, key, upload string }
	var artifacts []artifact
	exports, err := output.ExportRestoreArtifacts(ctx, journal)
	if err != nil {
		return err
	}
	for _, a := range exports {
		artifacts = append(artifacts, artifact{project: a.Project, kind: "export", storage: a.Storage, key: a.Key, upload: a.Upload})
	}
	pipelines, err := output.PipelineRestoreArtifacts(ctx, journal)
	if err != nil {
		return err
	}
	for _, a := range pipelines {
		artifacts = append(artifacts, artifact{project: a.Project, kind: "pipeline", storage: a.Storage, key: a.Key, upload: a.Upload})
	}
	for _, a := range artifacts {
		var tenant string
		if err := control.QueryRowContext(ctx, `SELECT tenant_id FROM projects WHERE id=$1
UNION ALL SELECT tenant_id FROM project_deletions WHERE project_id=$1 AND tenant_id<>'' LIMIT 1`, a.project).Scan(&tenant); err != nil || tenant == "" {
			return errors.New("restored artifact tenant ownership unavailable")
		}
		if _, err := control.ExecContext(ctx, `INSERT INTO restored_artifact_holds(tenant_id,project_id,kind,storage_id,object_key,upload_id)
VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING`, tenant, a.project, a.kind, a.storage, a.key, a.upload); err != nil {
			return err
		}
		if _, err := control.ExecContext(ctx, `UPDATE tenant_deletions SET state='deleting',error_code='restored_artifacts_pending',updated_at=$2 WHERE tenant_id=$1`, tenant, now()); err != nil {
			return err
		}
	}
	return nil
}
