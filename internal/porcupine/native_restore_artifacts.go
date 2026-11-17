package porcupine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/olup/porcupine/internal/exportstore"
)

// The reset deliberately cannot act on source destinations. Preserve only
// opaque references, and require reconciliation before tenant completion.
func preserveNativeRestoreArtifactHolds(ctx context.Context, journal *sql.DB, control *sql.Tx) error {
	type artifact struct{ project, kind, storage, key, upload string }
	var artifacts []artifact
	rows, err := journal.QueryContext(ctx, `SELECT j.project_id,f.storage_id,f.object_key,f.upload_id FROM export_files f JOIN export_jobs j ON j.id=f.job_id
UNION SELECT project_id,storage_id,object_key,upload_id FROM project_deletion_files`)
	if err != nil {
		return err
	}
	for rows.Next() {
		a := artifact{kind: "export"}
		if err := rows.Scan(&a.project, &a.storage, &a.key, &a.upload); err != nil {
			rows.Close()
			return err
		}
		artifacts = append(artifacts, a)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return err
	}
	rows, err = journal.QueryContext(ctx, `SELECT p.project_id,p.config,f.object_key,f.upload_id FROM pipeline_s3_files f
JOIN pipeline_s3_runs r ON r.id=f.run_id JOIN pipelines p ON p.id=r.pipeline_id WHERE r.state<>'succeeded'`)
	if err != nil {
		return err
	}
	for rows.Next() {
		a := artifact{kind: "pipeline"}
		var config string
		if err := rows.Scan(&a.project, &config, &a.key, &a.upload); err != nil {
			rows.Close()
			return err
		}
		var spec S3PipelineSpec
		if json.Unmarshal([]byte(config), &spec) == nil && spec.Destination.Bucket != "" {
			d := spec.Destination
			a.storage = exportstore.NamespaceOwnershipID(exportstore.Config{Endpoint: d.Endpoint, Region: d.Region, Bucket: d.Bucket, Prefix: d.Prefix})
		}
		artifacts = append(artifacts, a)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return err
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
