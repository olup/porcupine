package output

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/olup/porcupine/internal/exportstore"
)

// PipelineRestoreArtifacts enumerates opaque source ownership before reset.
func PipelineRestoreArtifacts(ctx context.Context, journal *sql.DB) ([]RestoreArtifact, error) {
	rows, err := journal.QueryContext(ctx, `SELECT p.project_id,p.config,f.object_key,f.upload_id FROM pipeline_s3_files f JOIN pipeline_s3_runs r ON r.id=f.run_id JOIN pipelines p ON p.id=r.pipeline_id WHERE r.state<>'succeeded'
 UNION ALL SELECT d.project_id,d.config,f.object_key,f.upload_id FROM pipeline_retired_files f JOIN pipeline_retired_destinations d ON d.id=f.destination_id
 UNION ALL SELECT project_id,config,pipeline_id||'/'||window_start_ms||'/'||attempt_id||'/', '' FROM pipeline_retired_destinations WHERE attempt_id<>''`)
	if err != nil {
		return nil, err
	}
	var artifacts []RestoreArtifact
	for rows.Next() {
		var a RestoreArtifact
		var config string
		if err = rows.Scan(&a.Project, &config, &a.Key, &a.Upload); err != nil {
			rows.Close()
			return nil, err
		}
		var spec S3PipelineSpec
		if json.Unmarshal([]byte(config), &spec) == nil && spec.Destination.Bucket != "" {
			d := spec.Destination
			a.Storage = exportstore.NamespaceOwnershipID(exportstore.Config{Endpoint: d.Endpoint, Region: d.Region, Bucket: d.Bucket, Prefix: d.Prefix})
		}
		artifacts = append(artifacts, a)
	}
	return artifacts, errors.Join(rows.Err(), rows.Close())
}
func ResetRestoredPipelinesTx(ctx context.Context, tx *sql.Tx) error {
	for _, table := range []string{"webhook_members", "webhook_deliveries", "webhook_cooldowns", "pipeline_s3_files", "pipeline_s3_runs", "pipeline_retired_files", "pipeline_retired_destinations", "webhook_queue_usage"} {
		if _, err := tx.ExecContext(ctx, "DELETE FROM "+table); err != nil {
			return err
		}
	}
	_, err := tx.ExecContext(ctx, `UPDATE pipelines SET state='paused',secret=X''`)
	return err
}

// Final metadata removal cannot discard private cleanup checkpoints.
func DeletePipelineProjectTx(ctx context.Context, tx *sql.Tx, project string) error {
	var pending bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pipeline_retired_destinations WHERE project_id=$1 UNION ALL SELECT 1 FROM pipeline_s3_files f JOIN pipeline_s3_runs r ON r.id=f.run_id JOIN pipelines p ON p.id=r.pipeline_id WHERE p.project_id=$1 UNION ALL SELECT 1 FROM pipeline_s3_runs r JOIN pipelines p ON p.id=r.pipeline_id WHERE p.project_id=$1 AND r.state='running')`, project).Scan(&pending); err != nil {
		return err
	}
	if pending {
		return errors.New("pipeline cleanup pending")
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM pipelines WHERE project_id=$1`, project); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `DELETE FROM webhook_queue_usage WHERE scope='project:'||$1`, project)
	return err
}
