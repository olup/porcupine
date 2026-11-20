package output

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/olup/porcupine/internal/apierr"
	"github.com/olup/porcupine/internal/exportstore"
)

func (w *pipelineS3Sink) beginHostedUpload(key string) (*exportstore.Upload, error) {
	err := w.s.withHostedScheduledRun(w.ctx, w.hosted, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(w.ctx, `INSERT INTO hosted_s3_pipeline_files(run_id,object_key) VALUES($1,$2)`, w.work.id, key)
		return err
	})
	if err != nil {
		return nil, err
	}
	// The key is committed before Begin, so a lost upload ID can be recovered
	// by listing multipart uploads for this exact key.
	var upload *exportstore.Upload
	err = w.s.withHostedScheduledRun(w.ctx, w.hosted, func(tx *sql.Tx) error {
		var err error
		upload, err = w.store.Begin(w.ctx, key)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(w.ctx, `UPDATE hosted_s3_pipeline_files SET upload_id=$3 WHERE run_id=$1 AND object_key=$2`, w.work.id, key, upload.ID())
		return err
	})
	return upload, err
}

func (s *PipelineJobs) cleanupHostedScheduledFiles(ctx context.Context, tx *sql.Tx, w *hostedScheduledWork, store *exportstore.Store) error {
	files, err := listScheduledFiles(ctx, tx, hostedScheduledFilesSelect, w.id)
	if err != nil {
		return err
	}
	for _, f := range files {
		items, err := exactScheduledFileArtifacts(ctx, w, store, f)
		if err != nil {
			return err
		}
		for _, item := range items {
			if err = store.DeleteExactArtifact(ctx, f.key, item.VersionID, item.UploadID); err != nil {
				return err
			}
		}
		remaining, err := exactScheduledFileArtifacts(ctx, w, store, f)
		if err != nil {
			return err
		}
		if len(remaining) != 0 {
			return errHostedOutputOwnership
		}
	}
	_, err = tx.ExecContext(ctx, `DELETE FROM hosted_s3_pipeline_files WHERE run_id=$1`, w.id)
	return err
}

const hostedScheduledArtifactColumns = `r.id,r.window_start_ms,r.attempt_id,r.destination_revision,r.secret,r.manifest,r.reserved_bytes,d.incarnation,
COALESCE(r.state='running' AND r.lease_until>clock_timestamp(),false)`
const hostedScheduledArtifactJoin = ` FROM hosted_s3_pipeline_runs r JOIN pipeline_effect_definitions d ON (d.project_id,d.id)=(r.project_id,r.pipeline_id)
WHERE r.project_id=$1 AND r.pipeline_id=$2`

func (s *PipelineJobs) retireHostedScheduledTx(ctx context.Context, tx *sql.Tx, tenant string, p storedPipeline) error {
	rows, err := tx.QueryContext(ctx, `SELECT `+hostedScheduledArtifactColumns+hostedScheduledArtifactJoin+
		` AND (r.state IN ('queued','running','failed') OR (r.state='canceled' AND r.attempt_id<>'')) ORDER BY r.window_start_ms FOR UPDATE OF r`, p.ProjectID, p.ID)
	if err != nil {
		return err
	}
	var pending []hostedScheduledWork
	for rows.Next() {
		var w hostedScheduledWork
		w.tenant, w.pipeline = tenant, p
		var live bool
		if err = scanHostedScheduledArtifacts(rows, &w, &live); err != nil {
			break
		}
		if live {
			err = apierr.New(409, "pipeline attempt is stopping; retry deletion")
			break
		}
		pending = append(pending, w)
	}
	if err = errors.Join(err, rows.Err(), rows.Close()); err != nil {
		return err
	}
	cleanup, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for _, w := range pending {
		if err := s.releaseHostedScheduledArtifactsTx(cleanup, tx, &w); err != nil {
			return err
		}
	}
	return nil
}

func scanHostedScheduledArtifacts(row rowScanner, w *hostedScheduledWork, live *bool) error {
	return row.Scan(&w.id, &w.start, &w.attemptID, &w.previousRevision, &w.previousSecret, &w.manifest, &w.reservedBytes, &w.incarnation, live)
}

func (s *PipelineJobs) releaseHostedScheduledArtifactsTx(ctx context.Context, tx *sql.Tx, w *hostedScheduledWork) error {
	if w.attemptID != "" {
		if err := json.Unmarshal(w.pipeline.Config, &w.spec); err != nil {
			return err
		}
		store, err := s.hostedScheduledStore(ctx, w, w.previousRevision, w.previousSecret)
		if err != nil {
			return err
		}
		defer store.Close()
		published, err := scheduledManifestPublished(ctx, w, store)
		if err != nil {
			return err
		}
		if published {
			var manifest pipelineS3Manifest
			if err := json.Unmarshal([]byte(w.manifest), &manifest); err != nil {
				return err
			}
			if err := s.RecordHostedScheduledUsageTx(ctx, tx, w.pipeline.ProjectID, pipelineManifestBytes(manifest)); err != nil {
				return err
			}
		} else {
			if w.manifest != "" {
				// A supported restore could replay this checkpoint. Project
				// deletion can reclaim its files after the recovery floor.
				return ErrPipelineConflict
			}
			if err = s.cleanupHostedScheduledFiles(ctx, tx, w, store); err != nil {
				return err
			}
		}

	}
	b := w.binding()
	return s.pg.effects.charge(ctx, tx, b, b.Demand, -1)
}

func (s *PipelineJobs) cancelReplacedHostedScheduleTx(ctx context.Context, tx *sql.Tx, tenant string, p storedPipeline) error {
	// A checkpointed commit still belongs to this window and namespace. Recover
	// it verbatim with current authority. Uncheckpointed attempts retire with
	// the original secret and advance the cursor just as in standalone mode.
	var id, attempt string
	var end, reserved int64
	err := tx.QueryRowContext(ctx, `
SELECT id,attempt_id,window_end_ms,reserved_bytes FROM hosted_s3_pipeline_runs
WHERE project_id=$1 AND pipeline_id=$2 AND state IN ('queued','running','failed') AND manifest='' FOR UPDATE`,
		p.ProjectID, p.ID).Scan(&id, &attempt, &end, &reserved)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE hosted_s3_pipeline_state SET cursor_ms=$3 WHERE project_id=$1 AND pipeline_id=$2`, p.ProjectID, p.ID, end); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE hosted_s3_pipeline_runs SET state='canceled',lease_until=NULL,error_code='destination_replaced',completed_at_ms=$2,next_attempt=clock_timestamp()
WHERE id=$1`, id, s.clock().UnixMilli()); err != nil {
		return err
	}
	if attempt == "" {
		w := hostedScheduledWork{tenant: tenant, reservedBytes: reserved}
		w.pipeline = p
		b := w.binding()
		if err = s.pg.effects.charge(ctx, tx, b, b.Demand, -1); err != nil {
			return err
		}
	}
	return pruneHostedScheduledHistory(ctx, tx, p.ProjectID, p.ID)
}

func pruneHostedScheduledHistory(ctx context.Context, tx *sql.Tx, project, pipeline string) error {
	_, err := tx.ExecContext(ctx, `DELETE FROM hosted_s3_pipeline_runs WHERE id IN (
SELECT id FROM hosted_s3_pipeline_runs WHERE project_id=$1 AND pipeline_id=$2 AND
 (state='succeeded' OR (state='canceled' AND attempt_id='')) ORDER BY window_start_ms DESC OFFSET 100)`, project, pipeline)
	return err
}

const hostedScheduledRetirementPresence = `SELECT EXISTS(SELECT 1 FROM hosted_s3_pipeline_runs
WHERE state='canceled' AND attempt_id<>'')`

func (s *PipelineJobs) cleanHostedScheduledRetirement(ctx context.Context) (bool, error) {
	var w hostedScheduledWork
	var config string
	err := s.db.QueryRowContext(ctx, `
SELECT r.id,r.project_id,r.pipeline_id,c.tenant_id,c.config
FROM hosted_s3_pipeline_runs r JOIN hosted_pipeline_configs c ON (c.project_id,c.id)=(r.project_id,r.pipeline_id)
WHERE r.state='canceled' AND r.attempt_id<>'' AND r.next_attempt<=clock_timestamp()
AND NOT EXISTS(SELECT 1 FROM project_deletions WHERE project_id=r.project_id)
AND NOT EXISTS(SELECT 1 FROM tenant_deletions WHERE tenant_id=c.tenant_id)
ORDER BY r.next_attempt,r.id LIMIT 1`).Scan(&w.id, &w.pipeline.ProjectID, &w.pipeline.ID, &w.tenant, &config)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	w.pipeline.Config = json.RawMessage(config)
	// Defer a failing destination without delaying a different retiring
	// destination. The project session serializes cleanup and finalization.
	result, err := s.db.ExecContext(ctx, `UPDATE hosted_s3_pipeline_runs SET next_attempt=clock_timestamp()+interval '1 minute'
WHERE id=$1 AND state='canceled' AND attempt_id<>'' AND next_attempt<=clock_timestamp()`, w.id)
	if err != nil {
		return false, err
	}
	n, err := result.RowsAffected()
	if err != nil || n != 1 {
		return false, err
	}
	cleanup, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	err = s.pg.transaction(cleanup, w.pipeline.ProjectID, func(ctx context.Context, tx *sql.Tx) error {
		if err := s.pg.effects.lockCapacity(ctx, tx, w.binding(), false); err != nil {
			return err
		}
		var live bool
		err := scanHostedScheduledArtifacts(tx.QueryRowContext(ctx, `SELECT `+hostedScheduledArtifactColumns+hostedScheduledArtifactJoin+
			` AND r.id=$3 AND r.state='canceled' AND r.attempt_id<>'' FOR UPDATE OF r`, w.pipeline.ProjectID, w.pipeline.ID, w.id), &w, &live)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if err = s.releaseHostedScheduledArtifactsTx(ctx, tx, &w); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE hosted_s3_pipeline_runs SET attempt_id='',secret=NULL WHERE id=$1`, w.id); err != nil {
			return err
		}
		return pruneHostedScheduledHistory(ctx, tx, w.pipeline.ProjectID, w.pipeline.ID)
	})
	return err == nil, err
}
