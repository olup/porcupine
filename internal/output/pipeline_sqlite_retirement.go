package output

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/olup/porcupine/internal/exportstore"
)

// Preserve old credentials and checkpoints in the same transaction that replaces
// the destination. Rotation does not need the old credentials to still work.
func (s *PipelineJobs) retireS3Tx(ctx context.Context, tx *sql.Tx, p storedPipeline) error {
	var running bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pipeline_s3_runs WHERE pipeline_id=$1 AND state='running')`, p.ID).Scan(&running); err != nil {
		return err
	}
	if running {
		return apiError(409, "pipeline attempt is stopping; retry credential replacement")
	}
	var total, project, added int
	if err := tx.QueryRowContext(ctx, `SELECT count(*),count(*) FILTER(WHERE project_id=$1) FROM (SELECT project_id FROM pipeline_retired_destinations UNION ALL SELECT project_id FROM pipelines)`, p.ProjectID).Scan(&total, &project); err != nil {
		return err
	}
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM pipeline_s3_runs r WHERE pipeline_id=$1 AND state IN('queued','failed') AND (r.attempt_id<>'' OR r.manifest<>'' OR EXISTS(SELECT 1 FROM pipeline_s3_files f WHERE f.run_id=r.id))`, p.ID).Scan(&added); err != nil {
		return err
	}
	if total+added > 512 || project+added > 64 {
		return apiError(409, "retired destination cleanup limit reached")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO pipeline_retired_destinations(id,pipeline_id,project_id,config,secret,window_start_ms,manifest,attempt_id,created_at_ms) SELECT r.id,p.id,p.project_id,p.config,p.secret,r.window_start_ms,r.manifest,r.attempt_id,$1 FROM pipeline_s3_runs r JOIN pipelines p ON p.id=r.pipeline_id WHERE p.id=$2 AND r.state IN('queued','failed') AND (r.attempt_id<>'' OR r.manifest<>'' OR EXISTS(SELECT 1 FROM pipeline_s3_files f WHERE f.run_id=r.id))`, s.clock().UnixMilli(), p.ID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO pipeline_retired_files SELECT r.id,f.object_key,f.upload_id FROM pipeline_s3_files f JOIN pipeline_retired_destinations r ON r.id=f.run_id WHERE r.pipeline_id=$1`, p.ID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM pipeline_s3_files WHERE run_id IN(SELECT id FROM pipeline_retired_destinations WHERE pipeline_id=$1)`, p.ID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE pipeline_s3_state SET cursor_ms=max(cursor_ms,coalesce((SELECT max(window_end_ms) FROM pipeline_s3_runs WHERE pipeline_id=$1 AND state IN('queued','failed')),cursor_ms)) WHERE pipeline_id=$1`, p.ID); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `UPDATE pipeline_s3_runs SET state='canceled',manifest='',error_code='destination_replaced',completed_at_ms=CASE WHEN completed_at_ms=0 THEN $1 ELSE completed_at_ms END WHERE pipeline_id=$2 AND state IN('queued','failed')`, s.clock().UnixMilli(), p.ID)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `DELETE FROM pipeline_s3_runs WHERE pipeline_id=$1 AND state='canceled' AND id NOT IN(SELECT id FROM pipeline_s3_runs WHERE pipeline_id=$1 AND state='canceled' ORDER BY window_start_ms DESC,id DESC LIMIT 1000)`, p.ID)
	return err
}

func (s *PipelineJobs) cleanupRetired(ctx context.Context, project, pipeline string) (bool, error) {
	if !s.retiredMu.TryLock() {
		if project != "" {
			return false, errors.New("pipeline cleanup in progress")
		}
		return false, nil
	}
	defer s.retiredMu.Unlock()
	if !s.Enabled() {
		return false, nil
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var p storedPipeline
	var id, config, manifest, attemptID string
	var start int64
	err := s.db.QueryRowContext(ctx, `SELECT id,pipeline_id,project_id,config,secret,window_start_ms,manifest,attempt_id FROM pipeline_retired_destinations WHERE ($1='' OR project_id=$1) AND ($3='' OR pipeline_id=$3) AND ($1<>'' OR next_attempt_ms<=$2) ORDER BY next_attempt_ms,id LIMIT 1`, project, s.clock().UnixMilli(), pipeline).Scan(&id, &p.ID, &p.ProjectID, &config, &p.Secret, &start, &manifest, &attemptID)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	// Back off failed credentials without delaying current destinations.
	if _, err = s.db.ExecContext(ctx, `UPDATE pipeline_retired_destinations SET next_attempt_ms=$1 WHERE id=$2`, s.clock().Add(time.Minute).UnixMilli(), id); err != nil {
		return false, err
	}
	if project == "" {
		var finish func()
		var active bool
		ctx, finish, active = s.beginActivity(ctx, p.ProjectID)
		if !active {
			return false, nil
		}
		defer finish()
		if err = s.requireActive(ctx, p.ProjectID); err != nil {
			return false, err
		}
	}
	p.Config = json.RawMessage(config)
	var spec S3PipelineSpec
	if json.Unmarshal(p.Config, &spec) != nil {
		return false, errors.New("retired destination unavailable")
	}
	store, err := s.pipelineS3Store(ctx, p, spec)
	if err != nil {
		return false, err
	}
	defer store.Close()
	remote, err := store.ReadManifest(ctx, fmt.Sprintf("%s/%d/manifest.json", p.ID, start))
	published := err == nil
	if published && (manifest == "" || !bytes.Equal(remote, []byte(manifest))) {
		return false, errors.New("retired manifest requires review")
	}
	if err != nil && !errors.Is(err, exportstore.ErrNotFound) {
		return false, err
	}
	if !published {
		rows, err := s.db.QueryContext(ctx, `SELECT object_key,upload_id FROM pipeline_retired_files WHERE destination_id=$1`, id)
		if err != nil {
			return false, err
		}
		type file struct{ key, upload string }
		var files []file
		for rows.Next() {
			var f file
			if err = rows.Scan(&f.key, &f.upload); err != nil {
				rows.Close()
				return false, err
			}
			files = append(files, f)
		}
		if err = errors.Join(rows.Err(), rows.Close()); err != nil {
			return false, err
		}
		for _, f := range files {
			if err = retainArtifactOwnership(ctx, s.control, p.ProjectID, "pipeline_unpublished", store, f.key, f.upload); err != nil {
				return false, err
			}
			if f.upload != "" {
				err = store.Abort(ctx, f.key, f.upload)
			} else {
				err = store.AbortKeyUploads(ctx, f.key)
			}
			if err != nil {
				return false, err
			}
			if err = store.Delete(ctx, f.key); err != nil {
				return false, err
			}
		}
		if attemptID != "" {
			prefix := fmt.Sprintf("%s/%d/%s/", p.ID, start, attemptID)
			if err = retainArtifactOwnership(ctx, s.control, p.ProjectID, "pipeline_unpublished", store, prefix, ""); err != nil {
				return false, err
			}
			if err = store.AbortAttemptUploads(ctx, prefix); err != nil {
				return false, err
			}
		}
	}
	_, err = s.db.ExecContext(ctx, `DELETE FROM pipeline_retired_destinations WHERE id=$1`, id)

	return err == nil, err
}
