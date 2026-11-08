package porcupine

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/olup/porcupine/internal/exportstore"
)

func (s *Service) s3PipelineRunPage(ctx context.Context, project, pipeline string, limit, offset int) ([]PipelineRun, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT r.id,r.pipeline_id,r.state,r.attempts,r.created_at_ms,r.completed_at_ms,r.error_code,r.window_start_ms,r.window_end_ms,r.row_count,r.byte_count FROM pipeline_s3_runs r JOIN pipelines p ON p.id=r.pipeline_id WHERE p.project_id=? AND p.id=? ORDER BY r.window_start_ms DESC,r.id LIMIT ? OFFSET ?`, project, pipeline, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []PipelineRun{}
	for rows.Next() {
		var r PipelineRun
		if err = rows.Scan(&r.ID, &r.PipelineID, &r.State, &r.Attempts, &r.CreatedAtMS, &r.CompletedAtMS, &r.Error, &r.WindowStartMS, &r.WindowEndMS, &r.Rows, &r.Bytes); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
func (s *Service) getS3PipelineRun(ctx context.Context, project, pipeline, id string) (PipelineRun, error) {
	var r PipelineRun
	err := s.db.QueryRowContext(ctx, `SELECT r.id,r.pipeline_id,r.state,r.attempts,r.created_at_ms,r.completed_at_ms,r.error_code,r.window_start_ms,r.window_end_ms,r.row_count,r.byte_count FROM pipeline_s3_runs r JOIN pipelines p ON p.id=r.pipeline_id WHERE p.project_id=? AND p.id=? AND r.id=?`, project, pipeline, id).Scan(&r.ID, &r.PipelineID, &r.State, &r.Attempts, &r.CreatedAtMS, &r.CompletedAtMS, &r.Error, &r.WindowStartMS, &r.WindowEndMS, &r.Rows, &r.Bytes)
	return r, err
}
func (s *Service) retryS3PipelineRun(ctx context.Context, project, pipeline, id string) (bool, error) {
	result, err := s.db.ExecContext(ctx, `UPDATE pipeline_s3_runs SET state='queued',attempts=0,next_attempt_ms=0,error_code='',completed_at_ms=0 WHERE id=? AND pipeline_id=? AND state='failed' AND EXISTS(SELECT 1 FROM pipelines WHERE id=? AND project_id=?)`, id, pipeline, pipeline, project)
	if err != nil {
		return false, err
	}
	n, err := result.RowsAffected()
	if n == 1 {
		s.pipelineWake()
	}
	return n == 1, err
}
func (s *Service) pipelineS3Store(ctx context.Context, p storedPipeline, spec S3PipelineSpec) (*exportstore.Store, error) {
	if s.validatePipelineS3Endpoint(spec.Destination.Endpoint) != nil {
		return nil, errors.New("destination_disabled")
	}
	plaintext, err := s.openPipelineSecret(p.ProjectID, p.ID, p.Secret)
	if err != nil {
		return nil, errors.New("credentials_unavailable")
	}
	var secret S3PipelineSecret
	if json.Unmarshal(plaintext, &secret) != nil || validateS3PipelineSecret(secret) != nil {
		return nil, errors.New("credentials_invalid")
	}
	d := spec.Destination
	return exportstore.New(ctx, exportstore.Config{Bucket: d.Bucket, Region: d.Region, Prefix: d.Prefix, Endpoint: d.Endpoint, UsePathStyle: d.UsePathStyle, AccessKeyID: secret.AccessKeyID, SecretAccessKey: secret.SecretAccessKey, SessionToken: secret.SessionToken})
}
func (s *Service) cleanupPipelineS3Run(ctx context.Context, store *exportstore.Store, id string) error {
	rows, err := s.db.QueryContext(ctx, `SELECT object_key,upload_id FROM pipeline_s3_files WHERE run_id=?`, id)
	if err != nil {
		return err
	}
	type artifact struct{ key, upload string }
	var files []artifact
	for rows.Next() {
		var f artifact
		if err = rows.Scan(&f.key, &f.upload); err != nil {
			rows.Close()
			return err
		}
		files = append(files, f)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, f := range files {
		if f.upload != "" {
			if err = store.Abort(ctx, f.key, f.upload); err != nil {
				return err
			}
		}
		if err = store.Delete(ctx, f.key); err != nil {
			return err
		}
		if _, err = s.db.ExecContext(ctx, `DELETE FROM pipeline_s3_files WHERE run_id=? AND object_key=?`, id, f.key); err != nil {
			return err
		}
	}
	return nil
}
func (s *Service) deleteS3Pipeline(ctx context.Context, project, id string) error {
	if s.pipelines == nil {
		return apiError(503, "pipelines are not configured")
	}
	release, err := s.lockS3Lifecycle(id)
	if err != nil {
		return err
	}
	defer release()
	// Pausing and claiming both serialize in SQLite, so a new attempt cannot start
	// after this point. A running attempt observes the pause and is cleaned next.
	if _, err := s.db.ExecContext(ctx, `UPDATE pipelines SET state='paused' WHERE project_id=? AND id=?`, project, id); err != nil {
		return apiError(503, "pipeline storage unavailable")
	}
	p, err := s.loadPipeline(ctx, project, id)
	if err != nil {
		return pipelineLookupError(err)
	}
	var running bool
	if s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pipeline_s3_runs WHERE pipeline_id=? AND state='running')`, id).Scan(&running) != nil || running {
		return apiError(503, "pipeline attempt is stopping; retry deletion")
	}
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT f.run_id,r.window_start_ms,r.manifest FROM pipeline_s3_files f JOIN pipeline_s3_runs r ON r.id=f.run_id WHERE r.pipeline_id=? AND r.state!='succeeded'`, id)
	if err != nil {
		return apiError(503, "pipeline storage unavailable")
	}
	type candidate struct {
		id       string
		start    int64
		manifest string
	}
	var ids []candidate
	for rows.Next() {
		var run candidate
		if rows.Scan(&run.id, &run.start, &run.manifest) != nil {
			rows.Close()
			return apiError(503, "pipeline storage unavailable")
		}
		ids = append(ids, run)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return apiError(503, "pipeline storage unavailable")
	}
	if len(ids) > 0 {
		var spec S3PipelineSpec
		if json.Unmarshal(p.Config, &spec) != nil {
			return apiError(503, "pipeline configuration unavailable")
		}
		store, err := s.pipelineS3Store(ctx, p, spec)
		if err != nil {
			return apiError(503, "pipeline destination unavailable")
		}
		defer store.Close()
		for _, run := range ids {
			remote, readErr := store.ReadManifest(ctx, fmt.Sprintf("%s/%d/manifest.json", id, run.start))
			if readErr == nil {
				if run.manifest == "" || !bytes.Equal(remote, []byte(run.manifest)) {
					return apiError(503, "pipeline manifest requires review")
				}
				continue
			}
			if !errors.Is(readErr, exportstore.ErrNotFound) {
				return apiError(503, "pipeline commit status unavailable")
			}
			if s.cleanupPipelineS3Run(ctx, store, run.id) != nil {
				return apiError(503, "pipeline cleanup incomplete")
			}
		}
	}
	_, err = s.db.ExecContext(ctx, `DELETE FROM pipelines WHERE project_id=? AND id=? AND NOT EXISTS(SELECT 1 FROM pipeline_s3_runs WHERE pipeline_id=? AND state='running')`, project, id, id)
	if err != nil {
		return apiError(503, "pipeline storage unavailable")
	}
	return nil
}

type s3PipelineWork struct {
	pipeline                                      storedPipeline
	spec                                          S3PipelineSpec
	id, state, attemptID, manifest                string
	start, end, eventWatermark, mutationWatermark int64
	attempts                                      int
}

func (s *Service) nextPipelineS3(ctx context.Context) (*s3PipelineWork, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `UPDATE metadata SET value=value WHERE key='initialized'`); err != nil {
		return nil, err
	}
	now := s.clock().UnixMilli()
	// last_attempt_ms rotates pipelines, including blocked and retrying windows.
	rows, err := tx.QueryContext(ctx, `SELECT p.id,p.project_id,p.actor_id,p.kind,p.name,p.state,p.config,p.secret,p.created_at_ms,st.cursor_ms FROM pipelines p JOIN pipeline_s3_state st ON st.pipeline_id=p.id WHERE p.kind='s3' AND p.state='active' ORDER BY (SELECT MAX(st2.last_attempt_ms) FROM pipeline_s3_state st2 JOIN pipelines p2 ON p2.id=st2.pipeline_id WHERE p2.project_id=p.project_id),st.last_attempt_ms,p.id`)
	if err != nil {
		return nil, err
	}
	var candidates []s3PipelineWork
	for rows.Next() {
		var w s3PipelineWork
		var config string
		if err = rows.Scan(&w.pipeline.ID, &w.pipeline.ProjectID, &w.pipeline.ActorID, &w.pipeline.Kind, &w.pipeline.Name, &w.pipeline.State, &config, &w.pipeline.Secret, &w.pipeline.CreatedAtMS, &w.start); err != nil {
			rows.Close()
			return nil, err
		}
		w.pipeline.Config = json.RawMessage(config)
		if json.Unmarshal([]byte(config), &w.spec) != nil {
			rows.Close()
			return nil, errors.New("invalid stored S3 pipeline")
		}
		w.end = w.start + w.spec.IntervalSeconds*1000
		if w.end <= now {
			candidates = append(candidates, w)
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	for _, w := range candidates {
		var next int64
		err = tx.QueryRowContext(ctx, `SELECT id,state,attempts,attempt_id,event_watermark,mutation_watermark,manifest,next_attempt_ms FROM pipeline_s3_runs WHERE pipeline_id=? AND window_start_ms=?`, w.pipeline.ID, w.start).Scan(&w.id, &w.state, &w.attempts, &w.attemptID, &w.eventWatermark, &w.mutationWatermark, &w.manifest, &next)
		if errors.Is(err, sql.ErrNoRows) {
			if s.pipelineWindowHasAdmissions(w.pipeline.ProjectID, w.start, w.end) {
				continue
			}
			w.id = newID()
			w.state = "queued"
			if err = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(id),0) FROM events WHERE project_id=?`, w.pipeline.ProjectID).Scan(&w.eventWatermark); err != nil {
				return nil, err
			}
			if err = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(seq),0) FROM mutation_audit WHERE project_id=?`, w.pipeline.ProjectID).Scan(&w.mutationWatermark); err != nil {
				return nil, err
			}
			if _, err = tx.ExecContext(ctx, `INSERT INTO pipeline_s3_runs(id,pipeline_id,window_start_ms,window_end_ms,state,created_at_ms,event_watermark,mutation_watermark) VALUES(?,?,?,?,'queued',?,?,?)`, w.id, w.pipeline.ID, w.start, w.end, now, w.eventWatermark, w.mutationWatermark); err != nil {
				return nil, err
			}
		} else if err != nil {
			return nil, err
		}
		if w.state != "queued" || next > now {
			continue
		}
		if _, err = tx.ExecContext(ctx, `UPDATE pipeline_s3_state SET last_attempt_ms=max(?,(SELECT coalesce(max(last_attempt_ms),0)+1 FROM pipeline_s3_state)) WHERE pipeline_id=?`, now, w.pipeline.ID); err != nil {
			return nil, err
		}
		if err = tx.Commit(); err != nil {
			return nil, err
		}
		return &w, nil
	}
	return nil, tx.Commit()
}
