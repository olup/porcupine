package output

import (
	"context"
	"errors"
	"time"

	eventapi "github.com/olup/porcupine/internal/event"
	"github.com/olup/porcupine/internal/exportstore"
)

func (s *PipelineJobs) pipelineS3Admission(ctx context.Context, w *s3PipelineWork) (bool, error) {
	var barrier, pending, failed int64
	err := s.db.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM events WHERE project_id=$1 AND id<=$2 AND is_imported=0)+(SELECT count(*) FROM mutation_outbox WHERE project_id=$3 AND seq<=$4),(SELECT count(*) FROM events)+(SELECT count(*) FROM mutation_outbox),(SELECT count(*) FROM metadata WHERE key='last_delivery_error')`, w.pipeline.ProjectID, w.eventWatermark, w.pipeline.ProjectID, w.mutationWatermark).Scan(&barrier, &pending, &failed)
	return barrier == 0 && pending <= s.config.ExportLimits.QueueThreshold && failed == 0, err
}
func (s *PipelineJobs) pipelineS3CanContinue(ctx context.Context, w *s3PipelineWork) error {
	var state string
	if err := s.db.QueryRowContext(ctx, `SELECT p.state FROM pipelines p JOIN pipeline_s3_runs r ON r.pipeline_id=p.id WHERE p.id=$1 AND p.project_id=$2 AND p.secret=$3 AND r.id=$4 AND r.claim_id=$5 AND r.state='running'`, w.pipeline.ID, w.pipeline.ProjectID, w.pipeline.Secret, w.id, w.claimID).Scan(&state); err != nil || state != "active" {
		return errors.New("pipeline_paused")
	}
	allowed, err := s.AuthorizeCreator(ctx, w.pipeline.ActorID, w.pipeline.ProjectID)
	if err != nil || !allowed {
		return errors.New("creator_unauthorized")
	}
	var pending, failed int64
	if err = s.db.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM events)+(SELECT count(*) FROM mutation_outbox),(SELECT count(*) FROM metadata WHERE key='last_delivery_error')`).Scan(&pending, &failed); err != nil {
		return err
	}
	if failed > 0 || pending > s.config.ExportLimits.QueueThreshold*4 {
		return errPipelinePressure
	}
	return nil
}
func (s *PipelineJobs) runSQLiteScheduled(ctx context.Context) (bool, error) {
	w, err := s.nextPipelineS3(ctx)
	if err != nil || w == nil {
		return false, err
	}
	unlock, lockErr := s.lockS3Lifecycle(w.pipeline.ID)
	if lockErr != nil {
		return false, nil
	}
	defer unlock()
	ctx, finish, active := s.beginActivity(ctx, w.pipeline.ProjectID)
	if !active {
		return false, nil
	}
	defer finish()
	allowed, err := s.AuthorizeCreator(ctx, w.pipeline.ActorID, w.pipeline.ProjectID)
	if err != nil {
		return false, err
	}
	if !allowed {
		_, err = s.db.ExecContext(ctx, `UPDATE pipeline_s3_runs SET state='failed',error_code='creator_unauthorized',completed_at_ms=$1 WHERE id=$2`, s.clock().UnixMilli(), w.id)
		return true, err
	}
	ready, err := s.pipelineS3Admission(ctx, w)
	if err != nil || !ready {
		return false, err
	}
	result, err := s.db.ExecContext(ctx, `UPDATE pipeline_s3_runs SET state='running',attempts=attempts+1,attempt_id=CASE WHEN manifest='' THEN $1 ELSE attempt_id END,claim_id=$4,error_code='' WHERE id=$2 AND state='queued' AND EXISTS(SELECT 1 FROM pipelines WHERE id=$3 AND state='active')`, eventapi.NewID(), w.id, w.pipeline.ID, eventapi.NewID())
	if err != nil {
		return false, err
	}
	n, _ := result.RowsAffected()
	if n != 1 {
		return false, nil
	}
	if err = s.db.QueryRowContext(ctx, `SELECT attempt_id,claim_id,attempts FROM pipeline_s3_runs WHERE id=$1`, w.id).Scan(&w.attemptID, &w.claimID, &w.attempts); err != nil {
		return false, err
	}
	attemptCtx, cancel := context.WithTimeout(ctx, time.Duration(s.config.ExportLimits.MaxExecutionSeconds)*time.Second)
	s.activeS3Mu.Lock()
	s.activeS3ID, s.activeS3Cancel = w.pipeline.ID, cancel
	s.activeS3Mu.Unlock()
	err = s.pipelineS3CanContinue(attemptCtx, w)
	if err == nil {
		err = s.executePipelineS3(attemptCtx, w)
	}
	cancel()
	s.activeS3Mu.Lock()
	s.activeS3ID, s.activeS3Cancel = "", nil
	s.activeS3Mu.Unlock()

	if err != nil {
		// Publication may have succeeded despite a lost acknowledgement. Keep the
		// validated manifest and its objects for read-back recovery on the next try.
		recordCtx, recordCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer recordCancel()
		state, code := scheduledFailure(ctx, w.attempts, false)
		_, recordErr := s.db.ExecContext(recordCtx, `UPDATE pipeline_s3_runs SET state=$1,error_code=$2,next_attempt_ms=$3,completed_at_ms=CASE WHEN $4='failed' THEN $5 ELSE 0 END WHERE id=$6 AND state='running' AND claim_id=$7`, state, code, s.clock().Add(time.Duration(1<<min(w.attempts, 6))*time.Second).UnixMilli(), state, s.clock().UnixMilli(), w.id, w.claimID)
		return true, recordErr
	}
	return true, nil
}
func (s *PipelineJobs) executePipelineS3(ctx context.Context, w *s3PipelineWork) (resultErr error) {
	if s.lake == nil {
		return errors.New("analytical store unavailable")
	}
	store, err := s.pipelineS3Store(ctx, w.pipeline, w.spec)
	if err != nil {
		return err
	}
	defer store.Close()
	cleanupAllowed := false
	defer func() {
		if resultErr != nil && w.manifest == "" && cleanupAllowed {
			cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = s.cleanupPipelineS3Run(cleanup, store, w.id, w.claimID)
		}
	}()
	if w.manifest != "" {
		return s.publishPipelineS3(ctx, w, store, []byte(w.manifest))
	}
	// Read before deleting any data: an externally committed marker is never
	// silently overwritten, even after a crash or an unexpected destination edit.
	_, err = store.ReadManifest(ctx, pipelineManifestKey(w.pipeline.ID, w.start))
	if err == nil {
		return errors.New("unexpected_existing_manifest")
	}
	if err != nil && !errors.Is(err, exportstore.ErrNotFound) {
		return err
	}
	cleanupAllowed = true
	cleanup, cancel := context.WithTimeout(ctx, 30*time.Second)
	err = s.cleanupPipelineS3Run(cleanup, store, w.id, w.claimID)
	cancel()
	if err != nil {
		return err
	}
	runCtx, cancelRun := context.WithCancel(ctx)
	monitorDone := make(chan struct{})
	go func() {
		defer close(monitorDone)
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		for {
			if s.pipelineS3CanContinue(runCtx, w) != nil {
				cancelRun()
				return
			}
			select {
			case <-runCtx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	defer func() {
		cancelRun()
		<-monitorDone
	}()
	manifest, err := s.buildScheduledManifest(runCtx, w, nil, store)
	if err != nil {
		return err
	}
	if err = s.pipelineS3CanContinue(ctx, w); err != nil {
		return err
	}
	encoded, err := encodePipelineManifest(manifest)
	if err != nil {
		return errors.New("manifest_limit")
	}
	checkpoint, err := s.db.ExecContext(ctx, `UPDATE pipeline_s3_runs SET manifest=$1 WHERE id=$2 AND state='running' AND claim_id=$3`, string(encoded), w.id, w.claimID)
	if err != nil {
		return err
	}
	if n, _ := checkpoint.RowsAffected(); n != 1 {
		return errors.New("attempt_conflict")
	}
	w.manifest = string(encoded)
	return s.publishPipelineS3(ctx, w, store, encoded)
}
func (s *PipelineJobs) publishPipelineS3(ctx context.Context, w *s3PipelineWork, store *exportstore.Store, encoded []byte) error {
	manifest, ok := decodePipelineManifest(w, encoded)
	if !ok {
		return errors.New("invalid_commit_checkpoint")
	}
	if err := s.pipelineS3CanContinue(ctx, w); err != nil {
		return err
	}
	if err := commitPipelineManifest(ctx, store, pipelineManifestKey(w.pipeline.ID, w.start), encoded, errors.New("manifest_conflict")); err != nil {
		return err
	}
	if err := s.pipelineS3CanContinue(ctx, w); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	bytes := pipelineManifestBytes(manifest)
	won, err := tx.ExecContext(ctx, `UPDATE pipeline_s3_runs SET state='succeeded',error_code='',completed_at_ms=$1,manifest='',row_count=$2,byte_count=$3 WHERE id=$4 AND pipeline_id=$5 AND state='running' AND claim_id=$6 AND EXISTS(SELECT 1 FROM pipelines WHERE id=$5 AND project_id=$7 AND state='active' AND secret=$8)`, s.clock().UnixMilli(), manifest.Rows, bytes, w.id, w.pipeline.ID, w.claimID, w.pipeline.ProjectID, w.pipeline.Secret)
	if err != nil {
		return err
	}
	if n, _ := won.RowsAffected(); n != 1 {
		return errors.New("attempt_conflict")
	}
	result, err := tx.ExecContext(ctx, `UPDATE pipeline_s3_state SET cursor_ms=$1 WHERE pipeline_id=$2 AND cursor_ms=$3`, w.end, w.pipeline.ID, w.start)
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n != 1 {
		return errors.New("cursor_conflict")
	}
	// A committed manifest owns its data; cleanup checkpoints are no longer needed.
	if _, err = tx.ExecContext(ctx, `DELETE FROM pipeline_s3_files WHERE run_id=$1`, w.id); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM pipeline_s3_runs WHERE pipeline_id=$1 AND state='succeeded' AND id NOT IN (SELECT id FROM pipeline_s3_runs WHERE pipeline_id=$2 AND state='succeeded' ORDER BY window_start_ms DESC LIMIT 1000)`, w.pipeline.ID, w.pipeline.ID); err != nil {
		return err
	}
	if err = s.RecordHostedScheduledUsageTx(ctx, tx, w.pipeline.ProjectID, bytes); err != nil {
		return err
	}
	return tx.Commit()
}
