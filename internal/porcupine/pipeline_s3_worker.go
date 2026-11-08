package porcupine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/olup/porcupine/internal/exportstore"
)

func pipelineManifestKey(w *s3PipelineWork) string {
	return fmt.Sprintf("%s/%d/manifest.json", w.pipeline.ID, w.start)
}
func (s *Service) pipelineS3Admission(ctx context.Context, w *s3PipelineWork) (bool, error) {
	var barrier, pending, failed int64
	err := s.db.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM events WHERE project_id=? AND id<=?)+(SELECT count(*) FROM mutation_outbox WHERE project_id=? AND seq<=?),(SELECT count(*) FROM events)+(SELECT count(*) FROM mutation_outbox),(SELECT count(*) FROM metadata WHERE key='last_delivery_error')`, w.pipeline.ProjectID, w.eventWatermark, w.pipeline.ProjectID, w.mutationWatermark).Scan(&barrier, &pending, &failed)
	return barrier == 0 && pending <= s.pipelines.config.ExportLimits.QueueThreshold && failed == 0, err
}
func (s *Service) pipelineS3CanContinue(ctx context.Context, w *s3PipelineWork) error {
	var state string
	if err := s.db.QueryRowContext(ctx, `SELECT state FROM pipelines WHERE id=? AND project_id=?`, w.pipeline.ID, w.pipeline.ProjectID).Scan(&state); err != nil || state != "active" {
		return errors.New("pipeline_paused")
	}
	allowed, err := pipelineCreatorAuthorized(ctx, s.db, w.pipeline.ActorID, w.pipeline.ProjectID)
	if err != nil || !allowed {
		return errors.New("creator_unauthorized")
	}
	var pending, failed int64
	if err = s.db.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM events)+(SELECT count(*) FROM mutation_outbox),(SELECT count(*) FROM metadata WHERE key='last_delivery_error')`).Scan(&pending, &failed); err != nil {
		return err
	}
	if failed > 0 || pending > s.pipelines.config.ExportLimits.QueueThreshold*4 {
		return errExportPressure
	}
	return nil
}
func (s *Service) stepPipelineS3(ctx context.Context) (bool, error) {
	if s.pipelines == nil || s.ch == nil {
		return false, nil
	}
	release, ok := s.acquirePipelineExport(ctx)
	if !ok {
		return false, nil
	}
	defer release()
	if _, err := s.db.ExecContext(ctx, `UPDATE pipeline_s3_runs SET state='queued',error_code='interrupted' WHERE state='running'`); err != nil {
		return false, err
	}
	w, err := s.nextPipelineS3(ctx)
	if err != nil || w == nil {
		return false, err
	}
	allowed, err := pipelineCreatorAuthorized(ctx, s.db, w.pipeline.ActorID, w.pipeline.ProjectID)
	if err != nil {
		return false, err
	}
	if !allowed {
		_, err = s.db.ExecContext(ctx, `UPDATE pipeline_s3_runs SET state='failed',error_code='creator_unauthorized',completed_at_ms=? WHERE id=?`, s.clock().UnixMilli(), w.id)
		return true, err
	}
	ready, err := s.pipelineS3Admission(ctx, w)
	if err != nil || !ready {
		return false, err
	}
	result, err := s.db.ExecContext(ctx, `UPDATE pipeline_s3_runs SET state='running',attempts=attempts+1,attempt_id=CASE WHEN manifest='' THEN ? ELSE attempt_id END,error_code='' WHERE id=? AND state='queued' AND EXISTS(SELECT 1 FROM pipelines WHERE id=? AND state='active')`, newID(), w.id, w.pipeline.ID)
	if err != nil {
		return false, err
	}
	n, _ := result.RowsAffected()
	if n != 1 {
		return false, nil
	}
	if err = s.db.QueryRowContext(ctx, `SELECT attempt_id,attempts FROM pipeline_s3_runs WHERE id=?`, w.id).Scan(&w.attemptID, &w.attempts); err != nil {
		return false, err
	}
	attemptCtx, cancel := context.WithTimeout(ctx, time.Duration(s.pipelines.config.ExportLimits.MaxExecutionSeconds)*time.Second)
	err = s.executePipelineS3(attemptCtx, w)
	cancel()
	if err != nil {
		// Publication may have succeeded despite a lost acknowledgement. Keep the
		// validated manifest and its objects for read-back recovery on the next try.
		recordCtx, recordCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer recordCancel()
		state := "queued"
		code := "attempt_failed"
		if w.attempts >= 3 {
			state = "failed"
			code = "attempts_exhausted"
		}
		if ctx.Err() != nil {
			code = "interrupted"
		}
		_, recordErr := s.db.ExecContext(recordCtx, `UPDATE pipeline_s3_runs SET state=?,error_code=?,next_attempt_ms=?,completed_at_ms=CASE WHEN ?='failed' THEN ? ELSE 0 END WHERE id=? AND state='running'`, state, code, s.clock().Add(time.Duration(1<<min(w.attempts, 6))*time.Second).UnixMilli(), state, s.clock().UnixMilli(), w.id)
		if recordErr != nil {
			return true, recordErr
		}
		return true, nil
	}
	return true, nil
}
func (s *Service) executePipelineS3(ctx context.Context, w *s3PipelineWork) (resultErr error) {
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
			_ = s.cleanupPipelineS3Run(cleanup, store, w.id)
		}
	}()
	if w.manifest != "" {
		return s.publishPipelineS3(ctx, w, store, []byte(w.manifest))
	}
	// Read before deleting any data: an externally committed marker is never
	// silently overwritten, even after a crash or an unexpected destination edit.
	_, err = store.ReadManifest(ctx, pipelineManifestKey(w))
	if err == nil {
		return errors.New("unexpected_existing_manifest")
	}
	if err != nil && !errors.Is(err, exportstore.ErrNotFound) {
		return err
	}
	cleanupAllowed = true
	cleanup, cancel := context.WithTimeout(ctx, 30*time.Second)
	err = s.cleanupPipelineS3Run(cleanup, store, w.id)
	cancel()
	if err != nil {
		return err
	}
	namespace, reader, password, err := s.exportReaderWithConfig(ctx, w.pipeline.ProjectID, s.pipelines.config.ExportLimits)
	if err != nil {
		return err
	}
	query, err := pipelineWindowSQL(w.spec.SQL, namespace)
	if err != nil {
		return err
	}
	queryID := "ps3_" + w.attemptID
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
		killCtx, killCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer killCancel()
		s.ch.cancelExport(killCtx, queryID)
	}()
	response, err := s.ch.openExport(runCtx, namespace, reader, password, queryID, query, pipelineWindowParameters(w.spec, w.start, w.end))
	if err != nil {
		return err
	}
	defer response.Close()
	sink := newPipelineS3Sink(runCtx, s, w, store)
	_, err = streamExportJSON(runCtx, response, s.pipelines.config.ExportLimits.MaxRowBytes, exportStreamCallbacks{Columns: sink.columns, Row: sink.row})
	closeErr := response.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	manifest, err := sink.finish()
	if err != nil {
		return err
	}
	if err = s.pipelineS3CanContinue(ctx, w); err != nil {
		return err
	}
	encoded, err := json.Marshal(manifest)
	if err != nil || len(encoded) > exportstore.MaxManifestBytes {
		return errors.New("manifest_limit")
	}
	if _, err = s.db.ExecContext(ctx, `UPDATE pipeline_s3_runs SET manifest=? WHERE id=? AND state='running'`, string(encoded), w.id); err != nil {
		return err
	}
	w.manifest = string(encoded)
	return s.publishPipelineS3(ctx, w, store, encoded)
}
func (s *Service) publishPipelineS3(ctx context.Context, w *s3PipelineWork, store *exportstore.Store, encoded []byte) error {
	var manifest pipelineS3Manifest
	if json.Unmarshal(encoded, &manifest) != nil || manifest.RunID != w.id || manifest.PipelineID != w.pipeline.ID || manifest.ProjectID != w.pipeline.ProjectID || manifest.WindowStartMS != w.start || manifest.WindowEndMS != w.end {
		return errors.New("invalid_commit_checkpoint")
	}
	if err := s.pipelineS3CanContinue(ctx, w); err != nil {
		return err
	}
	remote, err := store.ReadManifest(ctx, pipelineManifestKey(w))
	if errors.Is(err, exportstore.ErrNotFound) {
		err = store.CommitManifest(ctx, pipelineManifestKey(w), encoded)
		if err != nil && !errors.Is(err, exportstore.ErrExists) {
			return err
		}
		remote, err = store.ReadManifest(ctx, pipelineManifestKey(w))
	}
	if err != nil {
		return err
	}
	if !bytes.Equal(remote, encoded) {
		return errors.New("manifest_conflict")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE pipeline_s3_state SET cursor_ms=? WHERE pipeline_id=? AND cursor_ms=?`, w.end, w.pipeline.ID, w.start)
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n != 1 {
		return errors.New("cursor_conflict")
	}
	_, err = tx.ExecContext(ctx, `UPDATE pipeline_s3_runs SET state='succeeded',error_code='',completed_at_ms=?,manifest='',row_count=?,byte_count=? WHERE id=?`, s.clock().UnixMilli(), manifest.Rows, pipelineManifestBytes(manifest), w.id)
	if err != nil {
		return err
	}
	// A committed manifest owns its data; cleanup checkpoints are no longer needed.
	if _, err = tx.ExecContext(ctx, `DELETE FROM pipeline_s3_files WHERE run_id=?`, w.id); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM pipeline_s3_runs WHERE pipeline_id=? AND state='succeeded' AND id NOT IN (SELECT id FROM pipeline_s3_runs WHERE pipeline_id=? AND state='succeeded' ORDER BY window_start_ms DESC LIMIT 1000)`, w.pipeline.ID, w.pipeline.ID); err != nil {
		return err
	}
	return tx.Commit()
}
func pipelineManifestBytes(m pipelineS3Manifest) int64 {
	var n int64
	for _, f := range m.Files {
		n += f.Bytes
	}
	return n
}
