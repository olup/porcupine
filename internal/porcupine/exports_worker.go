package porcupine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/olup/porcupine/internal/auth"
	eventapi "github.com/olup/porcupine/internal/event"
	lakeapi "github.com/olup/porcupine/internal/lake"
)

var (
	errExportPressure             = errors.New("export delivery pressure")
	errExportRevoked              = errors.New("export creator revoked")
	errExportCanceled             = errors.New("export canceled")
	errExportAuthorizationExpired = errors.New("external export authorization expired")
)

func (s *Service) startExports() {
	if s.exports == nil || s.exports.store == nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.exports.cancel = cancel
	s.exports.done = make(chan struct{})
	if s.exports.wake == nil {
		s.exports.wake = make(chan struct{}, 1)
	}
	go func() { defer close(s.exports.done); s.exportsLoop(ctx) }()
}

func (s *Service) stopExports() {
	if s.exports == nil {
		return
	}
	if s.exports.cancel != nil {
		s.exports.cancel()
	}
	if s.exports.done != nil {
		<-s.exports.done
	}
}

func (s *Service) exportsLoop(ctx context.Context) {
	recovered := false
	for ctx.Err() == nil {
		if !recovered {
			_, err := s.db.ExecContext(ctx, `UPDATE export_jobs SET state='queued',cleanup_pending=1,error='interrupted' WHERE state='running'`)
			if err != nil {
				if !s.waitExports(ctx, time.Second) {
					return
				}
				continue
			}
			recovered = true
		}
		s.expireExports(ctx)
		if job, ok := s.nextCleanupExport(ctx); ok {
			cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			err := s.cleanupExport(cleanup, job)
			cancel()
			if err != nil {
				if !s.waitExports(ctx, time.Second) {
					return
				}
				continue
			}
		}
		job, ok := s.nextQueuedExport(ctx)
		if !ok {
			if !s.waitExports(ctx, time.Second) {
				return
			}
			continue
		}
		if job.CancelRequested {
			s.terminalExport(job, ExportCanceled, "canceled")
			continue
		}
		actor, err := s.exportJobActor(ctx, job)
		if errors.Is(err, errExportAuthorizationExpired) {
			s.terminalExport(job, ExportExpired, "authorization_expired")
			continue
		}
		if authenticationUnavailable(err) {
			if !s.waitExports(ctx, time.Second) {
				return
			}
			continue
		}
		if err != nil || !auth.Authorize(actor, "events:read", job.ProjectID) {
			s.terminalExport(job, ExportCanceled, "creator_revoked")
			continue
		}
		if job.Attempts >= 3 {
			s.terminalExport(job, ExportFailed, "attempt_limit")
			continue
		}
		ready, err := s.exportAdmission(ctx, job)
		if err != nil || !ready {
			if !s.waitExports(ctx, time.Second) {
				return
			}
			continue
		}
		release, available := s.acquirePipelineExport(ctx)
		if !available {
			if !s.waitExports(ctx, time.Second) {
				return
			}
			continue
		}
		claimed, err := s.claimExport(ctx, job)
		if err != nil {
			release()
			continue
		}
		s.runExport(ctx, claimed)
		release()
		// A transient SQLite failure while recording the outcome must not strand
		// a running row after its in-process attempt has stopped.
		recovered = false
		latest, loadErr := s.loadExport(ctx, claimed.ProjectID, claimed.ID)
		if loadErr == nil && latest.State == ExportQueued {
			timer := time.NewTimer(time.Duration(1<<min(claimed.Attempts-1, 4)) * time.Second)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}
}

func (s *Service) waitExports(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-s.exports.wake:
		return true
	case <-timer.C:
		return true
	}
}

func (s *Service) nextQueuedExport(ctx context.Context) (storedExport, bool) {
	job, err := scanExport(s.db.QueryRowContext(ctx, `SELECT `+exportJobColumns+` FROM export_jobs WHERE state='queued' AND cleanup_pending=0 ORDER BY rowid LIMIT 1`))
	return job, err == nil
}

func (s *Service) claimExport(ctx context.Context, job storedExport) (storedExport, error) {
	attempt := eventapi.NewID()
	started := job.StartedAt
	if started == "" {
		started = s.clock().UTC().Format(time.RFC3339Nano)
	}
	result, err := s.db.ExecContext(ctx, `UPDATE export_jobs SET state='running',attempt_id=$1,attempts=attempts+1,started_at=$2,rows=0,bytes=0,decoded_bytes=0,file_count=0,error='' WHERE id=$3 AND state='queued' AND cancel_requested=0 AND cleanup_pending=0`, attempt, started, job.ID)
	if err != nil {
		return storedExport{}, err
	}
	n, _ := result.RowsAffected()
	if n != 1 {
		return storedExport{}, errExportCanceled
	}
	return s.loadExport(ctx, job.ProjectID, job.ID)
}

func (s *Service) exportAdmission(ctx context.Context, job storedExport) (bool, error) {
	var barrier, pending int64
	err := s.db.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM events WHERE project_id=$1 AND id<=$2)+(SELECT count(*) FROM mutation_outbox WHERE project_id=$3 AND seq<=$4),(SELECT count(*) FROM events)+(SELECT count(*) FROM mutation_outbox)`, job.ProjectID, job.EventWatermark, job.ProjectID, job.MutationWatermark).Scan(&barrier, &pending)
	if err != nil {
		return false, err
	}
	var failed int
	if err = s.db.QueryRowContext(ctx, `SELECT count(*) FROM metadata WHERE key='last_delivery_error'`).Scan(&failed); err != nil {
		return false, err
	}
	return barrier == 0 && pending <= s.exports.config.QueueThreshold && failed == 0, nil
}

func (s *Service) exportPressure(ctx context.Context) (bool, error) {
	var count, failed int64
	err := s.db.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM events)+(SELECT count(*) FROM mutation_outbox),(SELECT count(*) FROM metadata WHERE key='last_delivery_error')`).Scan(&count, &failed)
	return count > s.exports.config.QueueThreshold*4 || failed > 0, err
}

func (s *Service) runExport(parent context.Context, job storedExport) {
	parent, finish, active := s.activity.begin(parent, job.ProjectID)
	if !active {
		return
	}
	defer finish()
	started, err := time.Parse(time.RFC3339Nano, job.StartedAt)
	if err != nil {
		s.failExport(parent, job, errors.New("invalid export start"))
		return
	}
	deadline := started.Add(time.Duration(s.exports.config.MaxExecutionSeconds) * time.Second)
	deadlineCause := error(context.DeadlineExceeded)
	if job.ExternalExpiresAtMS > 0 && time.UnixMilli(job.ExternalExpiresAtMS).Before(deadline) {
		deadline = time.UnixMilli(job.ExternalExpiresAtMS)
		deadlineCause = errExportAuthorizationExpired
	}
	ctx, cancel := context.WithDeadlineCause(parent, deadline, deadlineCause)
	ctx, cancelCause := context.WithCancelCause(ctx)
	s.exports.activeMu.Lock()
	s.exports.activeJob = job.ID
	s.exports.activeCancel = func() { cancelCause(errExportCanceled) }
	s.exports.activeMu.Unlock()
	monitorDone := make(chan struct{})
	go func() { defer close(monitorDone); s.monitorExport(ctx, job, cancelCause) }()
	err = s.executeExport(ctx, job)
	cause := context.Cause(ctx)
	cancelCause(context.Canceled)
	cancel()
	<-monitorDone
	s.exports.activeMu.Lock()
	s.exports.activeJob = ""
	s.exports.activeCancel = nil
	s.exports.activeMu.Unlock()
	if err != nil {
		if parent.Err() != nil {
			err = parent.Err()
		} else if cause != nil {
			err = cause
		}
		s.failExport(parent, job, err)
	}
}

func (s *Service) monitorExport(ctx context.Context, job storedExport, cancel context.CancelCauseFunc) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if err := s.exportCanContinue(ctx, job); err != nil {
			cancel(err)
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Service) exportCanContinue(ctx context.Context, job storedExport) error {
	if err := s.requireProjectActive(ctx, job.ProjectID); err != nil {
		return err
	}
	if err := s.requireSharedProjectOwner(ctx, job.ProjectID); err != nil {
		return err
	}
	current, err := s.loadExport(ctx, job.ProjectID, job.ID)
	if err != nil {
		return errExportPressure
	}
	if current.CancelRequested || current.State != ExportRunning || current.AttemptID != job.AttemptID {
		return errExportCanceled
	}
	actor, err := s.exportJobActor(ctx, job)
	if errors.Is(err, errExportAuthorizationExpired) {
		return err
	}
	if authenticationUnavailable(err) {
		return err
	}
	if err != nil || !auth.Authorize(actor, "events:read", job.ProjectID) {
		return errExportRevoked
	}
	pressure, err := s.exportPressure(ctx)
	if err != nil || pressure {
		return errExportPressure
	}
	return nil
}

func (s *Service) executeExport(ctx context.Context, job storedExport) error {
	if s.lake == nil {
		return errExportSink
	}
	if err := s.exportCanContinue(ctx, job); err != nil {
		return err
	}
	sink := newExportSink(ctx, s, job)
	result, err := s.lake.Export(ctx, job.ProjectID, job.SQL, job.Parameters, nil, s.exports.config.MaxRowBytes, lakeapi.ExportStreamCallbacks{Columns: sink.setColumns, Row: sink.writeRow})
	if err != nil {
		return err
	}
	if err = s.exportCanContinue(ctx, job); err != nil {
		return err
	}
	if err = sink.finish(); err != nil {
		return err
	}
	return s.publishExport(ctx, job, result, sink)
}

func (s *Service) publishExport(ctx context.Context, job storedExport, result lakeapi.ExportStreamResult, sink *exportSink) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.requireProjectActive(ctx, job.ProjectID); err != nil {
		return err
	}
	columns, err := json.Marshal(result.Columns)
	if err != nil {
		return lakeapi.ErrExportStreamInvalid
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return errExportSink
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `UPDATE metadata SET value=value WHERE key='initialized'`); err != nil {
		return errExportSink
	}
	current, err := s.exportJobActor(ctx, job)
	if errors.Is(err, errExportAuthorizationExpired) {
		return err
	}
	if errors.Is(err, sql.ErrNoRows) || errors.Is(err, errInteractiveGrant) {
		return errExportRevoked
	}
	if err != nil {
		return errExportSink
	}
	if !auth.Authorize(current, "events:read", job.ProjectID) {
		return errExportRevoked
	}
	completed := s.clock().UTC()
	expires := completed.Add(time.Duration(s.exports.config.RetentionHours) * time.Hour)
	updated, err := tx.ExecContext(ctx, `UPDATE export_jobs SET state='succeeded',sql='',parameters='{}',rows=$1,bytes=$2,decoded_bytes=$3,file_count=$4,columns_json=$5,completed_at=$6,expires_at=$7,expires_at_ms=$8,error='',cleanup_pending=0 WHERE id=$9 AND project_id=$10 AND attempt_id=$11 AND state='running' AND cancel_requested=0`, sink.rows, sink.compressed, sink.decoded, sink.files, string(columns), completed.Format(time.RFC3339Nano), expires.Format(time.RFC3339Nano), expires.UnixMilli(), job.ID, job.ProjectID, job.AttemptID)
	if err != nil {
		return errExportSink
	}
	count, _ := updated.RowsAffected()
	if count != 1 {
		return errExportCanceled
	}
	if err = tx.Commit(); err != nil {
		return errExportSink
	}
	return nil
}

func (s *Service) failExport(parent context.Context, job storedExport, failure error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	state, code := ExportFailed, "export_failed"
	current, err := s.loadExport(ctx, job.ProjectID, job.ID)
	actor, actorErr := s.exportJobActor(ctx, job)
	switch {
	case errors.Is(failure, errExportAuthorizationExpired) || errors.Is(actorErr, errExportAuthorizationExpired):
		state, code = ExportExpired, "authorization_expired"
	case err == nil && current.CancelRequested:
		state, code = ExportCanceled, "canceled"
	case authenticationUnavailable(actorErr) || authenticationUnavailable(failure):
		code = "authorization_unavailable"
		if job.Attempts < 3 {
			state = ExportQueued
		}
	case actorErr != nil || !auth.Authorize(actor, "events:read", job.ProjectID):
		state, code = ExportCanceled, "creator_revoked"
	case errors.Is(failure, errExportRevoked):
		state, code = ExportCanceled, "creator_revoked"
	case errors.Is(failure, context.DeadlineExceeded):
		code = "deadline_exceeded"
	case errors.Is(failure, errExportOutputLimit):
		code = "output_limit"
	case errors.Is(failure, lakeapi.ErrExportMetadataTooLarge):
		code = "metadata_limit"
	case errors.Is(failure, lakeapi.ErrExportValueTooLarge):
		code = "row_limit"
	case errors.Is(failure, lakeapi.ErrExportStreamInvalid):
		code = "stream_invalid"
	case parent.Err() != nil:
		state, code = ExportQueued, "interrupted"
	case errors.Is(failure, errExportPressure) && job.Attempts < 3:
		state, code = ExportQueued, "delivery_pressure"
	case errors.Is(failure, errExportCanceled):
		state, code = ExportCanceled, "canceled"
	case job.Attempts < 3:
		state, code = ExportQueued, "retry_pending"
	}
	completed := ""
	sqlKeep := job.SQL
	parameters, _ := json.Marshal(job.Parameters)
	if state != ExportQueued {
		completed = s.clock().UTC().Format(time.RFC3339Nano)
		sqlKeep = ""
		parameters = []byte("{}")
	}
	s.db.ExecContext(ctx, `UPDATE export_jobs SET state=$1,error=$2,cleanup_pending=1,completed_at=$3,sql=$4,parameters=$5 WHERE id=$6 AND attempt_id=$7 AND state='running'`, state, code, completed, sqlKeep, string(parameters), job.ID, job.AttemptID)
	cleanup, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cleanupCancel()
	if latest, err := s.loadExport(cleanup, job.ProjectID, job.ID); err == nil {
		s.cleanupExport(cleanup, latest)
	}
}

func (s *Service) terminalExport(job storedExport, state ExportState, code string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s.db.ExecContext(ctx, `UPDATE export_jobs SET state=$1,error=$2,cleanup_pending=1,completed_at=$3,sql='',parameters='{}' WHERE id=$4 AND state='queued'`, state, code, s.clock().UTC().Format(time.RFC3339Nano), job.ID)
}

func (s *Service) expireExports(ctx context.Context) {
	s.db.ExecContext(ctx, `UPDATE export_jobs SET state='expired',error='authorization_expired',cleanup_pending=1,sql='',parameters='{}' WHERE state='queued' AND id IN (SELECT job_id FROM export_external_authority WHERE expires_at_ms<=$1)`, s.clock().UnixMilli())
	s.db.ExecContext(ctx, `UPDATE export_jobs SET state='expired',cleanup_pending=1,sql='',parameters='{}' WHERE state='succeeded' AND expires_at_ms>0 AND expires_at_ms<=$1`, s.clock().UnixMilli())
}

func (s *Service) nextCleanupExport(ctx context.Context) (storedExport, bool) {
	job, err := scanExport(s.db.QueryRowContext(ctx, `SELECT `+exportJobColumns+` FROM export_jobs WHERE cleanup_pending=1 AND state!='running' ORDER BY rowid LIMIT 1`))
	return job, err == nil
}

func (s *Service) cleanupExport(ctx context.Context, job storedExport) error {
	if s.exports == nil || s.exports.store == nil {
		return errExportSink
	}
	rows, err := s.db.QueryContext(ctx, `SELECT attempt_id,file_index,object_key,upload_id,storage_id FROM export_files WHERE job_id=$1 ORDER BY attempt_id,file_index`, job.ID)
	if err != nil {
		return err
	}
	type cleanupFile struct {
		attempt          string
		index            int
		key, id, storage string
	}
	files := []cleanupFile{}
	for rows.Next() {
		var file cleanupFile
		if err = rows.Scan(&file.attempt, &file.index, &file.key, &file.id, &file.storage); err != nil {
			rows.Close()
			return err
		}
		files = append(files, file)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, file := range files {
		if file.storage == "" || file.storage != s.exports.store.OwnershipID() {
			return errors.New("export cleanup requires its original bound storage namespace")
		}
		if err := s.retainArtifactOwnership(ctx, job.ProjectID, "export", s.exports.store, file.key, file.id); err != nil {
			return err
		}
		if err = s.removeExportArtifact(ctx, file.key, file.id); err != nil {
			return err
		}
		if _, err = s.db.ExecContext(ctx, `DELETE FROM export_files WHERE job_id=$1 AND attempt_id=$2 AND file_index=$3`, job.ID, file.attempt, file.index); err != nil {
			return err
		}
	}
	_, err = s.db.ExecContext(ctx, `UPDATE export_jobs SET cleanup_pending=0 WHERE id=$1 AND state!='running'`, job.ID)
	return err
}
