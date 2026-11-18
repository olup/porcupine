package output

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/olup/porcupine/internal/auth"
	eventapi "github.com/olup/porcupine/internal/event"
	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/usage"
)

var (
	errExportPressure             = errors.New("export delivery pressure")
	ErrExportRevoked              = errors.New("export creator revoked")
	errExportCanceled             = errors.New("export canceled")
	errExportAuthorizationExpired = errors.New("external export authorization expired")
)

func (s *ExportJobs) Start() {
	if s == nil || s.store == nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.done = make(chan struct{})
	if s.wake == nil {
		s.wake = make(chan struct{}, 1)
	}
	go func() { defer close(s.done); s.exportsLoop(ctx) }()
}

func (s *ExportJobs) Stop() {
	if s == nil {
		return
	}
	if s.cancel != nil {
		s.cancel()
	}
	if s.done != nil {
		<-s.done
	}
}

func (s *ExportJobs) exportsLoop(ctx context.Context) {
	recovered := false
	for ctx.Err() == nil {
		if s.retained == nil && !recovered {
			_, err := s.db.ExecContext(ctx, `UPDATE export_jobs SET state='queued',cleanup_pending=1,error='interrupted' WHERE state='running'`)
			if err != nil {
				if !s.waitExports(ctx, time.Second) {
					return
				}
				continue
			}
			recovered = true
		}
		worked, _ := s.RunOne(ctx)
		if !worked {
			if !s.waitExports(ctx, time.Second) {
				return
			}
			continue
		}
		// A transient SQLite failure while recording the outcome must not strand
		// a running row after its in-process attempt has stopped.
		recovered = false
		latest, queued := s.nextQueuedExport(ctx)
		if queued && latest.Attempts > 0 {
			timer := time.NewTimer(time.Duration(1<<min(latest.Attempts-1, 4)) * time.Second)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}
}

func (s *ExportJobs) waitExports(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-s.wake:
		return true
	case <-timer.C:
		return true
	}
}

func (s *ExportJobs) nextQueuedExport(ctx context.Context) (storedExport, bool) {
	filter := ""
	if s.retained != nil {
		filter = ` AND NOT EXISTS(SELECT 1 FROM project_deletions d WHERE d.project_id=export_jobs.project_id)
 AND NOT EXISTS(SELECT 1 FROM tenant_deletions d JOIN projects p ON p.tenant_id=d.tenant_id WHERE p.id=export_jobs.project_id)`
	}
	job, err := s.scanExport(s.db.QueryRowContext(ctx, `SELECT `+s.exportColumns()+` FROM export_jobs WHERE state='queued' AND cleanup_pending=0`+filter+` ORDER BY `+s.exportOrder()+` LIMIT 1`))
	return job, err == nil
}

func (s *ExportJobs) claimExport(ctx context.Context, job storedExport) (storedExport, error) {
	attempt := eventapi.NewID()
	started := job.StartedAt
	if started == "" {
		started = s.clock().UTC().Format(time.RFC3339Nano)
	}
	claim := `UPDATE export_jobs SET state='running',attempt_id=$1,attempts=attempts+1,started_at=$2,
rows=0,bytes=0,decoded_bytes=0,file_count=0,error=''`
	eligible := ` WHERE id=$3 AND state='queued' AND cancel_requested=0 AND cleanup_pending=0 AND attempt_id=$4 AND attempts=$5`
	args := []any{attempt, started, job.ID, job.AttemptID, job.Attempts}
	if s.retained != nil {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return storedExport{}, err
		}
		defer tx.Rollback()
		if err := s.beginAttemptTx(ctx, tx, job.ProjectID, attempt); err != nil {
			return storedExport{}, err
		}
		// A registered attempt ends on every failed path, even after rollback.
		project := job.ProjectID
		err = verifyExportMutation(tx.ExecContext(ctx, claim+`,lease_until=`+exportLeaseExtension+eligible, args...))
		if err == nil {
			job, err = s.scanExport(tx.QueryRowContext(ctx, `SELECT `+s.exportColumns()+` FROM export_jobs WHERE id=$1`, job.ID))
		}
		if err == nil {
			err = tx.Commit()
		}
		if err != nil {
			s.finishExportActivity(project, attempt)
			return storedExport{}, err
		}
		return job, nil
	}
	if err := verifyExportMutation(s.db.ExecContext(ctx, claim+eligible, args...)); err != nil {
		return storedExport{}, err
	}
	return s.loadExport(ctx, job.ProjectID, job.ID)
}

func (s *ExportJobs) exportAdmission(ctx context.Context, job storedExport) (bool, error) {
	if s.retained != nil {
		return true, s.requireActive(ctx, job.ProjectID)
	}
	var barrier, pending int64
	err := s.db.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM events WHERE project_id=$1 AND id<=$2)+(SELECT count(*) FROM mutation_outbox WHERE project_id=$3 AND seq<=$4),(SELECT count(*) FROM events)+(SELECT count(*) FROM mutation_outbox)`, job.ProjectID, job.EventWatermark, job.ProjectID, job.MutationWatermark).Scan(&barrier, &pending)
	if err != nil {
		return false, err
	}
	var failed int
	if err = s.db.QueryRowContext(ctx, `SELECT count(*) FROM metadata WHERE key='last_delivery_error'`).Scan(&failed); err != nil {
		return false, err
	}
	return barrier == 0 && pending <= s.config.QueueThreshold && failed == 0, nil
}

func (s *ExportJobs) exportPressure(ctx context.Context) (bool, error) {
	if s.retained != nil {
		return false, nil
	}
	var count, failed int64
	err := s.db.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM events)+(SELECT count(*) FROM mutation_outbox),(SELECT count(*) FROM metadata WHERE key='last_delivery_error')`).Scan(&count, &failed)
	return count > s.config.QueueThreshold*4 || failed > 0, err
}

func (s *ExportJobs) runExport(parent context.Context, job storedExport) {
	parent, finish, active := s.beginActivity(parent, job.ProjectID)
	if !active {
		return
	}
	defer finish()
	started, err := time.Parse(time.RFC3339Nano, job.StartedAt)
	if err != nil {
		s.failExport(parent, job, errors.New("invalid export start"))
		return
	}
	deadline := started.Add(time.Duration(s.config.MaxExecutionSeconds) * time.Second)
	deadlineCause := error(context.DeadlineExceeded)
	if job.ExternalExpiresAtMS > 0 && time.UnixMilli(job.ExternalExpiresAtMS).Before(deadline) {
		deadline = time.UnixMilli(job.ExternalExpiresAtMS)
		deadlineCause = errExportAuthorizationExpired
	}
	ctx, cancel := context.WithDeadlineCause(parent, deadline, deadlineCause)
	ctx, cancelCause := context.WithCancelCause(ctx)
	s.activeMu.Lock()
	s.activeJob = job.ID
	s.activeCancel = func() { cancelCause(errExportCanceled) }
	s.activeMu.Unlock()
	monitorDone := make(chan struct{})
	go func() { defer close(monitorDone); s.monitorExport(ctx, job, cancelCause) }()
	err = s.executeExport(ctx, job)
	cause := context.Cause(ctx)
	cancelCause(context.Canceled)
	cancel()
	<-monitorDone
	s.activeMu.Lock()
	s.activeJob = ""
	s.activeCancel = nil
	s.activeMu.Unlock()
	if err != nil {
		if parent.Err() != nil {
			err = parent.Err()
		} else if cause != nil {
			err = cause
		}
		s.failExport(parent, job, err)
	}
}

func (s *ExportJobs) monitorExport(ctx context.Context, job storedExport, cancel context.CancelCauseFunc) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if s.retained != nil {
			if err := s.renewExport(ctx, job); err != nil {
				cancel(err)
				return
			}
		}
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

func (s *ExportJobs) exportCanContinue(ctx context.Context, job storedExport) error {
	if err := s.requireActive(ctx, job.ProjectID); err != nil {
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
		return ErrExportRevoked
	}
	pressure, err := s.exportPressure(ctx)
	if err != nil || pressure {
		return errExportPressure
	}
	return nil
}

func (s *ExportJobs) executeExport(ctx context.Context, job storedExport) error {
	if s.lake == nil && s.retained == nil {
		return errExportSink
	}
	if err := s.exportCanContinue(ctx, job); err != nil {
		return err
	}
	sink := newExportSink(ctx, s, job)
	callbacks := lakeapi.ExportStreamCallbacks{Columns: sink.setColumns, Row: sink.writeRow}
	var result lakeapi.ExportStreamResult
	var err error
	if s.retained != nil {
		result, err = s.retained.ExportAtSnapshot(ctx, job.ProjectID, job.SQL, job.Parameters, job.Snapshot, s.config.MaxRowBytes, callbacks)
	} else {
		result, err = s.lake.Export(ctx, job.ProjectID, job.SQL, job.Parameters, nil, s.config.MaxRowBytes, callbacks)
	}
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

func (s *ExportJobs) publishExport(ctx context.Context, job storedExport, result lakeapi.ExportStreamResult, sink *exportSink) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.requireActive(ctx, job.ProjectID); err != nil {
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
	if s.retained == nil {
		if err = s.lockExportQueue(ctx, tx); err != nil {
			return errExportSink
		}
	} else {
		if _, err = lockActiveExportProject(ctx, tx, job.ProjectID); err != nil {
			return err
		}
		if err = s.lockRunningExport(ctx, tx, job); err != nil {
			return err
		}
	}
	current, err := s.exportJobActorTx(ctx, tx, job)
	if errors.Is(err, errExportAuthorizationExpired) {
		return err
	}
	if errors.Is(err, sql.ErrNoRows) || errors.Is(err, ErrExportRevoked) {
		return ErrExportRevoked
	}
	if err != nil {
		return errExportSink
	}
	if !auth.Authorize(current, "events:read", job.ProjectID) {
		return ErrExportRevoked
	}
	if s.retained != nil && job.ExternalExpiresAtMS > 0 {
		var valid bool
		if err = tx.QueryRowContext(ctx, `SELECT expires_at_ms>extract(epoch FROM clock_timestamp())*1000 FROM export_external_authority WHERE job_id=$1`, job.ID).Scan(&valid); err != nil {
			return err
		}
		if !valid {
			return errExportAuthorizationExpired
		}
	}
	completed := s.clock().UTC()
	expires := completed.Add(time.Duration(s.config.RetentionHours) * time.Hour)
	updated, err := tx.ExecContext(ctx, `UPDATE export_jobs SET state='succeeded',sql='',parameters='{}',rows=$1,bytes=$2,decoded_bytes=$3,file_count=$4,columns_json=$5,completed_at=$6,expires_at=$7,expires_at_ms=$8,error='',cleanup_pending=0 WHERE id=$9 AND project_id=$10 AND attempt_id=$11 AND state='running' AND cancel_requested=0`+s.leaseGuard(), sink.rows, sink.compressed, sink.decoded, sink.files, string(columns), completed.Format(time.RFC3339Nano), expires.Format(time.RFC3339Nano), expires.UnixMilli(), job.ID, job.ProjectID, job.AttemptID)
	if err != nil {
		return errExportSink
	}
	count, _ := updated.RowsAffected()
	if count != 1 {
		return errExportCanceled
	}
	if s.usage != nil {
		if err = s.usage.AddTx(ctx, tx, job.ProjectID, completed, usage.Counters{ExportBytes: sink.compressed}, 0); err != nil {
			return errExportSink
		}
	}
	if err = tx.Commit(); err != nil {
		return errExportSink
	}
	return nil
}

func (s *ExportJobs) failExport(parent context.Context, job storedExport, failure error) {
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
	case errors.Is(failure, ErrExportRevoked):
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
	case errors.Is(failure, lakeapi.ErrExportSnapshotUnavailable):
		code = "snapshot_unavailable"
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
	updated, updateErr := s.db.ExecContext(ctx, `UPDATE export_jobs SET state=$1,error=$2,cleanup_pending=1,completed_at=$3,sql=$4,parameters=$5 WHERE id=$6 AND attempt_id=$7 AND state='running'`+s.leaseGuard(), state, code, completed, sqlKeep, string(parameters), job.ID, job.AttemptID)
	if verifyExportMutation(updated, updateErr) != nil {
		return
	}
	cleanup, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cleanupCancel()
	if latest, err := s.loadExport(cleanup, job.ProjectID, job.ID); err == nil {
		s.cleanupExport(cleanup, latest)
	}
}

func (s *ExportJobs) terminalExport(job storedExport, state ExportState, code string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s.db.ExecContext(ctx, `UPDATE export_jobs SET state=$1,error=$2,cleanup_pending=1,completed_at=$3,sql='',parameters='{}' WHERE id=$4 AND state='queued' AND attempt_id=$5 AND attempts=$6`, state, code, s.clock().UTC().Format(time.RFC3339Nano), job.ID, job.AttemptID, job.Attempts)
}

func (s *ExportJobs) expireExports(ctx context.Context) {
	s.db.ExecContext(ctx, `UPDATE export_jobs SET state='expired',error='authorization_expired',cleanup_pending=1,sql='',parameters='{}' WHERE state='queued' AND id IN (SELECT job_id FROM export_external_authority WHERE expires_at_ms<=$1)`, s.clock().UnixMilli())
	s.db.ExecContext(ctx, `UPDATE export_jobs SET state='expired',cleanup_pending=1,sql='',parameters='{}' WHERE state='succeeded' AND expires_at_ms>0 AND expires_at_ms<=$1`, s.clock().UnixMilli())
}

func (s *ExportJobs) nextCleanupExport(ctx context.Context) (storedExport, bool) {
	job, err := s.scanExport(s.db.QueryRowContext(ctx, `SELECT `+s.exportColumns()+` FROM export_jobs WHERE cleanup_pending=1 AND state!='running' ORDER BY `+s.exportOrder()+` LIMIT 1`))
	return job, err == nil
}

func (s *ExportJobs) cleanupExport(ctx context.Context, job storedExport) error {
	if s == nil || s.store == nil {
		return errExportSink
	}
	query := `SELECT attempt_id,file_index,object_key,upload_id,storage_id FROM export_files WHERE job_id=$1`
	args := []any{job.ID}
	if s.retained != nil {
		query += ` AND attempt_id=$2`
		args = append(args, job.AttemptID)
	}
	rows, err := s.db.QueryContext(ctx, query+` ORDER BY attempt_id,file_index`, args...)
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
		if file.storage == "" || file.storage != s.store.OwnershipID() {
			return errors.New("export cleanup requires its original bound storage namespace")
		}
		if err := retainArtifactOwnership(ctx, s.control, job.ProjectID, "export", s.store, file.key, file.id); err != nil {
			return err
		}
		if err = s.retireExportArtifact(ctx, file.key, file.id); err != nil {
			return err
		}
		if _, err = s.db.ExecContext(ctx, `DELETE FROM export_files WHERE job_id=$1 AND attempt_id=$2 AND file_index=$3`, job.ID, file.attempt, file.index); err != nil {
			return err
		}
	}
	retire := ""
	if s.retained != nil {
		retire = `,retire_after=CASE WHEN state IN ('failed','canceled','expired') THEN clock_timestamp()+interval '30 days' END`
	}
	_, err = s.db.ExecContext(ctx, `UPDATE export_jobs SET cleanup_pending=0`+retire+` WHERE id=$1 AND state!='running' AND attempt_id=$2 AND cleanup_pending=1`, job.ID, job.AttemptID)
	return err
}

// RunOne performs one bounded cleanup/admission/claim/execution cycle.
// Capacity and native readers remain held until execution actually returns.
func (s *ExportJobs) RunOne(ctx context.Context) (bool, error) {
	if !s.Enabled() {
		return false, nil
	}
	if s.retained != nil {
		if err := s.reclaimExports(ctx); err != nil {
			return false, err
		}
		if err := s.retireExports(ctx); err != nil {
			return false, err
		}
	}
	s.expireExports(ctx)
	var cleanupErr error
	if job, ok := s.nextCleanupExport(ctx); ok {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		cleanupErr = s.cleanupExport(cleanup, job)
		cancel()
		if cleanupErr != nil && s.retained == nil {
			return false, nil
		}
	}
	job, ok := s.nextQueuedExport(ctx)
	if !ok {
		return false, cleanupErr
	}
	if job.CancelRequested {
		s.terminalExport(job, ExportCanceled, "canceled")
		return true, nil
	}
	actor, err := s.exportJobActor(ctx, job)
	if errors.Is(err, errExportAuthorizationExpired) {
		s.terminalExport(job, ExportExpired, "authorization_expired")
		return true, nil
	}
	if authenticationUnavailable(err) {
		return false, nil
	}
	if err != nil || !auth.Authorize(actor, "events:read", job.ProjectID) {
		s.terminalExport(job, ExportCanceled, "creator_revoked")
		return true, nil
	}
	if job.Attempts >= 3 {
		s.terminalExport(job, ExportFailed, "attempt_limit")
		return true, nil
	}
	ready, err := s.exportAdmission(ctx, job)
	if err != nil || !ready {
		return false, nil
	}
	release, available := s.acquire(ctx)
	if !available {
		return false, nil
	}
	claimed, err := s.claimExport(ctx, job)
	if err != nil {
		release()
		return true, nil
	}
	s.runExport(ctx, claimed)
	release()
	if s.retained != nil {
		s.finishExportActivity(claimed.ProjectID, claimed.AttemptID)
	}
	return true, nil
}
