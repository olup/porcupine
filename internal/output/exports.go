package output

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/olup/porcupine/internal/apierr"
	"github.com/olup/porcupine/internal/auth"
	eventapi "github.com/olup/porcupine/internal/event"
	"github.com/olup/porcupine/internal/exportstore"
	lakeapi "github.com/olup/porcupine/internal/lake"
)

func exportQueueLimit(config ExportConfig) int {
	if config.MaxQueuedJobs <= 0 {
		return 100
	}
	return min(config.MaxQueuedJobs, 100)
}

func (s *ExportJobs) Submit(ctx context.Context, project string, request ExportRequest, caller auth.Principal) (ExportJob, error) {
	if !s.Enabled() {
		return ExportJob{}, apierr.New(503, "exports are not configured")
	}
	if s.retained != nil {
		finish, err := s.beginExportAdmissionActivity(ctx, project)
		if err != nil {
			return ExportJob{}, apierr.New(503, "export admission unavailable")
		}
		defer finish()
	}
	canonical := request.SQL
	if s.validateSQL != nil {
		var err error
		canonical, err = s.validateSQL(ctx, project, canonical, request.Parameters)
		if err != nil {
			return ExportJob{}, err
		}
	}
	format := request.Format
	if format == "" {
		format = "jsonl"
	}
	if format != "jsonl" && format != "csv" {
		return ExportJob{}, apierr.New(400, "export format must be jsonl or csv")
	}
	parameters := request.Parameters
	if parameters == nil {
		parameters = map[string]string{}
	}
	payload, err := json.Marshal(struct {
		SQL        string            `json:"sql"`
		Parameters map[string]string `json:"parameters"`
		Format     string            `json:"format"`
	}{canonical, parameters, format})
	if err != nil {
		return ExportJob{}, apierr.New(400, "invalid export request")
	}
	encodedParameters, _ := json.Marshal(parameters)
	keyHash, requestHash := sha256Hex([]byte(request.IdempotencyKey)), sha256Hex(payload)

	admission := exportAdmissionRequest{
		project: project, canonical: canonical, format: format, parameters: string(encodedParameters),
		keyHash: keyHash, requestHash: requestHash, caller: caller,
	}
	if s.retained == nil {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return ExportJob{}, apierr.New(503, "export storage unavailable")
		}
		defer tx.Rollback()
		job, err := s.submitExportTx(ctx, tx, admission, lakeapi.ExportSnapshot{})
		if err != nil {
			return ExportJob{}, err
		}
		if err = tx.Commit(); err != nil {
			return ExportJob{}, apierr.New(503, "export storage unavailable")
		}
		s.wakeWorker()
		return job, nil
	}
	// Fast idempotency lookup avoids recapturing a snapshot for an admitted
	// request. The insert transaction repeats it to handle competing requests.
	if previous, found, err := s.previousExport(ctx, s.db, admission); found || err != nil {
		return previous, err
	}
	var job ExportJob
	err = s.snapshotCut(ctx, project, func(cut context.Context, run ExportControlTx) error {
		snapshot, err := s.retained.CaptureExportSnapshot(cut, project)
		if err != nil || snapshot.CatalogID == "" || snapshot.Version < 0 {
			return apierr.New(503, "export analytical snapshot unavailable")
		}
		return run(cut, func(ctx context.Context, tx *sql.Tx) error {
			job, err = s.submitExportTx(ctx, tx, admission, snapshot)
			return err
		})
	})
	if err == nil && job.ID == "" {
		err = apierr.New(503, "export analytical snapshot unavailable")
	}
	if err != nil {
		var status interface{ GetStatus() int }
		if !errors.As(err, &status) {
			err = apierr.New(503, "export analytical snapshot unavailable")
		}
		return ExportJob{}, err
	}
	s.wakeWorker()
	return job, nil
}

type exportAdmissionRequest struct {
	project, canonical, format, parameters, keyHash, requestHash string
	caller                                                       auth.Principal
}

type exportQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (s *ExportJobs) previousExport(ctx context.Context, db exportQueryer, admission exportAdmissionRequest) (ExportJob, bool, error) {
	var previousID, previousHash string
	err := db.QueryRowContext(ctx, `SELECT id,request_hash FROM export_jobs WHERE project_id=$1 AND actor_id=$2 AND idempotency_hash=$3`, admission.project, admission.caller.ID, admission.keyHash).Scan(&previousID, &previousHash)
	if errors.Is(err, sql.ErrNoRows) {
		return ExportJob{}, false, nil
	}
	if err != nil {
		return ExportJob{}, false, apierr.New(503, "export storage unavailable")
	}
	if previousHash != admission.requestHash {
		return ExportJob{}, true, apierr.New(409, "idempotency_key was used for a different export request")
	}
	job, err := s.scanExport(db.QueryRowContext(ctx, `SELECT `+s.exportColumns()+` FROM export_jobs WHERE id=$1`, previousID))
	if err != nil {
		return ExportJob{}, true, apierr.New(503, "export storage unavailable")
	}
	return s.publicExportJob(job), true, nil
}

func (s *ExportJobs) submitExportTx(ctx context.Context, tx *sql.Tx, admission exportAdmissionRequest, snapshot lakeapi.ExportSnapshot) (ExportJob, error) {
	if err := s.lockExportQueue(ctx, tx); err != nil {
		return ExportJob{}, apierr.New(503, "export storage unavailable")
	}
	if previous, found, err := s.previousExport(ctx, tx, admission); found || err != nil {
		return previous, err
	}

	job := ExportJob{ID: eventapi.NewID(), ProjectID: admission.project, State: ExportQueued, Format: admission.format, CreatedAt: s.clock().UTC().Format(time.RFC3339Nano), Consistency: exportConsistency}
	windowColumns := "event_watermark,mutation_watermark"
	var first, second any
	var tenant string
	var err error
	if s.retained == nil {
		var pending int
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM export_jobs WHERE state IN ('queued','running')`).Scan(&pending); err != nil {
			return ExportJob{}, apierr.New(503, "export storage unavailable")
		}
		if pending >= exportQueueLimit(s.config) {
			return ExportJob{}, apierr.New(503, "export queue is full")
		}
		var events, mutations int64
		if err := tx.QueryRowContext(ctx, `SELECT coalesce((SELECT seq FROM sqlite_sequence WHERE name='events'),0),coalesce((SELECT max(seq) FROM mutation_audit),0)`).Scan(&events, &mutations); err != nil {
			return ExportJob{}, apierr.New(503, "export delivery barrier unavailable")
		}
		first, second = events, mutations
	} else {
		tenant, err = lockActiveExportProject(ctx, tx, admission.project)
		if errors.Is(err, sql.ErrNoRows) {
			return ExportJob{}, apierr.New(404, "project not found")
		}
		if err != nil {
			return ExportJob{}, apierr.New(503, "export project unavailable")
		}

		if err := s.checkHostedExportCapacity(ctx, tx, tenant, admission.project); err != nil {
			return ExportJob{}, err
		}
		if admission.caller.External == nil {
			actor, authorityErr := s.exportJobActorTx(ctx, tx, storedExport{ExportJob: job, ActorID: admission.caller.ID})
			if authenticationUnavailable(authorityErr) {
				return ExportJob{}, apierr.New(503, "export authorization unavailable")
			}
			if authorityErr != nil || !auth.Authorize(actor, "events:read", job.ProjectID) {
				return ExportJob{}, apierr.New(403, "export authorization has changed")
			}
		}
		windowColumns, first, second = "snapshot_catalog_id,snapshot_version", snapshot.CatalogID, snapshot.Version
		job.Consistency = retainedExportConsistency
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO export_jobs(id,project_id,actor_id,sql,parameters,format,idempotency_hash,request_hash,state,`+windowColumns+`,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`, job.ID, job.ProjectID, admission.caller.ID, admission.canonical, admission.parameters, job.Format, admission.keyHash, admission.requestHash, job.State, first, second, job.CreatedAt)
	if err == nil && admission.caller.External != nil {
		var actor auth.Principal
		var authorityErr error
		if s.retained == nil {
			actor, authorityErr = s.currentActor(ctx, admission.caller)
		} else {
			actor, authorityErr = s.currentActorTx(ctx, tx, admission.caller)
		}
		if authenticationUnavailable(authorityErr) {
			return ExportJob{}, apierr.New(503, "export authorization unavailable")
		}
		if authorityErr != nil || actor.External == nil || !auth.Authorize(actor, "events:read", job.ProjectID) {
			return ExportJob{}, apierr.New(403, "external export authorization expired or changed")
		}
		if s.retained == nil {
			err = s.control.QueryRowContext(ctx, `SELECT tenant_id FROM projects WHERE id=$1`, job.ProjectID).Scan(&tenant)
		}
		if err == nil {
			_, err = tx.ExecContext(ctx, `INSERT INTO export_external_authority(job_id,tenant_id,issuer,expires_at_ms) VALUES($1,$2,$3,$4)`, job.ID, tenant, actor.External.Issuer(), actor.External.Expires().UnixMilli())
		}
	}
	if err != nil {
		return ExportJob{}, apierr.New(503, "export storage unavailable")
	}
	return job, nil
}

func (s *ExportJobs) wakeWorker() {
	if s.Enabled() {
		select {
		case s.wake <- struct{}{}:
		default:
		}
	}
}

func (s *ExportJobs) publicExportJob(job storedExport) ExportJob {
	if job.State == ExportSucceeded && job.ExpiresAtMS > 0 && job.ExpiresAtMS <= s.clock().UnixMilli() {
		job.State = ExportExpired
	}
	return job.ExportJob
}

func validateExportPage(limit, offset int) error {
	if limit < 1 || limit > 100 || offset < 0 {
		return apierr.New(400, "export pagination requires limit 1 to 100 and a nonnegative offset")
	}
	return nil
}

func (s *ExportJobs) List(ctx context.Context, project string, limit, offset int) (ExportPage, error) {
	if err := validateExportPage(limit, offset); err != nil {
		return ExportPage{}, err
	}
	jobs, err := s.storedExportPage(ctx, project, limit+1, offset)
	if err != nil {
		return ExportPage{}, apierr.New(503, "export storage unavailable")
	}
	result := ExportPage{Jobs: []ExportJob{}}
	if len(jobs) > limit {
		next := offset + limit
		result.NextOffset = &next
		jobs = jobs[:limit]
	}
	for _, job := range jobs {
		result.Jobs = append(result.Jobs, s.publicExportJob(job))
	}

	return result, nil
}

func (s *ExportJobs) Get(ctx context.Context, project, id string) (ExportJob, error) {
	job, err := s.loadExport(ctx, project, id)
	if err != nil {
		return ExportJob{}, exportLookupError(err)
	}
	return s.publicExportJob(job), nil
}

func (s *ExportJobs) Cancel(ctx context.Context, project, id string, caller auth.Principal) (ExportJob, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ExportJob{}, apierr.New(503, "export storage unavailable")
	}
	defer tx.Rollback()
	if s.retained == nil {
		if err := s.lockExportQueue(ctx, tx); err != nil {
			return ExportJob{}, apierr.New(503, "export storage unavailable")
		}
	}
	job, err := s.scanExport(tx.QueryRowContext(ctx, `SELECT `+s.exportColumns()+` FROM export_jobs WHERE project_id=$1 AND id=$2`+s.lockExportJob(), project, id))
	if err != nil {
		return ExportJob{}, exportLookupError(err)
	}
	if caller.ID != job.ActorID && !caller.Admin() {
		return ExportJob{}, apierr.New(403, "only the export creator or an administrator can cancel it")
	}
	if job.State != ExportQueued && job.State != ExportRunning {
		return ExportJob{}, apierr.New(409, "export is already terminal")
	}
	if job.State == ExportQueued {
		job.State = ExportCanceled
		job.CompletedAt = s.clock().UTC().Format(time.RFC3339Nano)
		_, err = tx.ExecContext(ctx, `UPDATE export_jobs SET state='canceled',cancel_requested=1,cleanup_pending=1,completed_at=$1,sql='',parameters='{}' WHERE id=$2`, job.CompletedAt, job.ID)
	} else {
		_, err = tx.ExecContext(ctx, `UPDATE export_jobs SET cancel_requested=1,cleanup_pending=1 WHERE id=$1`, job.ID)
	}
	if err != nil || tx.Commit() != nil {
		return ExportJob{}, apierr.New(503, "export storage unavailable")
	}
	if s.Enabled() {
		s.activeMu.Lock()
		cancel := s.activeCancel
		if s.activeJob != job.ID {
			cancel = nil
		}
		s.activeMu.Unlock()
		if cancel != nil {
			cancel()
		}
	}
	s.wakeWorker()
	return job.ExportJob, nil
}

func (s *ExportJobs) Files(ctx context.Context, project, id string, limit, offset int, caller auth.Principal) (ExportFilePage, error) {
	if err := validateExportPage(limit, offset); err != nil {
		return ExportFilePage{}, err
	}
	job, err := s.loadExport(ctx, project, id)
	if err != nil {
		return ExportFilePage{}, exportLookupError(err)
	}
	now := s.clock().UTC()
	if job.State == ExportExpired || job.State == ExportSucceeded && job.ExpiresAtMS > 0 && job.ExpiresAtMS <= now.UnixMilli() {
		return ExportFilePage{}, apierr.New(410, "export has expired")
	}
	if job.State != ExportSucceeded {
		return ExportFilePage{}, apierr.New(409, "export files are available only after success")
	}
	if job.ExpiresAtMS <= 0 {
		return ExportFilePage{}, apierr.New(503, "export retention metadata unavailable")
	}
	if !s.Enabled() || s.store == nil {
		return ExportFilePage{}, apierr.New(503, "export storage is not configured")
	}
	rows, err := s.db.QueryContext(ctx, `SELECT file_index,object_key,rows,bytes,decoded_bytes,sha256,storage_id FROM export_files WHERE job_id=$1 AND attempt_id=$2 AND complete=1 ORDER BY file_index LIMIT $3 OFFSET $4`, job.ID, job.AttemptID, limit+1, offset)
	if err != nil {
		return ExportFilePage{}, apierr.New(503, "export file metadata unavailable")
	}
	artifacts := []exportArtifact{}
	for rows.Next() {
		var artifact exportArtifact
		var storage string
		if err := rows.Scan(&artifact.Index, &artifact.Key, &artifact.Rows, &artifact.Bytes, &artifact.DecodedBytes, &artifact.SHA256, &storage); err != nil {
			rows.Close()
			return ExportFilePage{}, apierr.New(503, "export file metadata unavailable")
		}
		if s.retained != nil && storage != s.store.OwnershipID() {
			rows.Close()
			return ExportFilePage{}, apierr.New(503, "export storage namespace unavailable")
		}
		artifacts = append(artifacts, artifact)
	}
	readErr := rows.Err()
	rows.Close()
	if readErr != nil {
		return ExportFilePage{}, apierr.New(503, "export file metadata unavailable")
	}
	result := ExportFilePage{Files: []ExportFile{}, Limit: limit, Offset: offset}
	if len(artifacts) > limit {
		next := offset + limit
		result.NextOffset = &next
		artifacts = artifacts[:limit]
	}
	for _, artifact := range artifacts {
		deadline := time.UnixMilli(job.ExpiresAtMS)
		if caller := caller; caller.External != nil && caller.External.Expires().Before(deadline) {
			deadline = caller.External.Expires()
		}
		ttl := min(exportLinkTTL, deadline.Sub(s.clock()))
		if ttl < time.Second {
			return ExportFilePage{}, apierr.New(410, "export download authorization has expired")
		}
		url, expires, err := s.store.PresignUntil(ctx, artifact.Key, deadline, ttl)
		if errors.Is(err, exportstore.ErrExpired) {
			return ExportFilePage{}, apierr.New(410, "export download authorization has expired")
		}
		if err != nil {
			return ExportFilePage{}, apierr.New(503, "export download link unavailable")
		}
		result.Files = append(result.Files, ExportFile{Index: artifact.Index, Name: fmt.Sprintf("part-%06d.%s.gz", artifact.Index, job.Format), Rows: artifact.Rows, Bytes: artifact.Bytes, DecodedBytes: artifact.DecodedBytes, SHA256: artifact.SHA256, URL: url, ExpiresAt: expires.Format(time.RFC3339Nano)})
	}
	return result, nil
}
