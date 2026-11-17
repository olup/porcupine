package output

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/olup/porcupine/internal/auth"
	eventapi "github.com/olup/porcupine/internal/event"
	"github.com/olup/porcupine/internal/exportstore"
)

func exportQueueLimit(config ExportConfig) int {
	if config.MaxQueuedJobs <= 0 {
		return 100
	}
	return min(config.MaxQueuedJobs, 100)
}

func (s *ExportJobs) Submit(ctx context.Context, project string, request ExportRequest, caller auth.Principal) (ExportJob, error) {
	if !s.Enabled() {
		return ExportJob{}, apiError(503, "exports are not configured")
	}
	canonical := request.SQL
	format := request.Format
	if format == "" {
		format = "jsonl"
	}
	if format != "jsonl" && format != "csv" {
		return ExportJob{}, apiError(400, "export format must be jsonl or csv")
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
		return ExportJob{}, apiError(400, "invalid export request")
	}
	encodedParameters, _ := json.Marshal(parameters)
	keyHash, requestHash := hash(request.IdempotencyKey), hash(string(payload))
	actorID := caller.ID
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ExportJob{}, apiError(503, "export storage unavailable")
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE metadata SET value=value WHERE key='initialized'`); err != nil {
		return ExportJob{}, apiError(503, "export storage unavailable")
	}
	var previousID, previousHash string
	err = tx.QueryRowContext(ctx, `SELECT id,request_hash FROM export_jobs WHERE project_id=$1 AND actor_id=$2 AND idempotency_hash=$3`, project, actorID, keyHash).Scan(&previousID, &previousHash)
	if err == nil {
		if previousHash != requestHash {
			return ExportJob{}, apiError(409, "idempotency_key was used for a different export request")
		}
		job, err := scanExport(tx.QueryRowContext(ctx, `SELECT `+exportJobColumns+` FROM export_jobs WHERE id=$1`, previousID))
		if err != nil {
			return ExportJob{}, apiError(503, "export storage unavailable")
		}
		return s.publicExportJob(job), nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return ExportJob{}, apiError(503, "export storage unavailable")
	}
	var pending int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM export_jobs WHERE state IN ('queued','running')`).Scan(&pending); err != nil {
		return ExportJob{}, apiError(503, "export storage unavailable")
	}
	if pending >= exportQueueLimit(s.config) {
		return ExportJob{}, apiError(503, "export queue is full")
	}
	var eventWatermark, mutationWatermark int64
	if err := tx.QueryRowContext(ctx, `SELECT coalesce((SELECT seq FROM sqlite_sequence WHERE name='events'),0),coalesce((SELECT max(seq) FROM mutation_audit),0)`).Scan(&eventWatermark, &mutationWatermark); err != nil {
		return ExportJob{}, apiError(503, "export delivery barrier unavailable")
	}
	job := ExportJob{ID: eventapi.NewID(), ProjectID: project, State: ExportQueued, Format: format, CreatedAt: s.clock().UTC().Format(time.RFC3339Nano), Consistency: exportConsistency}
	_, err = tx.ExecContext(ctx, `INSERT INTO export_jobs(id,project_id,actor_id,sql,parameters,format,idempotency_hash,request_hash,state,event_watermark,mutation_watermark,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`, job.ID, job.ProjectID, actorID, canonical, string(encodedParameters), format, keyHash, requestHash, job.State, eventWatermark, mutationWatermark, job.CreatedAt)
	if err == nil && caller.External != nil {
		actor, authorityErr := s.currentActor(ctx, caller)
		if authenticationUnavailable(authorityErr) {
			return ExportJob{}, apiError(503, "export authorization unavailable")
		}
		if authorityErr != nil || !auth.Authorize(actor, "events:read", job.ProjectID) {
			return ExportJob{}, apiError(403, "external export authorization expired or changed")
		}
		var tenant string
		if err = s.control.QueryRowContext(ctx, `SELECT tenant_id FROM projects WHERE id=$1`, job.ProjectID).Scan(&tenant); err == nil {
			_, err = tx.ExecContext(ctx, `INSERT INTO export_external_authority(job_id,tenant_id,issuer,expires_at_ms) VALUES($1,$2,$3,$4)`, job.ID, tenant, actor.External.Issuer(), actor.External.Expires().UnixMilli())
		}
	}
	if err != nil || tx.Commit() != nil {
		return ExportJob{}, apiError(503, "export storage unavailable")
	}
	s.Wake()
	return job, nil
}

func (s *ExportJobs) Wake() {
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
		return apiError(400, "export pagination requires limit 1 to 100 and a nonnegative offset")
	}
	return nil
}

func (s *ExportJobs) List(ctx context.Context, project string, limit, offset int) (ExportPage, error) {
	if err := validateExportPage(limit, offset); err != nil {
		return ExportPage{}, err
	}
	jobs, err := s.storedExportPage(ctx, project, limit+1, offset)
	if err != nil {
		return ExportPage{}, apiError(503, "export storage unavailable")
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
		return ExportJob{}, apiError(503, "export storage unavailable")
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE metadata SET value=value WHERE key='initialized'`); err != nil {
		return ExportJob{}, apiError(503, "export storage unavailable")
	}
	job, err := scanExport(tx.QueryRowContext(ctx, `SELECT `+exportJobColumns+` FROM export_jobs WHERE project_id=$1 AND id=$2`, project, id))
	if err != nil {
		return ExportJob{}, exportLookupError(err)
	}
	if caller.ID != job.ActorID && !caller.Admin() {
		return ExportJob{}, apiError(403, "only the export creator or an administrator can cancel it")
	}
	if job.State != ExportQueued && job.State != ExportRunning {
		return ExportJob{}, apiError(409, "export is already terminal")
	}
	if job.State == ExportQueued {
		job.State = ExportCanceled
		job.CompletedAt = s.clock().UTC().Format(time.RFC3339Nano)
		_, err = tx.ExecContext(ctx, `UPDATE export_jobs SET state='canceled',cancel_requested=1,cleanup_pending=1,completed_at=$1,sql='',parameters='{}' WHERE id=$2`, job.CompletedAt, job.ID)
	} else {
		_, err = tx.ExecContext(ctx, `UPDATE export_jobs SET cancel_requested=1,cleanup_pending=1 WHERE id=$1`, job.ID)
	}
	if err != nil || tx.Commit() != nil {
		return ExportJob{}, apiError(503, "export storage unavailable")
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
	s.Wake()
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
		return ExportFilePage{}, apiError(410, "export has expired")
	}
	if job.State != ExportSucceeded {
		return ExportFilePage{}, apiError(409, "export files are available only after success")
	}
	if job.ExpiresAtMS <= 0 {
		return ExportFilePage{}, apiError(503, "export retention metadata unavailable")
	}
	if !s.Enabled() || s.store == nil {
		return ExportFilePage{}, apiError(503, "export storage is not configured")
	}
	rows, err := s.db.QueryContext(ctx, `SELECT file_index,object_key,rows,bytes,decoded_bytes,sha256 FROM export_files WHERE job_id=$1 AND attempt_id=$2 AND complete=1 ORDER BY file_index LIMIT $3 OFFSET $4`, job.ID, job.AttemptID, limit+1, offset)
	if err != nil {
		return ExportFilePage{}, apiError(503, "export file metadata unavailable")
	}
	artifacts := []exportArtifact{}
	for rows.Next() {
		var artifact exportArtifact
		if err := rows.Scan(&artifact.Index, &artifact.Key, &artifact.Rows, &artifact.Bytes, &artifact.DecodedBytes, &artifact.SHA256); err != nil {
			rows.Close()
			return ExportFilePage{}, apiError(503, "export file metadata unavailable")
		}
		artifacts = append(artifacts, artifact)
	}
	readErr := rows.Err()
	rows.Close()
	if readErr != nil {
		return ExportFilePage{}, apiError(503, "export file metadata unavailable")
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
			return ExportFilePage{}, apiError(410, "export download authorization has expired")
		}
		url, expires, err := s.store.PresignUntil(ctx, artifact.Key, deadline, ttl)
		if errors.Is(err, exportstore.ErrExpired) {
			return ExportFilePage{}, apiError(410, "export download authorization has expired")
		}
		if err != nil {
			return ExportFilePage{}, apiError(503, "export download link unavailable")
		}
		result.Files = append(result.Files, ExportFile{Index: artifact.Index, Name: fmt.Sprintf("part-%06d.%s.gz", artifact.Index, job.Format), Rows: artifact.Rows, Bytes: artifact.Bytes, DecodedBytes: artifact.DecodedBytes, SHA256: artifact.SHA256, URL: url, ExpiresAt: expires.Format(time.RFC3339Nano)})
	}
	return result, nil
}
