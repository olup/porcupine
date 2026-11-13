package porcupine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/danielgtaylor/huma/v2"
)

type ExportCreateBody struct {
	IdempotencyKey string            `json:"idempotency_key" minLength:"1" maxLength:"200"`
	SQL            string            `json:"sql" minLength:"1" maxLength:"16384" doc:"One validated SELECT against the project events table, with the same SQL grammar as query-events. The delivery barrier does not create a point-in-time snapshot."`
	Parameters     map[string]string `json:"parameters,omitempty"`
	Format         string            `json:"format,omitempty" enum:"jsonl,csv" default:"jsonl"`
}

type ExportCreateInput struct {
	ID   string `path:"id"`
	Body ExportCreateBody
}

type ExportGetInput struct {
	ID       string `path:"id"`
	ExportID string `path:"export_id"`
}

type ExportJobOutput struct{ Body ExportJob }

type ExportListInput struct {
	ID     string `path:"id"`
	Limit  int    `query:"limit" default:"20" minimum:"1" maximum:"100"`
	Offset int    `query:"offset" default:"0" minimum:"0"`
}

type ExportLimits struct {
	MaxConcurrentJobs       int   `json:"max_concurrent_jobs"`
	MaxQueuedJobs           int   `json:"max_queued_jobs"`
	MaxExecutionSeconds     int   `json:"max_execution_seconds"`
	MaxReadBytes            int64 `json:"max_read_bytes" doc:"Physical scan byte cap. Zero means the selected engine does not enforce this cap."`
	MaxOutputBytes          int64 `json:"max_output_bytes"`
	FileBytes               int64 `json:"file_bytes"`
	MaxRowBytes             int64 `json:"max_row_bytes"`
	MemoryBytes             int64 `json:"memory_bytes" doc:"ClickHouse per-query memory limit, or DuckDB shared per-project engine budget. Neither is a process RSS limit."`
	BandwidthBytesPerSecond int64 `json:"bandwidth_bytes_per_second"`
	TemporaryBytes          int64 `json:"temporary_bytes" doc:"Per-query temporary-disk cap. Zero means the selected engine does not enforce this cap."`
	QueueThreshold          int64 `json:"queue_threshold"`
	RetentionHours          int   `json:"retention_hours"`
}

type ExportList struct {
	Enabled    bool          `json:"enabled"`
	Limits     *ExportLimits `json:"limits,omitempty"`
	Jobs       []ExportJob   `json:"jobs"`
	Limit      int           `json:"limit"`
	Offset     int           `json:"offset"`
	NextOffset *int          `json:"next_offset,omitempty"`
}

type ExportListOutput struct{ Body ExportList }

type ExportFilesInput struct {
	ID       string `path:"id"`
	ExportID string `path:"export_id"`
	Limit    int    `query:"limit" default:"20" minimum:"1" maximum:"100"`
	Offset   int    `query:"offset" default:"0" minimum:"0"`
}

type ExportFiles struct {
	Files      []ExportFile `json:"files"`
	Limit      int          `json:"limit"`
	Offset     int          `json:"offset"`
	NextOffset *int         `json:"next_offset,omitempty"`
}

type ExportFilesOutput struct{ Body ExportFiles }

func (s *Service) registerExports(api huma.API, op func(string, string, string, string, operationAccess, int) huma.Operation) {
	access := operationAccess{Permission: "events:read", ProjectParameter: "id"}
	base := "/api/v1/projects/{id}/exports"
	huma.Register(api, op("create-event-export", "POST", base, "Queue a private streamed SQL export with idempotency and a delivery barrier", access, 202), s.createEventExport)
	huma.Register(api, op("list-event-exports", "GET", base, "List project export jobs and operator resource limits", access, 200), s.listEventExports)
	huma.Register(api, op("get-event-export", "GET", base+"/{export_id}", "Read export status without SQL, storage keys or credentials", access, 200), s.getEventExport)
	huma.Register(api, op("cancel-event-export", "DELETE", base+"/{export_id}", "Request cancellation; restricted to the creator or an instance administrator", access, 202), s.cancelEventExport)
	huma.Register(api, op("list-event-export-files", "GET", base+"/{export_id}/files", "Get a bounded page of fresh five-minute download URLs after full export success", access, 200), s.eventExportFiles)
}

func exportQueueLimit(config ExportConfig) int {
	if config.MaxQueuedJobs <= 0 {
		return 100
	}
	return min(config.MaxQueuedJobs, 100)
}

func (s *Service) createEventExport(ctx context.Context, input *ExportCreateInput) (*ExportJobOutput, error) {
	if err := s.analyticsProject(ctx, input.ID); err != nil {
		return nil, err
	}
	if s.exports == nil {
		return nil, apiError(503, "exports are not configured")
	}
	if strings.TrimSpace(input.Body.IdempotencyKey) == "" || len(input.Body.IdempotencyKey) > 200 {
		return nil, apiError(400, "idempotency_key must contain 1 to 200 characters")
	}
	if err := validateQueryParameters(input.Body.Parameters); err != nil {
		return nil, err
	}
	canonical, err := s.validateExportSQL(ctx, input.ID, input.Body.SQL, input.Body.Parameters)
	if err != nil {
		if s.lake != nil {
			return nil, err
		}
		return nil, apiError(400, "unsupported analytics SQL")
	}
	format := input.Body.Format
	if format == "" {
		format = "jsonl"
	}
	if format != "jsonl" && format != "csv" {
		return nil, apiError(400, "export format must be jsonl or csv")
	}
	parameters := input.Body.Parameters
	if parameters == nil {
		parameters = map[string]string{}
	}
	payload, err := json.Marshal(struct {
		SQL        string            `json:"sql"`
		Parameters map[string]string `json:"parameters"`
		Format     string            `json:"format"`
	}{canonical, parameters, format})
	if err != nil {
		return nil, apiError(400, "invalid export request")
	}
	encodedParameters, _ := json.Marshal(parameters)
	keyHash, requestHash := hash(input.Body.IdempotencyKey), hash(string(payload))
	actorID := principal(ctx).ID
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, apiError(503, "export storage unavailable")
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE metadata SET value=value WHERE key='initialized'`); err != nil {
		return nil, apiError(503, "export storage unavailable")
	}
	var previousID, previousHash string
	err = tx.QueryRowContext(ctx, `SELECT id,request_hash FROM export_jobs WHERE project_id=? AND actor_id=? AND idempotency_hash=?`, input.ID, actorID, keyHash).Scan(&previousID, &previousHash)
	if err == nil {
		if previousHash != requestHash {
			return nil, apiError(409, "idempotency_key was used for a different export request")
		}
		job, err := scanExport(tx.QueryRowContext(ctx, `SELECT `+exportJobColumns+` FROM export_jobs WHERE id=?`, previousID))
		if err != nil {
			return nil, apiError(503, "export storage unavailable")
		}
		return &ExportJobOutput{Body: s.publicExportJob(job)}, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, apiError(503, "export storage unavailable")
	}
	var pending int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM export_jobs WHERE state IN ('queued','running')`).Scan(&pending); err != nil {
		return nil, apiError(503, "export storage unavailable")
	}
	if pending >= exportQueueLimit(s.exports.config) {
		return nil, apiError(503, "export queue is full")
	}
	var eventWatermark, mutationWatermark int64
	if err := tx.QueryRowContext(ctx, `SELECT coalesce((SELECT seq FROM sqlite_sequence WHERE name='events'),0),coalesce((SELECT max(seq) FROM mutation_audit),0)`).Scan(&eventWatermark, &mutationWatermark); err != nil {
		return nil, apiError(503, "export delivery barrier unavailable")
	}
	job := ExportJob{ID: newID(), ProjectID: input.ID, State: ExportQueued, Format: format, CreatedAt: s.clock().UTC().Format(time.RFC3339Nano), Consistency: exportConsistency}
	_, err = tx.ExecContext(ctx, `INSERT INTO export_jobs(id,project_id,actor_id,sql,parameters,format,idempotency_hash,request_hash,state,event_watermark,mutation_watermark,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`, job.ID, job.ProjectID, actorID, canonical, string(encodedParameters), format, keyHash, requestHash, job.State, eventWatermark, mutationWatermark, job.CreatedAt)
	if err != nil || tx.Commit() != nil {
		return nil, apiError(503, "export storage unavailable")
	}
	s.wakeExports()
	return &ExportJobOutput{Body: job}, nil
}

func (s *Service) wakeExports() {
	if s.exports != nil {
		select {
		case s.exports.wake <- struct{}{}:
		default:
		}
	}
}

func (s *Service) publicExportJob(job storedExport) ExportJob {
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

func (s *Service) listEventExports(ctx context.Context, input *ExportListInput) (*ExportListOutput, error) {
	if err := s.analyticsProject(ctx, input.ID); err != nil {
		return nil, err
	}
	if err := validateExportPage(input.Limit, input.Offset); err != nil {
		return nil, err
	}
	jobs, err := s.storedExportPage(ctx, input.ID, input.Limit+1, input.Offset)
	if err != nil {
		return nil, apiError(503, "export storage unavailable")
	}
	result := ExportList{Enabled: s.exports != nil, Jobs: []ExportJob{}, Limit: input.Limit, Offset: input.Offset}
	if len(jobs) > input.Limit {
		next := input.Offset + input.Limit
		result.NextOffset = &next
		jobs = jobs[:input.Limit]
	}
	for _, job := range jobs {
		result.Jobs = append(result.Jobs, s.publicExportJob(job))
	}
	if s.exports != nil {
		config := s.exports.config
		result.Limits = &ExportLimits{MaxConcurrentJobs: 1, MaxQueuedJobs: exportQueueLimit(config), MaxExecutionSeconds: config.MaxExecutionSeconds, MaxReadBytes: config.MaxReadBytes, MaxOutputBytes: config.MaxOutputBytes, FileBytes: config.FileBytes, MaxRowBytes: config.MaxRowBytes, MemoryBytes: config.MemoryBytes, BandwidthBytesPerSecond: config.BandwidthBytesPerSecond, TemporaryBytes: config.TemporaryBytes, QueueThreshold: config.QueueThreshold, RetentionHours: config.RetentionHours}
		if s.lake != nil {
			result.Limits.MaxReadBytes, result.Limits.TemporaryBytes = 0, 0
			result.Limits.MemoryBytes = int64(s.lakeMemoryMB) << 20
		}
	}
	return &ExportListOutput{Body: result}, nil
}

func (s *Service) validateExportSQL(ctx context.Context, project, raw string, parameters map[string]string) (string, error) {
	if s.lake != nil {
		return s.lake.ValidateExport(ctx, project, raw, parameters, false)
	}
	if err := validateSQLParameters(raw, parameters); err != nil {
		return "", err
	}
	return validateSQL(raw)
}

func (s *Service) getEventExport(ctx context.Context, input *ExportGetInput) (*ExportJobOutput, error) {
	job, err := s.loadExport(ctx, input.ID, input.ExportID)
	if err != nil {
		return nil, exportLookupError(err)
	}
	return &ExportJobOutput{Body: s.publicExportJob(job)}, nil
}

func (s *Service) cancelEventExport(ctx context.Context, input *ExportGetInput) (*ExportJobOutput, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, apiError(503, "export storage unavailable")
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE metadata SET value=value WHERE key='initialized'`); err != nil {
		return nil, apiError(503, "export storage unavailable")
	}
	job, err := scanExport(tx.QueryRowContext(ctx, `SELECT `+exportJobColumns+` FROM export_jobs WHERE project_id=? AND id=?`, input.ID, input.ExportID))
	if err != nil {
		return nil, exportLookupError(err)
	}
	caller := principal(ctx)
	if caller.ID != job.ActorID && !caller.admin() {
		return nil, apiError(403, "only the export creator or an administrator can cancel it")
	}
	if job.State != ExportQueued && job.State != ExportRunning {
		return nil, apiError(409, "export is already terminal")
	}
	if job.State == ExportQueued {
		job.State = ExportCanceled
		job.CompletedAt = s.clock().UTC().Format(time.RFC3339Nano)
		_, err = tx.ExecContext(ctx, `UPDATE export_jobs SET state='canceled',cancel_requested=1,cleanup_pending=1,completed_at=?,sql='',parameters='{}' WHERE id=?`, job.CompletedAt, job.ID)
	} else {
		_, err = tx.ExecContext(ctx, `UPDATE export_jobs SET cancel_requested=1,cleanup_pending=1 WHERE id=?`, job.ID)
	}
	if err != nil || tx.Commit() != nil {
		return nil, apiError(503, "export storage unavailable")
	}
	if s.exports != nil {
		s.exports.activeMu.Lock()
		cancel := s.exports.activeCancel
		if s.exports.activeJob != job.ID {
			cancel = nil
		}
		s.exports.activeMu.Unlock()
		if cancel != nil {
			cancel()
		}
	}
	s.wakeExports()
	return &ExportJobOutput{Body: job.ExportJob}, nil
}

func (s *Service) eventExportFiles(ctx context.Context, input *ExportFilesInput) (*ExportFilesOutput, error) {
	if err := validateExportPage(input.Limit, input.Offset); err != nil {
		return nil, err
	}
	job, err := s.loadExport(ctx, input.ID, input.ExportID)
	if err != nil {
		return nil, exportLookupError(err)
	}
	now := s.clock().UTC()
	if job.State == ExportExpired || job.State == ExportSucceeded && job.ExpiresAtMS > 0 && job.ExpiresAtMS <= now.UnixMilli() {
		return nil, apiError(410, "export has expired")
	}
	if job.State != ExportSucceeded {
		return nil, apiError(409, "export files are available only after success")
	}
	if job.ExpiresAtMS <= 0 {
		return nil, apiError(503, "export retention metadata unavailable")
	}
	if s.exports == nil || s.exports.store == nil {
		return nil, apiError(503, "export storage is not configured")
	}
	rows, err := s.db.QueryContext(ctx, `SELECT file_index,object_key,rows,bytes,decoded_bytes,sha256 FROM export_files WHERE job_id=? AND attempt_id=? AND complete=1 ORDER BY file_index LIMIT ? OFFSET ?`, job.ID, job.AttemptID, input.Limit+1, input.Offset)
	if err != nil {
		return nil, apiError(503, "export file metadata unavailable")
	}
	artifacts := []exportArtifact{}
	for rows.Next() {
		var artifact exportArtifact
		if err := rows.Scan(&artifact.Index, &artifact.Key, &artifact.Rows, &artifact.Bytes, &artifact.DecodedBytes, &artifact.SHA256); err != nil {
			rows.Close()
			return nil, apiError(503, "export file metadata unavailable")
		}
		artifacts = append(artifacts, artifact)
	}
	readErr := rows.Err()
	rows.Close()
	if readErr != nil {
		return nil, apiError(503, "export file metadata unavailable")
	}
	result := ExportFiles{Files: []ExportFile{}, Limit: input.Limit, Offset: input.Offset}
	if len(artifacts) > input.Limit {
		next := input.Offset + input.Limit
		result.NextOffset = &next
		artifacts = artifacts[:input.Limit]
	}
	ttl := exportLinkTTL
	if job.ExpiresAtMS > 0 {
		ttl = min(ttl, time.UnixMilli(job.ExpiresAtMS).Sub(now))
	}
	if ttl < time.Second {
		return nil, apiError(410, "export has expired")
	}
	for _, artifact := range artifacts {
		url, err := s.exports.store.Presign(ctx, artifact.Key, ttl)
		if err != nil {
			return nil, apiError(503, "export download link unavailable")
		}
		result.Files = append(result.Files, ExportFile{Index: artifact.Index, Name: fmt.Sprintf("part-%06d.%s.gz", artifact.Index, job.Format), Rows: artifact.Rows, Bytes: artifact.Bytes, DecodedBytes: artifact.DecodedBytes, SHA256: artifact.SHA256, URL: url, ExpiresAt: now.Add(ttl).Format(time.RFC3339Nano)})
	}
	return &ExportFilesOutput{Body: result}, nil
}
