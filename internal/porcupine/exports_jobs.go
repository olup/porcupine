package porcupine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
)

const exportConsistency = "delivery_barrier_nontransactional"

func (s *Service) initializeExports(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS export_jobs (
id TEXT PRIMARY KEY, project_id TEXT NOT NULL REFERENCES projects(id), actor_id TEXT NOT NULL REFERENCES access_keys(id),
sql TEXT NOT NULL, parameters TEXT NOT NULL, format TEXT NOT NULL,
idempotency_hash TEXT NOT NULL, request_hash TEXT NOT NULL, state TEXT NOT NULL,
attempt_id TEXT NOT NULL DEFAULT '',
event_watermark INTEGER NOT NULL, mutation_watermark INTEGER NOT NULL,
cancel_requested INTEGER NOT NULL DEFAULT 0,
rows INTEGER NOT NULL DEFAULT 0, bytes INTEGER NOT NULL DEFAULT 0, decoded_bytes INTEGER NOT NULL DEFAULT 0,
file_count INTEGER NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0,
created_at TEXT NOT NULL, started_at TEXT NOT NULL DEFAULT '', completed_at TEXT NOT NULL DEFAULT '',
expires_at TEXT NOT NULL DEFAULT '', expires_at_ms INTEGER NOT NULL DEFAULT 0,
error TEXT NOT NULL DEFAULT '', columns_json TEXT NOT NULL DEFAULT '[]', cleanup_pending INTEGER NOT NULL DEFAULT 0,
UNIQUE(project_id,actor_id,idempotency_hash));
CREATE INDEX IF NOT EXISTS export_jobs_pending ON export_jobs(state,cleanup_pending);
CREATE INDEX IF NOT EXISTS export_jobs_expiration ON export_jobs(state,expires_at_ms);
CREATE TABLE IF NOT EXISTS export_files (
job_id TEXT NOT NULL REFERENCES export_jobs(id), attempt_id TEXT NOT NULL, file_index INTEGER NOT NULL,
object_key TEXT NOT NULL, upload_id TEXT NOT NULL DEFAULT '', complete INTEGER NOT NULL DEFAULT 0,
rows INTEGER NOT NULL DEFAULT 0, bytes INTEGER NOT NULL DEFAULT 0, decoded_bytes INTEGER NOT NULL DEFAULT 0,
sha256 TEXT NOT NULL DEFAULT '', PRIMARY KEY(job_id,attempt_id,file_index)) WITHOUT ROWID;`)
	return err
}

const exportJobColumns = `id,project_id,actor_id,sql,parameters,format,state,attempt_id,event_watermark,mutation_watermark,cancel_requested,rows,bytes,decoded_bytes,file_count,attempts,created_at,started_at,completed_at,expires_at,expires_at_ms,error,columns_json,cleanup_pending`

type exportScanner interface{ Scan(...any) error }

func scanExport(row exportScanner) (storedExport, error) {
	var job storedExport
	var parameters, columns string
	err := row.Scan(&job.ID, &job.ProjectID, &job.ActorID, &job.SQL, &parameters, &job.Format, &job.State, &job.AttemptID, &job.EventWatermark, &job.MutationWatermark, &job.CancelRequested, &job.Rows, &job.Bytes, &job.DecodedBytes, &job.FileCount, &job.Attempts, &job.CreatedAt, &job.StartedAt, &job.CompletedAt, &job.ExpiresAt, &job.ExpiresAtMS, &job.Error, &columns, &job.CleanupPending)
	if err != nil {
		return job, err
	}
	if json.Unmarshal([]byte(parameters), &job.Parameters) != nil || json.Unmarshal([]byte(columns), &job.Columns) != nil {
		return storedExport{}, errors.New("invalid stored export metadata")
	}
	job.Consistency = exportConsistency
	return job, nil
}

func (s *Service) loadExport(ctx context.Context, projectID, jobID string) (storedExport, error) {
	return scanExport(s.db.QueryRowContext(ctx, `SELECT `+exportJobColumns+` FROM export_jobs WHERE project_id=$1 AND id=$2`, projectID, jobID))
}

func (s *Service) exportActor(ctx context.Context, keyID string) (actor, error) {
	return s.exportActorFrom(ctx, s.control, keyID)
}

func (s *Service) exportActorFrom(ctx context.Context, query oauthQueryer, keyID string) (actor, error) {
	return s.loadCredentialActor(ctx, query, keyID)
}

func (s *Service) exportArtifacts(ctx context.Context, jobID, attemptID string) ([]exportArtifact, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT file_index,object_key,upload_id,complete,rows,bytes,decoded_bytes,sha256 FROM export_files WHERE job_id=$1 AND attempt_id=$2 ORDER BY file_index`, jobID, attemptID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	artifacts := []exportArtifact{}
	for rows.Next() {
		var artifact exportArtifact
		if err := rows.Scan(&artifact.Index, &artifact.Key, &artifact.UploadID, &artifact.Complete, &artifact.Rows, &artifact.Bytes, &artifact.DecodedBytes, &artifact.SHA256); err != nil {
			return nil, err
		}
		artifacts = append(artifacts, artifact)
	}
	return artifacts, rows.Err()
}

func (s *Service) storedExportPage(ctx context.Context, project string, limit, offset int) ([]storedExport, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+exportJobColumns+` FROM export_jobs WHERE project_id=$1 ORDER BY rowid DESC LIMIT $2 OFFSET $3`, project, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	jobs := []storedExport{}
	for rows.Next() {
		job, err := scanExport(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

func exportLookupError(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return apiError(404, "export not found")
	}
	return apiError(503, "export storage unavailable")
}
