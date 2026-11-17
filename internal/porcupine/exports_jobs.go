package porcupine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/olup/porcupine/internal/auth"
)

const exportConsistency = "delivery_barrier_nontransactional"

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

func (s *Service) exportActor(ctx context.Context, keyID string) (auth.Principal, error) {
	return s.loadCredentialActor(ctx, keyID)
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
