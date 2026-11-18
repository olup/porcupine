package output

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/olup/porcupine/internal/apierr"
	"github.com/olup/porcupine/internal/auth"
)

const exportConsistency = "delivery_barrier_nontransactional"

const exportJobPrefix = `id,project_id,actor_id,sql,parameters,format,state,attempt_id,`
const exportJobSuffix = `,cancel_requested,rows,bytes,decoded_bytes,file_count,attempts,created_at,started_at,completed_at,expires_at,expires_at_ms,error,columns_json,cleanup_pending,
coalesce((SELECT tenant_id FROM export_external_authority a WHERE a.job_id=export_jobs.id),''),
coalesce((SELECT issuer FROM export_external_authority a WHERE a.job_id=export_jobs.id),''),
coalesce((SELECT expires_at_ms FROM export_external_authority a WHERE a.job_id=export_jobs.id),0)`

type exportScanner interface{ Scan(...any) error }

func (s *ExportJobs) exportColumns() string {
	windowColumns := "event_watermark,mutation_watermark"
	if s.retained != nil {
		windowColumns = "snapshot_catalog_id,snapshot_version"
	}
	return exportJobPrefix + windowColumns + exportJobSuffix
}

func (s *ExportJobs) scanExport(row exportScanner) (storedExport, error) {
	var job storedExport
	var parameters, columns string
	var first, second any = &job.EventWatermark, &job.MutationWatermark
	job.Consistency = exportConsistency
	if s.retained != nil {
		first, second = &job.Snapshot.CatalogID, &job.Snapshot.Version
		job.Consistency = retainedExportConsistency
	}
	err := row.Scan(&job.ID, &job.ProjectID, &job.ActorID, &job.SQL, &parameters, &job.Format, &job.State, &job.AttemptID, first, second, &job.CancelRequested, &job.Rows, &job.Bytes, &job.DecodedBytes, &job.FileCount, &job.Attempts, &job.CreatedAt, &job.StartedAt, &job.CompletedAt, &job.ExpiresAt, &job.ExpiresAtMS, &job.Error, &columns, &job.CleanupPending, &job.ExternalTenantID, &job.ExternalIssuer, &job.ExternalExpiresAtMS)
	if err != nil {
		return job, err
	}
	if json.Unmarshal([]byte(parameters), &job.Parameters) != nil || json.Unmarshal([]byte(columns), &job.Columns) != nil {
		return storedExport{}, errors.New("invalid stored export metadata")
	}
	return job, nil
}

func (s *ExportJobs) loadExport(ctx context.Context, projectID, jobID string) (storedExport, error) {
	return s.scanExport(s.db.QueryRowContext(ctx, `SELECT `+s.exportColumns()+` FROM export_jobs WHERE project_id=$1 AND id=$2`, projectID, jobID))
}

func (s *ExportJobs) exportJobActor(ctx context.Context, job storedExport) (auth.Principal, error) {
	if job.ExternalExpiresAtMS == 0 {
		return s.loadActor(ctx, job.ActorID)
	}
	if s.issuer == "" || job.ExternalExpiresAtMS <= s.clock().UnixMilli() {
		return auth.Principal{}, errExportAuthorizationExpired
	}
	if job.ExternalIssuer != s.issuer {
		return auth.Principal{}, ErrExportRevoked
	}
	if err := s.requireActive(ctx, job.ProjectID); err != nil {
		return auth.Principal{}, err
	}
	return exportExternalActor(ctx, s.control, job)
}

func exportExternalActor(ctx context.Context, db exportQueryer, job storedExport) (auth.Principal, error) {
	var tenant string
	if err := db.QueryRowContext(ctx, `SELECT tenant_id FROM projects WHERE id=$1`, job.ProjectID).Scan(&tenant); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return auth.Principal{}, ErrExportRevoked
		}
		return auth.Principal{}, auth.ErrUnavailable
	}
	if tenant != job.ExternalTenantID {
		return auth.Principal{}, ErrExportRevoked
	}
	return auth.Principal{ID: job.ActorID, TenantIDs: []string{tenant}, Projects: []string{job.ProjectID}, Permissions: []string{"events:read"}}, nil
}

func (s *ExportJobs) exportArtifacts(ctx context.Context, jobID, attemptID string) ([]exportArtifact, error) {
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

func (s *ExportJobs) storedExportPage(ctx context.Context, project string, limit, offset int) ([]storedExport, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+s.exportColumns()+` FROM export_jobs WHERE project_id=$1 ORDER BY `+s.exportOrder()+` DESC LIMIT $2 OFFSET $3`, project, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	jobs := []storedExport{}
	for rows.Next() {
		job, err := s.scanExport(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

func exportLookupError(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return apierr.New(404, "export not found")
	}
	return apierr.New(503, "export storage unavailable")
}

func (s *ExportJobs) exportJobActorTx(ctx context.Context, tx *sql.Tx, job storedExport) (auth.Principal, error) {
	if s.retained == nil {
		return s.exportJobActor(ctx, job)
	}
	if job.ExternalExpiresAtMS > 0 {
		if s.issuer == "" || job.ExternalExpiresAtMS <= s.clock().UnixMilli() {
			return auth.Principal{}, errExportAuthorizationExpired
		}
		if job.ExternalIssuer != s.issuer {
			return auth.Principal{}, ErrExportRevoked
		}
		return exportExternalActor(ctx, tx, job)
	}
	var id string
	err := tx.QueryRowContext(ctx, `SELECT id FROM access_keys WHERE id=$1 AND revoked_at IS NULL FOR UPDATE`, job.ActorID).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return auth.Principal{}, ErrExportRevoked
	}
	if err != nil {
		return auth.Principal{}, auth.ErrUnavailable
	}
	return s.loadActorTx(ctx, tx, job.ActorID)
}
