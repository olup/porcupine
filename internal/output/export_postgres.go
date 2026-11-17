package output

import (
	"context"
	"database/sql"
	"errors"
	"time"

	eventapi "github.com/olup/porcupine/internal/event"
	lakeapi "github.com/olup/porcupine/internal/lake"
)

// ExportPostgresSchemaV1 is installed by the numbered shared control migration.
const ExportPostgresSchemaV1 = `
CREATE TABLE export_jobs (
 id TEXT PRIMARY KEY, project_id TEXT NOT NULL REFERENCES projects(id), actor_id TEXT NOT NULL,
 sql TEXT NOT NULL, parameters TEXT NOT NULL, format TEXT NOT NULL CHECK(format IN ('jsonl','csv')),
 idempotency_hash TEXT NOT NULL, request_hash TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('queued','running','succeeded','failed','canceled','expired')),
 attempt_id TEXT NOT NULL DEFAULT '', lease_until TIMESTAMPTZ,
 snapshot_catalog_id TEXT NOT NULL, snapshot_version BIGINT NOT NULL CHECK(snapshot_version>=0),
 cancel_requested INTEGER NOT NULL DEFAULT 0 CHECK(cancel_requested IN (0,1)),
 rows BIGINT NOT NULL DEFAULT 0, bytes BIGINT NOT NULL DEFAULT 0, decoded_bytes BIGINT NOT NULL DEFAULT 0,
 file_count INTEGER NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0,
 created_at TEXT NOT NULL, started_at TEXT NOT NULL DEFAULT '', completed_at TEXT NOT NULL DEFAULT '',
 expires_at TEXT NOT NULL DEFAULT '', expires_at_ms BIGINT NOT NULL DEFAULT 0,
 error TEXT NOT NULL DEFAULT '', columns_json TEXT NOT NULL DEFAULT '[]', retire_after TIMESTAMPTZ,
 cleanup_pending INTEGER NOT NULL DEFAULT 0 CHECK(cleanup_pending IN (0,1)),
 ordinal BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
 UNIQUE(project_id,actor_id,idempotency_hash));
CREATE INDEX export_jobs_project_page ON export_jobs(project_id,ordinal DESC);
CREATE INDEX export_jobs_active ON export_jobs(state) WHERE state IN ('queued','running');
CREATE INDEX export_jobs_pending ON export_jobs(ordinal) WHERE state='queued' AND cleanup_pending=0;
CREATE INDEX export_jobs_retirement ON export_jobs(retire_after) WHERE retire_after IS NOT NULL;
CREATE INDEX export_jobs_lease ON export_jobs(lease_until) WHERE state='running';
CREATE INDEX export_jobs_expiry ON export_jobs(expires_at_ms) WHERE state='succeeded';
CREATE INDEX export_jobs_cleanup ON export_jobs(ordinal) WHERE cleanup_pending=1 AND state<>'running';
CREATE TABLE export_files (
 job_id TEXT NOT NULL REFERENCES export_jobs(id) ON DELETE CASCADE,
 attempt_id TEXT NOT NULL, file_index INTEGER NOT NULL,
 object_key TEXT NOT NULL, storage_id TEXT NOT NULL, upload_id TEXT NOT NULL DEFAULT '',
 complete INTEGER NOT NULL DEFAULT 0 CHECK(complete IN (0,1)),
 rows BIGINT NOT NULL DEFAULT 0, bytes BIGINT NOT NULL DEFAULT 0, decoded_bytes BIGINT NOT NULL DEFAULT 0,
 sha256 TEXT NOT NULL DEFAULT '', PRIMARY KEY(job_id,attempt_id,file_index));
CREATE TABLE export_external_authority (
 job_id TEXT PRIMARY KEY REFERENCES export_jobs(id) ON DELETE CASCADE,
 tenant_id TEXT NOT NULL, issuer TEXT NOT NULL, expires_at_ms BIGINT NOT NULL CHECK(expires_at_ms>0));
CREATE INDEX export_external_authority_expiry ON export_external_authority(expires_at_ms);
CREATE TABLE project_deletion_files (
 project_id TEXT NOT NULL, object_key TEXT NOT NULL, upload_id TEXT NOT NULL, storage_id TEXT NOT NULL,
 PRIMARY KEY(project_id,object_key,upload_id));`

const retainedExportConsistency = "retained_analytical_snapshot"

// ExportControlTx commits admission using the physical connection that holds
// the project write lock.
type ExportControlTx func(context.Context, func(context.Context, *sql.Tx) error) error

// ExportSnapshotCut holds the project's mutation write session while capture
// and durable admission run, after the accepted mutation prefix is settled.
type ExportSnapshotCut func(context.Context, string, func(context.Context, ExportControlTx) error) error

type RetainedExportReader interface {
	CaptureExportSnapshot(context.Context, string) (lakeapi.ExportSnapshot, error)
	ExportAtSnapshot(context.Context, string, string, map[string]string, lakeapi.ExportSnapshot, int64, lakeapi.ExportStreamCallbacks) (lakeapi.ExportStreamResult, error)
}

// NewPostgresExports keeps job ownership and accounting in shared control. The
// journal remains available to the separate, journal-backed deployment mode.
func NewPostgresExports(d ExportDependencies, reader RetainedExportReader, config *ExportConfig) (*ExportJobs, error) {
	if d.Control == nil || reader == nil || d.SnapshotCut == nil ||
		d.BeginAttemptTx == nil || d.EndAttempt == nil || d.BeginActivity == nil || d.Acquire == nil ||
		d.LoadActor == nil || d.LoadActorTx == nil || d.CurrentActorTx == nil || d.RequireActive == nil {
		return nil, errors.New("shared exports require PostgreSQL control, activity tracking, mutation readiness, authority and retained analytical snapshots")
	}
	return newExports(d, d.Control, reader, config)
}

const exportLeaseExtension = `clock_timestamp()+interval '15 seconds'`
const exportLiveLease = ` AND lease_until>clock_timestamp()`

func (s *ExportJobs) leaseGuard() string {
	if s.retained != nil {
		return exportLiveLease
	}
	return ""
}

func (s *ExportJobs) exportOrder() string {
	if s.retained != nil {
		return "ordinal"
	}
	return "rowid"
}

func (s *ExportJobs) lockExportQueue(ctx context.Context, tx *sql.Tx) error {
	q := `UPDATE metadata SET value=value WHERE key='initialized'`
	if s.retained != nil {
		q = `SELECT pg_advisory_xact_lock(689648748537109974)`
	}
	_, err := tx.ExecContext(ctx, q)
	return err
}

func (s *ExportJobs) lockExportJob() string {
	if s.retained != nil {
		return " FOR UPDATE"
	}
	return ""
}

func (s *ExportJobs) reclaimExports(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `UPDATE export_jobs
SET state='queued',lease_until=NULL,cleanup_pending=1,error='interrupted'
WHERE id IN (SELECT id FROM export_jobs
WHERE state='running' AND lease_until<=clock_timestamp()
ORDER BY lease_until LIMIT 32 FOR UPDATE SKIP LOCKED)`)
	return err
}

func (s *ExportJobs) renewExport(ctx context.Context, job storedExport) error {
	result, err := s.db.ExecContext(ctx, `UPDATE export_jobs SET lease_until=`+exportLeaseExtension+`
WHERE id=$1 AND attempt_id=$2 AND state='running' AND cancel_requested=0`+exportLiveLease, job.ID, job.AttemptID)
	return verifyExportMutation(result, err)
}

func verifyExportMutation(result sql.Result, err error) error {
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return errExportCanceled
	}
	return nil
}

func (s *ExportJobs) lockRunningExport(ctx context.Context, tx *sql.Tx, job storedExport) error {
	var id string
	err := tx.QueryRowContext(ctx, `SELECT id FROM export_jobs
WHERE id=$1 AND attempt_id=$2 AND state='running' AND cancel_requested=0`+exportLiveLease+` FOR UPDATE`, job.ID, job.AttemptID).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return errExportCanceled
	}
	return err
}

type exportExecer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func (s *ExportJobs) withExportAttempt(ctx context.Context, job storedExport, run func(exportExecer) error) error {
	if s.retained == nil {
		return run(s.db)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = s.lockRunningExport(ctx, tx, job); err != nil {
		return err
	}
	if err = run(tx); err != nil {
		return err
	}
	// This transaction excludes a reclaimer for the duration of the
	// upload checkpoint. Refresh before giving up its row lock.
	if _, err = tx.ExecContext(ctx, `UPDATE export_jobs SET lease_until=`+exportLeaseExtension+` WHERE id=$1`, job.ID); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *ExportJobs) finishExportActivity(project, attempt string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = s.endAttempt(ctx, project, attempt)
}

func lockActiveExportProject(ctx context.Context, tx *sql.Tx, project string) (string, error) {
	var tenant string
	// Follow the shared query admission and deletion lock order. Recheck
	// tombstones in a fresh statement after a concurrent deleter commits.
	err := tx.QueryRowContext(ctx, `SELECT t.id FROM tenants t JOIN projects p ON p.tenant_id=t.id WHERE p.id=$1 FOR UPDATE OF t,p`, project).Scan(&tenant)
	if err != nil {
		return "", err
	}
	var deleting bool
	err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM project_deletions WHERE project_id=$1) OR EXISTS(SELECT 1 FROM tenant_deletions WHERE tenant_id=$2)`, project, tenant).Scan(&deleting)
	if deleting {
		return "", sql.ErrNoRows
	}
	return tenant, err
}

func (s *ExportJobs) beginExportAdmissionActivity(ctx context.Context, project string) (func(), error) {
	attempt := eventapi.NewID()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if err = s.beginAttemptTx(ctx, tx, project, attempt); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		s.finishExportActivity(project, attempt)
		return nil, err
	}
	return func() { s.finishExportActivity(project, attempt) }, nil
}

const (
	maxHostedExportJobs          = 10000
	maxHostedTenantExportJobs    = 2000
	maxHostedProjectExportJobs   = 1000
	maxHostedOwnedExports        = 1000
	maxHostedTenantOwnedExports  = 400
	maxHostedProjectOwnedExports = 100
)

func (s *ExportJobs) checkHostedExportCapacity(ctx context.Context, tx *sql.Tx, tenant, project string) error {
	var pending, total, tenantJobs, projectJobs, owned, tenantOwned, projectOwned int
	err := tx.QueryRowContext(ctx, `WITH jobs AS (
 SELECT j.state,j.project_id,p.tenant_id,
 j.state IN ('queued','running','succeeded') OR j.cleanup_pending=1 AS owns_artifacts
 FROM export_jobs j JOIN projects p ON p.id=j.project_id
)
SELECT count(*) FILTER (WHERE state IN ('queued','running')),
 count(*),count(*) FILTER (WHERE tenant_id=$1),count(*) FILTER (WHERE project_id=$2),
 count(*) FILTER (WHERE owns_artifacts),
 count(*) FILTER (WHERE owns_artifacts AND tenant_id=$1),
 count(*) FILTER (WHERE owns_artifacts AND project_id=$2)
FROM jobs`, tenant, project).Scan(&pending, &total, &tenantJobs, &projectJobs, &owned, &tenantOwned, &projectOwned)
	if err != nil {
		return apiError(503, "export storage unavailable")
	}
	if pending >= exportQueueLimit(s.config) {
		return apiError(503, "export queue is full")
	}
	if total >= maxHostedExportJobs || tenantJobs >= maxHostedTenantExportJobs || projectJobs >= maxHostedProjectExportJobs ||
		owned >= maxHostedOwnedExports || tenantOwned >= maxHostedTenantOwnedExports || projectOwned >= maxHostedProjectOwnedExports {
		return apiError(503, "retained export capacity is full")
	}
	return nil
}

func (s *ExportJobs) retireExports(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM export_jobs WHERE id IN (
 SELECT j.id FROM export_jobs j
 WHERE retire_after<=clock_timestamp() AND state IN ('failed','canceled','expired') AND cleanup_pending=0
 AND NOT EXISTS(SELECT 1 FROM export_files f WHERE f.job_id=j.id)
 ORDER BY retire_after LIMIT 32 FOR UPDATE SKIP LOCKED)`)
	return err
}
