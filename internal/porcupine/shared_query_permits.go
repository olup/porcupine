package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Expiry is a recovery bound, not a renewable admission lease: it must remain
// valid while a canceled native query finishes its in-flight object-store I/O.
const sharedQueryPermitSafetyMargin = 30 * time.Second
const sharedQueryWatchdogSafetyMargin = 15 * time.Second
const sharedQueryWatchdogExitCode = 70
const sharedQueryWatchdogLogGrace = time.Second

func sharedQueryIOGrace(s3RequestTimeoutSeconds, metadataRequestTimeoutSeconds int) time.Duration {
	return time.Duration(max(0, s3RequestTimeoutSeconds)+max(0, metadataRequestTimeoutSeconds)) * time.Second
}

func sharedQueryPermitLifetime(limits InteractiveQueryConfig, s3RequestTimeoutSeconds, metadataRequestTimeoutSeconds int) time.Duration {
	return time.Duration(limits.TimeoutMS)*time.Millisecond + sharedQueryIOGrace(s3RequestTimeoutSeconds, metadataRequestTimeoutSeconds) + sharedQueryPermitSafetyMargin
}

func sharedQueryWatchdogDeadline(hardDeadline time.Time, s3RequestTimeoutSeconds, metadataRequestTimeoutSeconds int) time.Time {
	return hardDeadline.Add(sharedQueryIOGrace(s3RequestTimeoutSeconds, metadataRequestTimeoutSeconds) + sharedQueryWatchdogSafetyMargin)
}

// The process must stop before another worker can recycle its permit, even if
// native blocking I/O ignores cancellation. A returned handler alone cannot
// establish that native work stopped; only its actual scope release disarms it.
func armSharedQueryWatchdog(hardDeadline time.Time, s3RequestTimeoutSeconds, metadataRequestTimeoutSeconds int, clock func() time.Time) func() {
	deadline := sharedQueryWatchdogDeadline(hardDeadline, s3RequestTimeoutSeconds, metadataRequestTimeoutSeconds)
	timer := time.NewTimer(max(0, deadline.Sub(clock())))
	done := make(chan struct{})
	var state atomic.Uint32
	go func() {
		defer timer.Stop()
		select {
		case <-done:
			return
		case <-timer.C:
		}
		if state.CompareAndSwap(0, 2) {
			// A blocked log sink must not prevent the process from fencing native work.
			logged := make(chan struct{})
			go func() {
				slog.Error("shared analytics execution did not stop before its safety horizon; terminating worker")
				close(logged)
			}()
			select {
			case <-logged:
			case <-time.After(sharedQueryWatchdogLogGrace):
			}
			os.Exit(sharedQueryWatchdogExitCode)
		}
	}()
	return func() {
		if state.CompareAndSwap(0, 1) {
			close(done)
		}
	}
}

func (s *Service) initializeSharedQueryPermits(ctx context.Context) error {
	tx, err := s.control.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(22591942986388057)`); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS shared_query_permits (
  permit_id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  worker_id TEXT NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL
 );
 CREATE INDEX IF NOT EXISTS shared_query_permits_tenant_expiry ON shared_query_permits(tenant_id,expires_at);
 CREATE INDEX IF NOT EXISTS shared_query_permits_project_expiry ON shared_query_permits(project_id,expires_at)`)
	if err != nil {
		return err
	}
	return tx.Commit()
}

// acquireSharedQueryPermit rechecks policy while holding its authoritative tenant
// row. Every process participates in the same transaction; local worker slots
// remain a separate protection for each process's own native engines.
func (s *Service) acquireSharedQueryPermit(ctx context.Context, project, tenantID string) (InteractiveQueryConfig, func(), error) {
	tx, err := s.control.BeginTx(ctx, nil)
	if err != nil {
		return InteractiveQueryConfig{}, nil, sharedQueryPermitError(ctx, err)
	}
	defer tx.Rollback()
	columns := strings.Split(tenantColumns, ",")
	for i := range columns {
		columns[i] = "t." + columns[i]
	}
	tenant, err := scanTenant(tx.QueryRowContext(ctx, `SELECT `+strings.Join(columns, ",")+` FROM tenants t JOIN projects p ON p.tenant_id=t.id WHERE t.id=$1 AND p.id=$2 AND NOT EXISTS(SELECT 1 FROM project_deletions d WHERE d.project_id=p.id) FOR UPDATE OF t,p`, tenantID, project))
	if errors.Is(err, sql.ErrNoRows) {
		return InteractiveQueryConfig{}, nil, apiError(404, "project not found")
	}
	if err != nil {
		return InteractiveQueryConfig{}, nil, sharedQueryPermitError(ctx, err)
	}
	limits := effectiveInteractiveQueries(s.interactiveConfig, tenant.Budgets)
	if _, err = tx.ExecContext(ctx, `DELETE FROM shared_query_permits WHERE tenant_id=$1 AND expires_at<=clock_timestamp()`, tenantID); err != nil {
		return InteractiveQueryConfig{}, nil, sharedQueryPermitError(ctx, err)
	}
	var active int
	if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM shared_query_permits WHERE tenant_id=$1`, tenantID).Scan(&active); err != nil {
		return InteractiveQueryConfig{}, nil, sharedQueryPermitError(ctx, err)
	}
	if active >= limits.MaxConcurrent {
		return InteractiveQueryConfig{}, nil, tenantQueryConcurrencyError()
	}
	token := newID()
	lifetimeMS := sharedQueryPermitLifetime(limits, s.lakeS3RequestTimeoutSeconds, s.lakeMetadataTimeoutSeconds).Milliseconds()
	_, err = tx.ExecContext(ctx, `INSERT INTO shared_query_permits(permit_id,tenant_id,project_id,worker_id,expires_at) VALUES($1,$2,$3,$4,clock_timestamp()+$5::bigint*interval '1 millisecond')`, token, tenantID, project, s.shared.config.WorkerID, lifetimeMS)
	if err != nil {
		return InteractiveQueryConfig{}, nil, sharedQueryPermitError(ctx, err)
	}
	if err = tx.Commit(); err != nil {
		return InteractiveQueryConfig{}, nil, sharedQueryPermitError(ctx, err)
	}
	var once sync.Once
	release := func() {
		once.Do(func() {
			// Caller cancellation must not prevent cleanup. If PostgreSQL is unavailable,
			// the fixed safe expiry recovers this permit without freeing somebody else's.
			cleanup, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, _ = s.control.ExecContext(cleanup, `DELETE FROM shared_query_permits WHERE permit_id=$1 AND worker_id=$2`, token, s.shared.config.WorkerID)
		})
	}
	return limits, release, nil
}

func sharedQueryPermitError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return duckLakeQueryError(ctx, err)
	}
	return apiError(503, "shared query admission unavailable")
}

// waitSharedProjectQueries is called after the durable project deletion fence is
// committed, which prevents further permits. Files remain intact until existing
// readers release their permits or their full native safety horizons expire.
func (s *Service) waitSharedProjectQueries(ctx context.Context, project string) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if _, err := s.control.ExecContext(ctx, `DELETE FROM shared_query_permits WHERE project_id=$1 AND expires_at<=clock_timestamp()`, project); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return apiError(503, "shared query state unavailable")
		}
		var active int
		if err := s.control.QueryRowContext(ctx, `SELECT count(*) FROM shared_query_permits WHERE project_id=$1`, project).Scan(&active); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return apiError(503, "shared query state unavailable")
		}
		if active == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
