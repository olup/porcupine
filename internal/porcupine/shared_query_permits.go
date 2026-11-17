package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"time"

	eventapi "github.com/olup/porcupine/internal/event"
	lakeapi "github.com/olup/porcupine/internal/lake"
)

const sharedQueryPermitLifetime = 30 * time.Second
const sharedQueryRenewInterval = 5 * time.Second
const sharedQueryControlTimeout = 2 * time.Second
const sharedQueryAdmissionTimeout = 10 * time.Second
const sharedQueryUnresponsiveGrace = 15 * time.Second

func sharedQueryIOGrace(s3RequestTimeoutSeconds, metadataRequestTimeoutSeconds int) time.Duration {
	return time.Duration(max(0, s3RequestTimeoutSeconds)+max(0, metadataRequestTimeoutSeconds)) * time.Second
}

func sharedQueryUnresponsiveDeadline(hardDeadline time.Time, s3RequestTimeoutSeconds, metadataRequestTimeoutSeconds int) time.Time {
	return hardDeadline.Add(sharedQueryIOGrace(s3RequestTimeoutSeconds, metadataRequestTimeoutSeconds) + sharedQueryUnresponsiveGrace)
}

type sharedQueryPermit struct {
	id, worker, project string
	control             *sql.DB
	hosted              *hostedQueryRuntime
	once                sync.Once
}

func (p *sharedQueryPermit) release() {
	p.once.Do(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), sharedQueryControlTimeout)
		defer cancel()
		// An unavailable control store leaves a conservative reservation until TTL.
		if p.hosted != nil {
			p.hosted.noteReturn(p.id, p.project)
		}
		_, _ = p.control.ExecContext(cleanup, `DELETE FROM shared_query_permits WHERE permit_id=$1 AND worker_id=$2`, p.id, p.worker)
		if p.hosted != nil {
			_ = p.hosted.acknowledgeReturn(cleanup, p.control, p.id, p.project)
		}
	})
}

// acquireSharedQueryPermit rechecks policy while holding its authoritative tenant
// row. Every process participates in the same transaction; local worker slots
// remain a separate protection for each process's own native engines.
func (s *Service) acquireSharedQueryPermit(ctx context.Context, project, tenantID string) (InteractiveQueryConfig, *sharedQueryPermit, error) {
	// The initial grant must have enough lifetime left for the first renewal,
	// even when a high query timeout permits a delayed COMMIT acknowledgement.
	ctx, cancel := context.WithTimeout(ctx, sharedQueryAdmissionTimeout)
	defer cancel()
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
	token := eventapi.NewID()
	lifetimeMS := sharedQueryPermitLifetime.Milliseconds()
	_, err = tx.ExecContext(ctx, `INSERT INTO shared_query_permits(permit_id,tenant_id,project_id,worker_id,expires_at) VALUES($1,$2,$3,$4,clock_timestamp()+$5::bigint*interval '1 millisecond')`, token, tenantID, project, s.shared.config.WorkerID, lifetimeMS)
	if err != nil {
		return InteractiveQueryConfig{}, nil, sharedQueryPermitError(ctx, err)
	}
	hosted := s.hostedQueries
	if hosted != nil {
		if !hosted.reserveCompletion() {
			return InteractiveQueryConfig{}, nil, apiError(503, "hosted query completion backlog unavailable")
		}
		if err := hosted.register(ctx, tx, project, token); err != nil {
			hosted.dropReservation()
			return InteractiveQueryConfig{}, nil, sharedQueryPermitError(ctx, err)
		}
	}
	if err = tx.Commit(); err != nil {
		if hosted != nil {
			// An ambiguous commit may have installed the blocker. No native
			// call can start without a successful admission response.
			hosted.noteReturn(token, project)
		}
		return InteractiveQueryConfig{}, nil, sharedQueryPermitError(ctx, err)
	}
	return limits, &sharedQueryPermit{id: token, worker: s.shared.config.WorkerID, project: project, control: s.control, hosted: hosted}, nil
}

func sharedQueryPermitError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return lakeapi.DuckLakeQueryError(ctx, err)
	}
	return apiError(503, "shared query admission unavailable")
}

// waitSharedProjectQueries is called after the durable project deletion fence is
// committed, which prevents further permits. Files remain intact until existing
// readers release their permits or their renewable TTL expires. Expiry can also
// follow a network partition, so it is not proof that a native reader stopped.
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
