package query

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/olup/porcupine/internal/apierr"
	"github.com/olup/porcupine/internal/event"
	"github.com/olup/porcupine/internal/lake"
)

const (
	permitLifetime    = 30 * time.Second
	renewInterval     = 5 * time.Second
	controlTimeout    = 2 * time.Second
	admissionTimeout  = 10 * time.Second
	unresponsiveGrace = 15 * time.Second
)

type permit struct {
	id, project string
}

const policySQL = `SELECT t.id,t.hot_query_max_concurrent,t.hot_query_timeout_ms,t.hot_query_max_rows,t.hot_query_max_result_bytes
FROM tenants t JOIN projects p ON p.tenant_id=t.id
WHERE p.id=$1 AND NOT EXISTS(SELECT 1 FROM project_deletions WHERE project_id=p.id)`

func scanPolicy(row *sql.Row) (string, InteractiveQueryConfig, error) {
	var tenant string
	var limits InteractiveQueryConfig
	err := row.Scan(&tenant, &limits.MaxConcurrent, &limits.TimeoutMS, &limits.MaxRows, &limits.MaxResultBytes)
	if errors.Is(err, sql.ErrNoRows) {
		err = apierr.New(404, "project not found")
	}
	return tenant, limits, err
}

func projectPolicy(ctx context.Context, db *sql.DB, project string) (string, InteractiveQueryConfig, error) {
	tenant, limits, err := scanPolicy(db.QueryRowContext(ctx, policySQL, project))
	var managed *apierr.Error
	if err != nil && !errors.As(err, &managed) {
		err = apierr.New(503, "tenant storage unavailable")
	}
	return tenant, limits, err
}

// Shared admission serializes concurrent grants and policy changes on the
// tenant row and locks the project against deletion.
func (r *Runtime) acquirePermit(ctx context.Context, project, tenant string) (InteractiveQueryConfig, *permit, error) {
	// Leave enough lease lifetime for the first renewal after commit.
	ctx, cancel := context.WithTimeout(ctx, min(r.limits.timeout(), admissionTimeout))
	defer cancel()
	tx, err := r.control.BeginTx(ctx, nil)
	if err != nil {
		return InteractiveQueryConfig{}, nil, admissionError(ctx, err)
	}
	defer tx.Rollback()

	_, limits, err := scanPolicy(tx.QueryRowContext(ctx, policySQL+` AND t.id=$2 FOR UPDATE OF t,p`, project, tenant))
	if err != nil {
		return InteractiveQueryConfig{}, nil, admissionError(ctx, err)
	}
	limits = r.limits.Effective(limits)
	if _, err = tx.ExecContext(ctx, `DELETE FROM shared_query_permits WHERE tenant_id=$1 AND expires_at<=clock_timestamp()`, tenant); err != nil {
		return InteractiveQueryConfig{}, nil, admissionError(ctx, err)
	}
	var active int
	if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM shared_query_permits WHERE tenant_id=$1`, tenant).Scan(&active); err != nil {
		return InteractiveQueryConfig{}, nil, admissionError(ctx, err)
	}
	if active >= limits.MaxConcurrent {
		return InteractiveQueryConfig{}, nil, errTenantFull
	}
	p := &permit{id: event.NewID(), project: project}
	_, err = tx.ExecContext(ctx, `INSERT INTO shared_query_permits(permit_id,tenant_id,project_id,worker_id,expires_at) VALUES($1,$2,$3,$4,clock_timestamp()+$5::bigint*interval '1 millisecond')`, p.id, tenant, project, r.worker, permitLifetime.Milliseconds())
	if err != nil {
		return InteractiveQueryConfig{}, nil, admissionError(ctx, err)
	}
	if h := r.hosted; h != nil {
		if err := h.registerPermit(ctx, tx, p); err != nil {
			return InteractiveQueryConfig{}, nil, admissionError(ctx, err)
		}
	}
	if err := tx.Commit(); err != nil {
		if r.hosted != nil {
			// A missed commit acknowledgement may leave a blocker. No
			// execution starts unless the transaction succeeds.
			r.hosted.noteReturn(p)
		}
		return InteractiveQueryConfig{}, nil, admissionError(ctx, err)
	}
	return limits, p, nil
}

func admissionError(ctx context.Context, err error) error {
	var managed *apierr.Error
	if errors.As(err, &managed) {
		return managed
	}
	if ctx.Err() != nil {
		return lake.DuckLakeQueryError(ctx, err)
	}
	return apierr.New(503, "shared query admission unavailable")
}

func (r *Runtime) releasePermit(p *permit) {
	cleanup, cancel := context.WithTimeout(context.Background(), controlTimeout)
	defer cancel()
	// A failed release leaves the reservation in place until expiry.
	_, _ = r.control.ExecContext(cleanup, `DELETE FROM shared_query_permits WHERE permit_id=$1 AND worker_id=$2`, p.id, r.worker)
	if r.hosted != nil {
		_ = r.hosted.acknowledgeReturn(cleanup, r.control, p)
	}
}
