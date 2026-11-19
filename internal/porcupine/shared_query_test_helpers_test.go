package porcupine

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"github.com/olup/porcupine/internal/apierr"
	"github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/query"
)

// Wait for the renewable permits to release or expire. Hosted deletion also
// requires the separate actual-return barrier.
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
			return apierr.New(503, "shared query state unavailable")
		}
		var active int
		if err := s.control.QueryRowContext(ctx, `SELECT count(*) FROM shared_query_permits WHERE project_id=$1`, project).Scan(&active); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return apierr.New(503, "shared query state unavailable")
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

type queryReadHook struct {
	query.Reader
	read func(context.Context, lake.QueryExecutionLimits) error
}

func (q queryReadHook) Query(ctx context.Context, _, _ string, _ map[string]string, limits lake.QueryExecutionLimits) (lake.AnalyticsResult, error) {
	return lake.AnalyticsResult{}, q.read(ctx, limits)
}

// Hold a real runtime scope open so another worker can test admission while
// the read has yet to return. As with native I/O, cancellation does not end it.
func (s *Service) holdQuery(ctx context.Context, project string) (context.Context, lake.QueryExecutionLimits, func() error, error) {
	type admission struct {
		ctx    context.Context
		limits lake.QueryExecutionLimits
	}
	entered := make(chan admission)
	resume := make(chan struct{})
	done := make(chan struct{})
	var result error
	go func() {
		defer close(done)
		_, _, result = s.queries.SQL(ctx, queryReadHook{read: func(native context.Context, limits lake.QueryExecutionLimits) error {
			entered <- admission{ctx: native, limits: limits}
			<-resume
			return nil
		}}, project, "", nil, nil)
	}()
	select {
	case started := <-entered:
		release := sync.OnceValue(func() error {
			close(resume)
			<-done
			return result
		})
		return started.ctx, started.limits, release, nil
	case <-done:
		return nil, lake.QueryExecutionLimits{}, nil, result
	}
}

func (s *Service) holdSharedQuery(ctx context.Context, project string) (string, func() error, error) {
	_, _, release, err := s.holdQuery(ctx, project)
	if err != nil {
		return "", nil, err
	}
	var token string
	if err := s.control.QueryRowContext(ctx, `SELECT permit_id FROM shared_query_permits WHERE worker_id=$1 AND project_id=$2`, s.shared.workerID, project).Scan(&token); err != nil {
		release()
		return "", nil, err
	}
	return token, release, nil
}

func waitQueryStatus(ctx context.Context, runtime *query.Runtime, ready func(query.QueryExecutionStatus) bool) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for !ready(runtime.Status()) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
	return nil
}

// The tests temporarily shorten the lease, so an expiration over 20 seconds
// out proves that the production keeper renewed it.
func waitForRenewedPermits(ctx context.Context, db *sql.DB, want int) error {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		var count int
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM shared_query_permits WHERE expires_at>clock_timestamp()+interval '20 seconds'`).Scan(&count); err != nil {
			return err
		}
		if count == want {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
