package query

import (
	"context"
	"database/sql"
	"errors"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/olup/porcupine/internal/apierr"
	"github.com/olup/porcupine/internal/usage"
)

const HostedUsageSource = "hosted_query_executions"
const completionLimit = 256
const hostedQueryLostAfter = 5 * time.Minute
const hostedQueryRecoverBatch = 1000

type hostedQueries struct {
	register func(context.Context, *sql.Tx, string, string) error
	finish   func(context.Context, *sql.DB, string, string) error
	mu       sync.Mutex
	returned map[string]*permit
	inFlight int
}

// UseHostedActivity registers the project lifetime blocker inside the same
// transaction that allocates a permit. The blocker outlives request cancellation.
func (r *Runtime) UseHostedActivity(register func(context.Context, *sql.Tx, string, string) error, finish func(context.Context, *sql.DB, string, string) error) error {
	if r.worker == "" || r.control == nil || register == nil || finish == nil {
		return errors.New("hosted reads require shared query admission, usage and project lifetime authority")
	}
	r.hosted = &hostedQueries{register: register, finish: finish, returned: make(map[string]*permit)}
	r.recorder = usage.NewRecorder(r.control, HostedUsageSource, r.clock)
	return nil
}

func (h *hostedQueries) registerPermit(ctx context.Context, tx *sql.Tx, p *permit) error {
	h.mu.Lock()
	if h.inFlight+len(h.returned) >= completionLimit {
		h.mu.Unlock()
		return apierr.New(503, "hosted query completion backlog unavailable")
	}
	h.inFlight++
	h.mu.Unlock()
	if err := h.register(ctx, tx, p.project, p.id); err != nil {
		h.mu.Lock()
		h.inFlight--
		h.mu.Unlock()
		return err
	}
	return nil
}

func (h *hostedQueries) noteReturn(p *permit) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.inFlight--
	h.returned[p.id] = p
}

func (h *hostedQueries) acknowledgeReturn(ctx context.Context, db *sql.DB, p *permit) error {
	if err := h.finish(ctx, db, p.project, p.id); err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.returned, p.id)
	return nil
}

func (r *Runtime) ObserveHostedUsage(ctx context.Context) error {
	h := r.hosted
	h.mu.Lock()
	pending := slices.Collect(maps.Values(h.returned))
	h.mu.Unlock()

	var returnsErr error
	for _, p := range pending {
		if returnsErr = h.acknowledgeReturn(ctx, r.control, p); returnsErr != nil {
			break
		}
	}
	return errors.Join(returnsErr, r.recorder.Observe(ctx))
}

func (r *Runtime) PruneHostedUsage(ctx context.Context, retentionDays int) error {
	return r.recorder.PruneExecutions(ctx, retentionDays)
}

// A live owner observes its usage executions every ObservationInterval. A
// stale record is provisional because a partitioned worker can observe it again.
func (r *Runtime) RecoverHostedUsage(ctx context.Context) (bool, error) {
	recovered, err := r.recorder.RecoverStale(ctx, r.clock().Add(-hostedQueryLostAfter), hostedQueryRecoverBatch)
	return recovered < hostedQueryRecoverBatch, err
}
