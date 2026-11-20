// Package catalog persists and refreshes materialized event catalogs.
package catalog

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"time"

	"github.com/olup/porcupine/internal/acceptance"
	"github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/query"
)

const (
	catalogRefreshProbe   = time.Minute
	catalogRefreshTimeout = 45 * time.Second
	catalogRefreshLease   = 2 * time.Minute
)

// Status describes the current refresh and the last successful check, if any.
type Status struct {
	Status    string
	UpdatedAt *time.Time
}

type Refresher struct {
	store   catalogSummaryStore
	lake    *lake.DuckLakeStore
	queries *query.Runtime
	clock   func() time.Time
	wake    chan struct{}
	stop    context.CancelFunc
	done    chan struct{}
}

// Start runs refresh jobs until Stop is called. Hosted callers supply the
// project lock used by deletion, which runs in the same transaction as the write.
func Start(control *sql.DB, store *lake.DuckLakeStore, queries *query.Runtime, clock func() time.Time, lockHostedProject func(context.Context, *sql.Tx, string) error) *Refresher {
	ctx, stop := context.WithCancel(context.Background())
	refresh := &Refresher{
		store: catalogSummaryStore{db: control, lockHostedProject: lockHostedProject},
		lake:  store, queries: queries, clock: clock,
		wake: make(chan struct{}, 1), stop: stop, done: make(chan struct{}),
	}
	go refresh.run(ctx)
	return refresh
}

// Stop cancels refresh work without waiting for it to return.
func (r *Refresher) Stop() { r.stop() }

// Done closes after any in-flight refresh has returned.
func (r *Refresher) Done() <-chan struct{} { return r.done }

func (r *Refresher) signal() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

func (r *Refresher) run(ctx context.Context) {
	defer close(r.done)
	ticker := time.NewTicker(catalogRefreshProbe)
	defer ticker.Stop()
	r.signal()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-r.wake:
		}
		for ctx.Err() == nil && r.refreshOne(ctx) {
		}
	}
}

func (r *Refresher) refreshOne(ctx context.Context) bool {
	ctx, cancel := context.WithTimeout(ctx, catalogRefreshTimeout)
	defer cancel()
	claim, err := r.store.claim(ctx, r.clock(), catalogRefreshLease)
	if errors.Is(err, sql.ErrNoRows) {
		return false
	}
	if err != nil {
		if ctx.Err() == nil {
			slog.Warn("event catalog refresh claim unavailable")
		}
		return false
	}
	var prior *lake.ExportSnapshot
	if claim.Snapshot.CatalogID != "" {
		prior = &claim.Snapshot
	}
	var result lake.CatalogSnapshotRead
	err = r.queries.RefreshCatalog(ctx, claim.ProjectID, func(native context.Context) error {
		var readErr error
		result, readErr = r.lake.ReadCatalogSnapshot(native, claim.ProjectID, prior)
		return readErr
	})
	if err == nil {
		err = r.store.complete(ctx, claim, result, r.clock())
	}
	if err == nil {
		return true
	}
	if !errors.Is(err, errCatalogRefreshClaimLost) && !errors.Is(err, acceptance.ErrProjectFenced) {
		// The job context can be expired while the control store remains healthy.
		record, stop := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer stop()
		discard := errors.Is(err, lake.ErrCatalogSummaryTooLarge)
		if err := r.store.fail(record, claim, r.clock(), discard); err != nil &&
			!errors.Is(err, errCatalogRefreshClaimLost) && !errors.Is(err, acceptance.ErrProjectFenced) {
			slog.Warn("event catalog refresh retry could not be recorded")
		}
	}
	return true
}

func catalogRefreshStatus(state catalogSummaryState) *Status {
	var status Status
	switch state.Status {
	case "queued", "running":
		status.Status = state.Status
	case "current":
		status.Status = "ready"
	case "retrying":
		status.Status = "failed"
	default:
		return nil
	}
	if !state.RefreshedAt.IsZero() {
		at := state.RefreshedAt.UTC()
		status.UpdatedAt = &at
	}
	return &status
}

// Page serves the materialized summary if its checkpoint remains valid. Call
// within an admitted catalog query because validation can open a native snapshot.
func (r *Refresher) Page(ctx context.Context, input lake.EventCatalogInput) (lake.EventCatalog, *Status, bool) {
	at := r.clock()
	page, state, err := r.store.page(ctx, input, at)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return lake.EventCatalog{}, nil, false
	}
	status := catalogRefreshStatus(state)
	if status == nil {
		status = &Status{Status: "ready"}
	}
	if state.Snapshot.CatalogID == "" {
		return lake.EventCatalog{}, status, false
	}
	valid, err := r.lake.CatalogSnapshotValid(ctx, input.ID, state.Snapshot)
	if err == nil && valid {
		return page, status, true
	}
	if err == nil && r.store.invalidate(ctx, input.ID, state.Snapshot, at) == nil {
		r.signal()
		status = &Status{Status: "queued"}
	}
	return lake.EventCatalog{}, status, false
}

// Request makes a project eligible for refresh and wakes the worker.
func (r *Refresher) Request(ctx context.Context, project string) (*Status, error) {
	state, err := r.store.request(ctx, project, r.clock())
	if err != nil {
		return nil, err
	}
	status := catalogRefreshStatus(state)
	if status != nil {
		r.signal()
	}
	return status, nil
}

// Status reads the refresh state without accessing DuckLake.
func (r *Refresher) Status(ctx context.Context, project string) (Status, error) {
	state, err := r.store.status(ctx, project, r.clock())
	if err != nil {
		return Status{}, err
	}
	if status := catalogRefreshStatus(state); status != nil {
		return *status, nil
	}
	return Status{Status: "ready"}, nil
}
