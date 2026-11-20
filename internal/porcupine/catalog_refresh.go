package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"time"

	"github.com/olup/porcupine/internal/apierr"
	"github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/query"
)

const (
	catalogRefreshProbe   = time.Minute
	catalogRefreshTimeout = 45 * time.Second
	catalogRefreshLease   = 2 * time.Minute
)

type CatalogRefreshStatus struct {
	Status    string     `json:"status" enum:"queued,running,ready,failed" doc:"A failed refresh can be retried or requested again."`
	UpdatedAt *time.Time `json:"updated_at,omitempty" doc:"Last successfully checked or refreshed time for the stored catalog."`
}

type catalogRefreshOutput struct{ Body CatalogRefreshStatus }

type catalogRefresh struct {
	store   catalogSummaryStore
	lake    *lake.DuckLakeStore
	queries *query.Runtime
	clock   func() time.Time
	wake    chan struct{}
	stop    context.CancelFunc
	done    chan struct{}
}

func (s *Service) startCatalogRefresh() {
	if s.lake == nil {
		return
	}
	ctx, stop := context.WithCancel(context.Background())
	refresh := &catalogRefresh{
		store: catalogSummaryStore{db: s.control, hosted: s.hosted != nil},
		lake:  s.lake, queries: s.queries, clock: s.clock,
		wake: make(chan struct{}, 1), stop: stop, done: make(chan struct{}),
	}
	s.eventCatalogRefresh = refresh
	go refresh.run(ctx)
}

func (r *catalogRefresh) signal() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

func (r *catalogRefresh) run(ctx context.Context) {
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

func (r *catalogRefresh) refreshOne(ctx context.Context) bool {
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
	if !errors.Is(err, errCatalogRefreshClaimLost) && !errors.Is(err, errCatalogProjectFenced) {
		// The job context can be expired while the control store remains healthy.
		record, stop := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer stop()
		discard := errors.Is(err, lake.ErrCatalogSummaryTooLarge)
		if err := r.store.fail(record, claim, r.clock(), discard); err != nil &&
			!errors.Is(err, errCatalogRefreshClaimLost) && !errors.Is(err, errCatalogProjectFenced) {
			slog.Warn("event catalog refresh retry could not be recorded")
		}
	}
	return true
}

func catalogRefreshStatus(state catalogSummaryState) *CatalogRefreshStatus {
	var status CatalogRefreshStatus
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

func (r *catalogRefresh) page(ctx context.Context, input lake.EventCatalogInput) (lake.EventCatalog, *CatalogRefreshStatus, bool) {
	at := r.clock()
	page, state, err := r.store.page(ctx, input, at)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return lake.EventCatalog{}, nil, false
	}
	if state.Status == "" {
		state, err = r.store.request(ctx, input.ID, at, false)
		if err == nil {
			r.signal()
		}
	}
	status := catalogRefreshStatus(state)
	if state.Snapshot.CatalogID == "" {
		return lake.EventCatalog{}, status, false
	}
	valid, err := r.lake.CatalogSnapshotValid(ctx, input.ID, state.Snapshot)
	if err == nil && valid {
		return page, status, true
	}
	if err == nil && r.store.invalidate(ctx, input.ID, state.Snapshot, at) == nil {
		r.signal()
		status = &CatalogRefreshStatus{Status: "queued"}
	}
	return lake.EventCatalog{}, status, false
}

func (s *Service) refreshEventCatalog(ctx context.Context, input *ProjectIDInput) (*catalogRefreshOutput, error) {
	if err := s.analyticsProject(ctx, input.ID); err != nil {
		return nil, err
	}
	refresh := s.eventCatalogRefresh
	if refresh == nil {
		return nil, apierr.New(503, "event catalog refresh unavailable")
	}
	state, err := refresh.store.request(ctx, input.ID, refresh.clock(), true)
	if errors.Is(err, errCatalogProjectFenced) {
		return nil, apierr.New(404, "project not found")
	}
	if err != nil {
		return nil, apierr.New(503, "event catalog refresh unavailable")
	}
	status := catalogRefreshStatus(state)
	if status == nil {
		return nil, apierr.New(503, "event catalog refresh unavailable")
	}
	refresh.signal()
	return &catalogRefreshOutput{Body: *status}, nil
}
