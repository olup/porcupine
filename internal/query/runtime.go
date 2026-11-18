// Package query owns admission and the lifetime of interactive analytical reads.
package query

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"time"

	"github.com/olup/porcupine/internal/apierr"
	"github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/observability"
	"github.com/olup/porcupine/internal/usage"
)

type Runtime struct {
	limits   InteractiveQueryConfig
	capacity capacity
	control  *sql.DB
	worker   string
	ioGrace  time.Duration
	keeper   keeper
	hosted   *hostedQueries
	recorder *usage.Recorder
	clock    func() time.Time
	metrics  metrics
}

func New(limits InteractiveQueryConfig, clock func() time.Time, registry *observability.Registry) *Runtime {
	return &Runtime{limits: limits, clock: clock, metrics: metrics{Registry: registry},
		capacity: capacity{max: limits.MaxConcurrent, byTenant: make(map[string]int)}}
}

func (r *Runtime) Bind(control *sql.DB, worker string, ioGrace time.Duration) {
	r.control, r.worker, r.ioGrace = control, worker, ioGrace
}

func (r *Runtime) SetLocalRecorder(recorder *usage.Recorder) {
	r.recorder = recorder
}

func (r *Runtime) Limits() InteractiveQueryConfig {
	return r.limits
}

type ResponseBounds struct {
	Deadline time.Time
	MaxBytes int
}

// run keeps worker and tenant capacity until the backend returns. The native
// context may be canceled much earlier when an operation ignores interruption.
func (r *Runtime) run(ctx context.Context, project string, operation operation, read func(context.Context, InteractiveQueryConfig) error) (bounds ResponseBounds, resultErr error) {
	started := time.Now()
	native, limits, release, err := r.admit(ctx, project)
	var execution *usage.Execution
	if err == nil && r.recorder != nil {
		execution, err = r.recorder.StartExecution(native, release.tenant, project, operation.name())
		if err != nil {
			err = apierr.New(503, "usage admission unavailable; query was not executed")
			if stopped := release.finish(); stopped != nil {
				err = resultError(native, stopped, nil)
			}
		}
	}
	report := r.metrics.Registry != nil
	if report {
		r.metrics.admission(ctx.Err(), err, operation, time.Since(started))
	}
	if err != nil {
		return ResponseBounds{}, err
	}
	if report {
		r.metrics.active[operation].Add(1)
	}
	returned := false
	defer func() {
		elapsed, contextErr := time.Since(started), native.Err()
		if report {
			r.metrics.active[operation].Add(-1)
		}
		if execution != nil {
			execution.Finish()
		}
		stopped := release.finish()
		if returned {
			resultErr = resultError(native, stopped, resultErr)
			if report {
				if errors.Is(stopped, errPermitUncertain) {
					contextErr = nil
				}
				r.metrics.backend(contextErr, resultErr, operation, elapsed)
			}
		}
	}()
	deadline, _ := native.Deadline()
	started = time.Now()
	if execution != nil {
		execution.Begin()
	}
	resultErr = read(native, limits)
	returned = true
	return ResponseBounds{Deadline: deadline, MaxBytes: limits.MaxResultBytes}, resultErr
}

type reservation struct {
	tenant string
	finish func() error
}

func (r *Runtime) admit(ctx context.Context, project string) (context.Context, InteractiveQueryConfig, reservation, error) {
	if r.worker != "" {
		if err := r.keeper.allow(project); err != nil {
			return nil, InteractiveQueryConfig{}, reservation{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, InteractiveQueryConfig{}, reservation{}, lake.DuckLakeQueryError(ctx, err)
	}
	lookup, stopLookup := context.WithTimeout(ctx, r.limits.timeout())
	tenant, limits, err := projectPolicy(lookup, r.control, project)
	stopLookup()
	if err != nil {
		return nil, InteractiveQueryConfig{}, reservation{}, err
	}
	if err := ctx.Err(); err != nil {
		return nil, InteractiveQueryConfig{}, reservation{}, lake.DuckLakeQueryError(ctx, err)
	}
	started := time.Now()
	limits = r.limits.Effective(limits)
	localTenant := tenant
	if r.worker != "" {
		localTenant = ""
	}
	releaseSlot, err := r.capacity.tryAcquire(localTenant, limits.MaxConcurrent)
	if err != nil {
		return nil, InteractiveQueryConfig{}, reservation{}, err
	}
	var permit *permit
	if r.worker != "" {
		limits, permit, err = r.acquirePermit(ctx, project, tenant)
		if err != nil {
			releaseSlot()
			return nil, InteractiveQueryConfig{}, reservation{}, err
		}
	}

	// Shared admission uses part of the execution budget.
	deadline := started.Add(limits.timeout())
	native, cancel := context.WithDeadline(ctx, deadline)
	finish := sync.OnceValue(func() error {
		defer releaseSlot()
		if permit == nil {
			stopped := native.Err()
			cancel()
			return stopped
		}
		stopped := r.keeper.returned(native, permit, r.hosted)
		cancel()
		r.releasePermit(permit)
		return stopped
	})
	if err := native.Err(); err != nil {
		finish()
		return nil, InteractiveQueryConfig{}, reservation{}, lake.DuckLakeQueryError(native, err)
	}
	if permit != nil {
		if err := r.keeper.start(permit, cancel, deadline.Add(r.ioGrace+unresponsiveGrace), r.control, r.worker); err != nil {
			finish()
			return nil, InteractiveQueryConfig{}, reservation{}, err
		}
	}
	return native, limits, reservation{tenant: tenant, finish: finish}, nil
}

func resultError(ctx context.Context, stopped, nativeErr error) error {
	if errors.Is(stopped, errPermitUncertain) {
		return apierr.Detailed(503, "query_permit_uncertain", "shared query reservation is unavailable", "Retry on a healthy worker after shared control recovers.", "")
	}
	if nativeErr != nil {
		return nativeErr
	}
	if stopped != nil {
		return lake.DuckLakeQueryError(ctx, stopped)
	}
	return nil
}
