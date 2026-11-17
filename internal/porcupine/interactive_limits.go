package porcupine

import (
	"context"
	"errors"
	"sync"
	"time"

	lakeapi "github.com/olup/porcupine/internal/lake"
)

// InteractiveQueryConfig defines deployment safety ceilings. Tenant policy can
// reduce these limits, but cannot increase the deployment's available capacity.
type InteractiveQueryConfig struct {
	MaxConcurrent  int `json:"max_concurrent" doc:"Worker analytical slots, including the background export safety slot; also bounds the effective tenant concurrency policy. Shared mode additionally enforces tenant permits across workers."`
	TimeoutMS      int `json:"timeout_ms" doc:"Maximum interactive execution duration in milliseconds."`
	MaxRows        int `json:"max_rows"`
	MaxResultBytes int `json:"max_result_bytes"`
}

func normalizeInteractiveQueries(raw *InteractiveQueryConfig) (InteractiveQueryConfig, error) {
	c := InteractiveQueryConfig{MaxConcurrent: 2, TimeoutMS: 5000, MaxRows: lakeapi.AnalyticsMaxRows, MaxResultBytes: lakeapi.AnalyticsMaxBytes}
	if raw != nil {
		if raw.MaxConcurrent != 0 {
			c.MaxConcurrent = raw.MaxConcurrent
		}
		if raw.TimeoutMS != 0 {
			c.TimeoutMS = raw.TimeoutMS
		}
		if raw.MaxRows != 0 {
			c.MaxRows = raw.MaxRows
		}
		if raw.MaxResultBytes != 0 {
			c.MaxResultBytes = raw.MaxResultBytes
		}
	}
	if c.MaxConcurrent < 2 || c.MaxConcurrent > 64 || c.TimeoutMS < 1 || c.TimeoutMS > 60000 || c.MaxRows < 1 || c.MaxRows > 100000 || c.MaxResultBytes < 256 || c.MaxResultBytes > 64<<20 {
		return InteractiveQueryConfig{}, errors.New("interactive query ceilings require concurrency 2..64, timeout 1..60000 ms, rows 1..100000 and result bytes 256..67108864")
	}
	return c, nil
}

func effectiveInteractiveQueries(deployment InteractiveQueryConfig, tenant TenantBudgets) InteractiveQueryConfig {
	return InteractiveQueryConfig{
		MaxConcurrent:  min(deployment.MaxConcurrent, tenant.HotQueryMaxConcurrent),
		TimeoutMS:      min(deployment.TimeoutMS, tenant.HotQueryTimeoutMS),
		MaxRows:        min(deployment.MaxRows, tenant.HotQueryMaxRows),
		MaxResultBytes: min(deployment.MaxResultBytes, tenant.HotQueryMaxResultBytes),
	}
}

func (c InteractiveQueryConfig) executionLimits() lakeapi.QueryExecutionLimits {
	return lakeapi.QueryExecutionLimits{Timeout: time.Duration(c.TimeoutMS) * time.Millisecond, MaxRows: c.MaxRows, MaxResultBytes: c.MaxResultBytes}
}

type tenantQueryAdmission struct {
	mu     sync.Mutex
	active map[string]int
}

func (a *tenantQueryAdmission) acquire(tenant string, limit int) (func(), bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.active[tenant] >= limit {
		return nil, false
	}
	if a.active == nil {
		a.active = make(map[string]int)
	}
	a.active[tenant]++
	var once sync.Once
	return func() {
		once.Do(func() {
			a.mu.Lock()
			defer a.mu.Unlock()
			a.active[tenant]--
			if a.active[tenant] == 0 {
				delete(a.active, tenant)
			}
		})
	}, true
}

// beginInteractiveQuery reserves tenant and deployment capacity together for
// SQL and discovery. Existing jobs retain their independent heavy-query policy.
// Finish immediately after the lake method returns; its error preserves execution
// cancellation before cleanup cancels the native context. Response processing
// needs its own context with the same deadline, derived from the request parent.
func (s *Service) beginInteractiveQuery(ctx context.Context, project string) (context.Context, InteractiveQueryConfig, func() error, error) {
	if s.shared != nil {
		if err := s.sharedQueries.allow(project); err != nil {
			return nil, InteractiveQueryConfig{}, nil, err
		}
	}
	if ctx.Err() != nil {
		return nil, InteractiveQueryConfig{}, nil, lakeapi.DuckLakeQueryError(ctx, ctx.Err())
	}
	lookup, stopLookup := context.WithTimeout(ctx, time.Duration(s.interactiveConfig.TimeoutMS)*time.Millisecond)
	tenant, err := s.projectTenant(lookup, project)
	stopLookup()
	if err != nil {
		return nil, InteractiveQueryConfig{}, nil, err
	}
	limits := effectiveInteractiveQueries(s.interactiveConfig, tenant.Budgets)
	if ctx.Err() != nil {
		return nil, InteractiveQueryConfig{}, nil, lakeapi.DuckLakeQueryError(ctx, ctx.Err())
	}
	started := time.Now()
	var releaseTenant func()
	var permit *sharedQueryPermit
	if s.shared != nil {
		// Reserve local capacity first to avoid borrowing shared capacity when this
		// process cannot execute. The shared transaction rechecks current policy.
		select {
		case s.querySlots <- struct{}{}:
		default:
			return nil, InteractiveQueryConfig{}, nil, apiError(429, "analytics query concurrency limit reached")
		}
		admission, stop := context.WithTimeout(ctx, time.Duration(s.interactiveConfig.TimeoutMS)*time.Millisecond)
		limits, permit, err = s.acquireSharedQueryPermit(admission, project, tenant.ID)
		stop()
		if err != nil {
			<-s.querySlots
			return nil, InteractiveQueryConfig{}, nil, err
		}
		releaseTenant = permit.release
	} else {
		var ok bool
		releaseTenant, ok = s.tenantQueries.acquire(tenant.ID, limits.MaxConcurrent)
		if !ok {
			return nil, InteractiveQueryConfig{}, nil, tenantQueryConcurrencyError()
		}
		select {
		case s.querySlots <- struct{}{}:
		default:
			releaseTenant()
			return nil, InteractiveQueryConfig{}, nil, apiError(429, "analytics query concurrency limit reached")
		}
	}
	// Admission uses part of the execution budget instead of restarting it.
	hardDeadline := started.Add(time.Duration(limits.TimeoutMS) * time.Millisecond)
	child, cancel := context.WithDeadline(ctx, hardDeadline)
	var once sync.Once
	var finishShared func() error
	var stopped error
	release := func() error {
		once.Do(func() {
			if finishShared != nil {
				stopped = finishShared()
			} else {
				stopped = child.Err()
				releaseTenant()
			}
			cancel()
			<-s.querySlots
		})
		return stopped
	}
	if child.Err() != nil {
		release()
		return nil, InteractiveQueryConfig{}, nil, lakeapi.DuckLakeQueryError(child, child.Err())
	}
	if s.shared != nil {
		finishShared, err = s.sharedQueries.start(child, permit, project, cancel, sharedQueryUnresponsiveDeadline(hardDeadline, s.lakeS3RequestTimeoutSeconds, s.lakeMetadataTimeoutSeconds))
		if err != nil {
			release()
			return nil, InteractiveQueryConfig{}, nil, err
		}
	}
	return child, limits, release, nil
}

func tenantQueryConcurrencyError() error {
	return analyticsError(429, "tenant_query_concurrency_limit", "tenant interactive query concurrency limit reached", "Wait for another query in this tenant to finish, then retry.", "")
}

func interactiveQueryResultError(ctx context.Context, stopped, nativeErr error) error {
	if errors.Is(stopped, errSharedQueryPermitUncertain) {
		return analyticsError(503, "query_permit_uncertain", "shared query reservation is unavailable", "Retry on a healthy worker after shared control recovers.", "")
	}
	if nativeErr != nil {
		return nativeErr
	}
	if stopped != nil {
		return lakeapi.DuckLakeQueryError(ctx, stopped)
	}
	return nil
}
