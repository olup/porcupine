package porcupine

import (
	"context"
	"errors"
	"sync"
	"time"
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
	c := InteractiveQueryConfig{MaxConcurrent: 2, TimeoutMS: 5000, MaxRows: analyticsMaxRows, MaxResultBytes: analyticsMaxBytes}
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

type queryExecutionLimits struct {
	Timeout        time.Duration
	MaxRows        int
	MaxResultBytes int
}

func (c InteractiveQueryConfig) executionLimits() queryExecutionLimits {
	return queryExecutionLimits{Timeout: time.Duration(c.TimeoutMS) * time.Millisecond, MaxRows: c.MaxRows, MaxResultBytes: c.MaxResultBytes}
}

func defaultQueryExecutionLimits() queryExecutionLimits {
	c, _ := normalizeInteractiveQueries(nil)
	return c.executionLimits()
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
func (s *Service) beginInteractiveQuery(ctx context.Context, project string) (context.Context, InteractiveQueryConfig, func(), error) {
	lookup, stopLookup := context.WithTimeout(ctx, time.Duration(s.interactiveConfig.TimeoutMS)*time.Millisecond)
	tenant, err := s.projectTenant(lookup, project)
	stopLookup()
	if err != nil {
		return nil, InteractiveQueryConfig{}, nil, err
	}
	limits := effectiveInteractiveQueries(s.interactiveConfig, tenant.Budgets)
	if ctx.Err() != nil {
		return nil, InteractiveQueryConfig{}, nil, duckLakeQueryError(ctx, ctx.Err())
	}
	started := time.Now()
	var releaseTenant func()
	if s.shared != nil {
		// Reserve local capacity first to avoid borrowing shared capacity when this
		// process cannot execute. The shared transaction rechecks current policy.
		select {
		case s.querySlots <- struct{}{}:
		default:
			return nil, InteractiveQueryConfig{}, nil, apiError(429, "analytics query concurrency limit reached")
		}
		admission, stop := context.WithTimeout(ctx, time.Duration(s.interactiveConfig.TimeoutMS)*time.Millisecond)
		limits, releaseTenant, err = s.acquireSharedQueryPermit(admission, project, tenant.ID)
		stop()
		if err != nil {
			<-s.querySlots
			return nil, InteractiveQueryConfig{}, nil, err
		}
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
	// Starting the hard deadline before shared admission ensures a delayed COMMIT
	// response cannot start native work after its permit's recovery horizon.
	hardDeadline := started.Add(time.Duration(limits.TimeoutMS) * time.Millisecond)
	child, cancel := context.WithDeadline(ctx, hardDeadline)
	var once sync.Once
	var stopWatchdog func()
	release := func() {
		once.Do(func() {
			if stopWatchdog != nil {
				stopWatchdog()
			}
			cancel()
			<-s.querySlots
			releaseTenant()
		})
	}
	if child.Err() != nil {
		release()
		return nil, InteractiveQueryConfig{}, nil, duckLakeQueryError(child, child.Err())
	}
	if s.shared != nil {
		stopWatchdog = armSharedQueryWatchdog(hardDeadline, s.lakeS3RequestTimeoutSeconds, s.lakeMetadataTimeoutSeconds, time.Now)
	}
	return child, limits, release, nil
}

func tenantQueryConcurrencyError() error {
	return analyticsError(429, "tenant_query_concurrency_limit", "tenant interactive query concurrency limit reached", "Wait for another query in this tenant to finish, then retry.", "")
}
