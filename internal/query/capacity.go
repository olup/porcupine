package query

import (
	"context"
	"sync"

	"github.com/olup/porcupine/internal/apierr"
)

var errWorkerFull = apierr.New(429, "analytics query concurrency limit reached")
var errTenantFull = apierr.Detailed(429, "tenant_query_concurrency_limit", "tenant interactive query concurrency limit reached", "Wait for another query in this tenant to finish, then retry.", "")

type capacity struct {
	mu       sync.Mutex
	max      int
	active   int
	byTenant map[string]int
}

func (c *capacity) tryAcquire(tenant string, limit int) (func(), error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if tenant != "" && c.byTenant[tenant] >= limit {
		return nil, errTenantFull
	}
	if c.active >= c.max {
		return nil, errWorkerFull
	}
	return c.reserve(tenant), nil
}

// Background jobs start only on an idle worker and occupy one analytical slot.
func (r *Runtime) TryBackground(ctx context.Context) (func(), bool) {
	c := &r.capacity
	c.mu.Lock()
	defer c.mu.Unlock()
	if ctx.Err() != nil || c.active != 0 || c.max == 0 {
		return nil, false
	}
	return c.reserve(""), true
}

func (c *capacity) reserve(tenant string) func() {
	c.active++
	if tenant != "" {
		c.byTenant[tenant]++
	}
	return sync.OnceFunc(func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.active--
		if tenant != "" {
			c.byTenant[tenant]--
			if c.byTenant[tenant] == 0 {
				delete(c.byTenant, tenant)
			}
		}
	})
}

func (r *Runtime) UsedSlots() int {
	r.capacity.mu.Lock()
	defer r.capacity.mu.Unlock()
	return r.capacity.active
}
