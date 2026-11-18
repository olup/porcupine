package query

import (
	"cmp"
	"errors"
	"time"

	"github.com/olup/porcupine/internal/lake"
)

// InteractiveQueryConfig defines deployment safety ceilings. Tenant policy can
// reduce these limits, but cannot increase the deployment's available capacity.
type InteractiveQueryConfig struct {
	MaxConcurrent  int `json:"max_concurrent" doc:"Worker analytical slots, including the background export safety slot; also bounds the effective tenant concurrency policy. Shared mode additionally enforces tenant permits across workers."`
	TimeoutMS      int `json:"timeout_ms" doc:"Maximum interactive execution duration in milliseconds."`
	MaxRows        int `json:"max_rows"`
	MaxResultBytes int `json:"max_result_bytes"`
}

func Normalize(raw *InteractiveQueryConfig) (InteractiveQueryConfig, error) {
	var c InteractiveQueryConfig
	if raw != nil {
		c = *raw
	}
	c.MaxConcurrent = cmp.Or(c.MaxConcurrent, 2)
	c.TimeoutMS = cmp.Or(c.TimeoutMS, 5000)
	c.MaxRows = cmp.Or(c.MaxRows, lake.AnalyticsMaxRows)
	c.MaxResultBytes = cmp.Or(c.MaxResultBytes, lake.AnalyticsMaxBytes)
	if c.MaxConcurrent < 2 || c.MaxConcurrent > 64 || c.TimeoutMS < 1 || c.TimeoutMS > 60000 || c.MaxRows < 1 || c.MaxRows > 100000 || c.MaxResultBytes < 256 || c.MaxResultBytes > 64<<20 {
		return InteractiveQueryConfig{}, errors.New("interactive query ceilings require concurrency 2..64, timeout 1..60000 ms, rows 1..100000 and result bytes 256..67108864")
	}
	return c, nil
}

func (deployment InteractiveQueryConfig) Effective(tenant InteractiveQueryConfig) InteractiveQueryConfig {
	return InteractiveQueryConfig{
		MaxConcurrent:  min(deployment.MaxConcurrent, tenant.MaxConcurrent),
		TimeoutMS:      min(deployment.TimeoutMS, tenant.TimeoutMS),
		MaxRows:        min(deployment.MaxRows, tenant.MaxRows),
		MaxResultBytes: min(deployment.MaxResultBytes, tenant.MaxResultBytes),
	}
}

func (c InteractiveQueryConfig) executionLimits() lake.QueryExecutionLimits {
	return lake.QueryExecutionLimits{Timeout: c.timeout(), MaxRows: c.MaxRows, MaxResultBytes: c.MaxResultBytes}
}

func (c InteractiveQueryConfig) timeout() time.Duration {
	return time.Duration(c.TimeoutMS) * time.Millisecond
}
