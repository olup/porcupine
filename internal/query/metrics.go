package query

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/olup/porcupine/internal/apierr"
	"github.com/olup/porcupine/internal/observability"
)

type operation uint8

const (
	opSQL operation = iota
	opCatalog
	opProfile
)

var operationNames = [...]string{"sql", "catalog", "profile"}

func (op operation) name() string { return operationNames[op] }

type metrics struct {
	*observability.Registry
	active [len(operationNames)]atomic.Int64
}

func (m *metrics) backend(contextErr, err error, op operation, elapsed time.Duration) {
	m.ObserveDuration("porcupine_query_backend_duration_seconds", elapsed,
		observability.Label{Name: "operation", Value: op.name()},
		observability.Label{Name: "outcome", Value: queryOutcome(contextErr, err)})
}

func (r *Runtime) AppendGauges(gauges []observability.Gauge) []observability.Gauge {
	gauges = append(gauges,
		observability.Gauge{Name: "porcupine_query_slots_used", Value: float64(r.UsedSlots())},
		observability.Gauge{Name: "porcupine_query_slots_capacity", Value: float64(r.limits.MaxConcurrent)})
	for op, name := range operationNames {
		gauges = append(gauges, observability.Gauge{Name: "porcupine_query_active", Value: float64(r.metrics.active[op].Load()),
			Labels: []observability.Label{{Name: "operation", Value: name}}})
	}
	return gauges
}

func queryOutcome(contextErr, err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(contextErr, context.Canceled), errors.Is(err, context.Canceled):
		return "cancelled"
	case errors.Is(contextErr, context.DeadlineExceeded), errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	}
	var managed *apierr.Error
	if errors.As(err, &managed) && managed.GetStatus() == 429 && managed.Message == "analytics execution time limit reached" {
		return "timeout"
	}
	return "error"
}

func admissionOutcome(contextErr, err error) string {
	outcome := queryOutcome(contextErr, err)
	switch outcome {
	case "ok":
		return "admitted"
	case "error":
		switch {
		case errors.Is(err, errWorkerFull):
			return "worker_limited"
		case errors.Is(err, errTenantFull):
			return "tenant_limited"
		}
		var managed *apierr.Error
		if errors.As(err, &managed) && managed.GetStatus() == 429 {
			return "limited"
		}
	}
	return outcome
}

func (m *metrics) admission(contextErr, err error, op operation, elapsed time.Duration) {
	kind := observability.Label{Name: "operation", Value: op.name()}
	m.Add("porcupine_query_admissions_total", 1, kind, observability.Label{Name: "outcome", Value: admissionOutcome(contextErr, err)})
	m.ObserveDuration("porcupine_query_admission_duration_seconds", elapsed, kind)
}
