package porcupine

import (
	"context"
	"database/sql/driver"
	"errors"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/olup/porcupine/internal/acceptance"
	"github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/observability"
	"github.com/olup/porcupine/internal/output"
)

func (m *operationalMetrics) observeHostedAdmissionGuard(contextErr, err error, elapsed time.Duration) {
	if m == nil {
		return
	}
	var outcome string
	switch {
	case err == nil:
		outcome = "acquired"
	case errors.Is(err, errHostedProjectFenced):
		outcome = "fenced"
	case errors.Is(err, acceptance.ErrProjectQuarantined):
		outcome = "quarantined"
	case errors.Is(contextErr, context.DeadlineExceeded), errors.Is(err, context.DeadlineExceeded):
		outcome = "timeout"
	case errors.Is(contextErr, context.Canceled), errors.Is(err, context.Canceled):
		outcome = "cancelled"
	default:
		outcome = "error"
	}
	m.ObserveDuration("porcupine_ingestion_admission_guard_duration_seconds", elapsed, observability.Label{Name: "outcome", Value: outcome})
}

func (m *operationalMetrics) observeHostedFlushPhase(phase string, elapsed time.Duration) {
	if m != nil {
		m.ObserveDuration("porcupine_flush_phase_duration_seconds", elapsed, observability.Label{Name: "phase", Value: phase})
	}
}

func hostedFlushRetryReason(err error) string {
	var native *duckdb.Error
	var postgres *pgconn.PgError
	switch {
	case errors.Is(err, lake.ErrProjectWriteBusy):
		return "coordinator_busy"
	case errors.Is(err, lake.ErrProjectWriteSessionLost):
		return "write_session_lost"
	case errors.Is(err, lake.ErrProjectWriteFenceStale), errors.Is(err, lake.ErrProjectWriteSealed):
		return "native_fence"
	case errors.Is(err, errHostedIncarnationLost):
		return "incarnation_lost"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "cancelled"
	case errors.Is(err, lake.ErrNativeEffectsPending):
		return "native_effects_pending"
	case errors.Is(err, lake.ErrNativeEvidenceGap):
		return "native_evidence_gap"
	case errors.Is(err, lake.ErrAnalyticalS3Unavailable):
		return "object_storage"
	case errors.Is(err, acceptance.ErrBatchTooLarge), errors.Is(err, output.ErrPipelineCapacity):
		return "pipeline_capacity"
	case errors.Is(err, output.ErrPipelineAuthority):
		return "pipeline_authority"
	case errors.Is(err, output.ErrPipelineConflict):
		return "pipeline_conflict"
	case errors.As(err, &native):
		if native.Type == duckdb.ErrorTypeTransaction {
			return "native_conflict"
		}
		return "native_error"
	case errors.As(err, &postgres), errors.Is(err, driver.ErrBadConn):
		return "postgres_error"
	default:
		return "other"
	}
}
