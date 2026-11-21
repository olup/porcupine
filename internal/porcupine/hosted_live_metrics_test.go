package porcupine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/observability"
	"github.com/olup/porcupine/internal/query"
)

func TestHostedFlushRetryReason(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		err  error
		want string
	}{
		{lakeapi.ErrProjectWriteBusy, "coordinator_busy"},
		{errors.Join(context.Canceled, lakeapi.ErrProjectWriteSessionLost), "write_session_lost"},
		{lakeapi.ErrAnalyticalS3Unavailable, "object_storage"},
		{context.DeadlineExceeded, "timeout"},
		{errors.New("private storage error"), "other"},
	} {
		if got := hostedFlushRetryReason(fmt.Errorf("flush attempt: %w", tc.err)); got != tc.want {
			t.Errorf("retry reason for %v = %q, want %q", tc.err, got, tc.want)
		}
	}
}

func TestSharedHostedAdmissionGuardAndPermitMetrics(t *testing.T) {
	t.Parallel()
	_, a, b := sharedQueryTestFixture(t)
	ctx := t.Context()
	registry := observability.NewRegistry("hosted")
	f := &hostedLiveFlusher{publisher: &hostedEffectPublisher{control: a.control}, metrics: &operationalMetrics{Registry: registry}}
	first := query.New(a.queries.Limits(), time.Now, registry)
	first.Bind(a.control, "first-metrics", 0)
	second := query.New(b.queries.Limits(), time.Now, registry)
	second.Bind(b.control, "second-metrics", 0)

	shouldNotRead := func(context.Context) error {
		t.Error("query ran without a permit")
		return nil
	}
	admission, err := f.lockLiveAdmission(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	bounded, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	_, err = second.Catalog(bounded, "a", shouldNotRead)
	cancel()
	admission()
	if queryStatus(err) != 429 {
		t.Fatalf("query permit waiting for ingestion's shared lock returned %v", err)
	}
	var waitingMetrics strings.Builder
	registry.Write(&waitingMetrics, nil)
	requireMetric(t, waitingMetrics.String(), `porcupine_query_shared_permit_lock_duration_seconds_bucket{profile="hosted",le="0.1"} 0`)

	tx, err := b.control.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT t.id FROM tenants t JOIN projects p ON p.tenant_id=t.id WHERE p.id='a' FOR UPDATE OF t,p`); err != nil {
		t.Fatal(err)
	}
	bounded, cancel = context.WithTimeout(ctx, 250*time.Millisecond)
	admission, err = f.lockLiveAdmission(bounded, "a")
	cancel()
	if admission != nil {
		admission()
	}
	if err == nil {
		t.Fatal("ingestion admission passed the query permit's exclusive lock")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, err := f.lockLiveAdmission(ctx, "unknown-project"); !errors.Is(err, errHostedProjectFenced) {
		t.Fatalf("missing project guard returned %v", err)
	}

	background, ok := first.TryBackground(ctx)
	if !ok {
		t.Fatal("could not occupy an idle worker slot")
	}
	defer background()
	_, err = first.Catalog(ctx, "a", func(ctx context.Context) error {
		if _, err := first.Catalog(ctx, "c", shouldNotRead); queryStatus(err) != 429 {
			t.Errorf("worker capacity returned %v", err)
		}
		if _, err := second.Catalog(ctx, "b", shouldNotRead); queryStatus(err) != 429 {
			t.Errorf("tenant capacity returned %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	var out strings.Builder
	registry.Write(&out, nil)
	for _, line := range []string{
		`porcupine_ingestion_admission_guard_duration_seconds_count{profile="hosted",outcome="acquired"} 1`,
		`porcupine_ingestion_admission_guard_duration_seconds_count{profile="hosted",outcome="fenced"} 1`,
		`porcupine_ingestion_admission_guard_duration_seconds_count{profile="hosted",outcome="timeout"} 1`,
		`porcupine_ingestion_admission_guard_duration_seconds_bucket{profile="hosted",outcome="timeout",le="0.1"} 0`,
		`porcupine_query_admissions_total{profile="hosted",operation="catalog",outcome="worker_limited"} 1`,
		`porcupine_query_admissions_total{profile="hosted",operation="catalog",outcome="tenant_limited"} 1`,
		`porcupine_query_admissions_total{profile="hosted",operation="catalog",outcome="timeout"} 1`,
		`porcupine_query_shared_permit_duration_seconds_count{profile="hosted",outcome="admitted"} 1`,
		`porcupine_query_shared_permit_duration_seconds_count{profile="hosted",outcome="tenant_limited"} 1`,
		`porcupine_query_shared_permit_duration_seconds_count{profile="hosted",outcome="timeout"} 1`,
		`porcupine_query_shared_permit_lock_duration_seconds_count{profile="hosted"} 3`,
	} {
		requireMetric(t, out.String(), line)
	}
}
