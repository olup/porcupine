package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/acceptance"
	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/output"
)

func TestHostedDeterministicFlushError(t *testing.T) {
	t.Parallel()
	publisher := &hostedEffectPublisher{}
	_, bindingMismatch := publisher.verify(t.Context(), output.PipelineBinding{}, lakeapi.NativeCommitEvidence{}, lakeapi.ImmutableObjectRef{})
	for _, test := range []struct {
		err  error
		code string
	}{
		{fmt.Errorf("checkpoint: %w", lakeapi.ErrNativeEvidenceGap), "native_evidence_gap"},
		{errors.Join(bindingMismatch, nil), "pipeline_evidence_conflict"},
		{fmt.Errorf("prepare: %w", lakeapi.ErrInvalidLiveProjection), "invalid_live_projection"},
		{errors.Join(lakeapi.ErrNativeEvidenceGap, sql.ErrConnDone), ""},
		{errors.Join(bindingMismatch, context.Canceled), ""},
		{sql.ErrConnDone, ""},
		{context.DeadlineExceeded, ""},
		{lakeapi.ErrProjectWriteSessionLost, ""},
		{lakeapi.ErrProjectWriteFenceStale, ""},
		{output.ErrPipelineConflict, ""},
		{output.ErrPipelineCapacity, ""},
		{output.ErrPipelineDemandTooLarge, ""},
		{errors.New("S3 request failed"), ""},
	} {
		if got := hostedDeterministicFlushError(test.err); got != test.code {
			t.Errorf("%v classified %q, want %q", test.err, got, test.code)
		}
	}
}

func TestHostedIngestionQuarantineHTTP(t *testing.T) {
	t.Parallel()
	s := service(t, 20)
	broken, brokenKey := project(t, s)
	healthy, healthyKey := project(t, s)
	_, reader := key(t, s, []string{"management:read"}, []string{healthy})
	s.deletionCancel()
	<-s.deletionDone
	s.stopUsage()
	c := testBufferConfig()
	c.FlushBytes = 1
	c.RetryMin, c.RetryMax = time.Millisecond, time.Millisecond
	c.DeterministicError = hostedDeterministicFlushError
	healthyFlushed := make(chan struct{}, 1)
	b := newTestBuffer(t, c, func(_ context.Context, batch acceptance.FlushBatch) error {
		if batch.ProjectID == broken {
			return lakeapi.ErrNativeEvidenceGap
		}
		healthyFlushed <- struct{}{}
		return nil
	})
	s.hosted = &hostedRuntime{buffer: b, flusher: &hostedLiveFlusher{policy: hostedIngestionPolicy{db: s.control}}}
	s.acceptance = b
	t.Cleanup(func() { s.hosted = nil })

	request(t, s, "POST", "/capture/", "", captureBody(brokenKey, "before-quarantine"), 200)
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for b.Snapshot().QuarantinedProjects[broken] == "" {
		select {
		case <-deadline.C:
			t.Fatal("quarantine did not take effect")
		case <-tick.C:
		}
	}
	r := httptest.NewRequest("POST", "/capture/", strings.NewReader(captureBody(brokenKey, "after-quarantine")))
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != 503 || w.Header().Get("Retry-After") == "" || !strings.Contains(w.Body.String(), "project ingestion is quarantined") {
		t.Fatalf("quarantined admission returned %d, %v, %s", w.Code, w.Header(), w.Body)
	}
	request(t, s, "GET", "/readyz", "", "", 200)
	request(t, s, "POST", "/capture/", "", captureBody(healthyKey, "healthy-commit"), 200)
	select {
	case <-healthyFlushed:
	case <-time.After(2 * time.Second):
		t.Fatal("healthy project did not flush")
	}

	status := request(t, s, "GET", "/api/v1/status", adminToken, "", 200)
	ingestion, _ := status["hosted_ingestion"].(map[string]any)
	quarantined, _ := ingestion["quarantined_projects"].(map[string]any)
	if status["status"] != "degraded" || len(quarantined) != 1 || quarantined[broken] != "native_evidence_gap" || ingestion["dropped_requests"] != float64(1) || ingestion["dropped_events"] != float64(1) {
		t.Fatal("incorrect hosted status", status)
	}
	if status = request(t, s, "GET", "/api/v1/status", reader, "", 200); status["hosted_ingestion"] != nil || status["status"] != "ok" {
		t.Fatal("project reader saw instance-admin details", status)
	}
}
