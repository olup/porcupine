package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/acceptance"
	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/output"
)

func TestHostedFlushError(t *testing.T) {
	t.Parallel()
	publisher := &hostedEffectPublisher{}
	_, bindingMismatch := publisher.verify(t.Context(), output.PipelineBinding{}, lakeapi.NativeCommitEvidence{}, lakeapi.ImmutableObjectRef{})
	for _, test := range []struct {
		err              error
		project, request string
	}{
		{fmt.Errorf("checkpoint: %w", lakeapi.ErrNativeEvidenceGap), "native_evidence_gap", ""},
		{errors.Join(bindingMismatch, nil), "pipeline_evidence_conflict", ""},
		{fmt.Errorf("prepare: %w", lakeapi.ErrInvalidLiveProjection), "", "invalid_live_projection"},
		{errors.Join(lakeapi.ErrNativeEvidenceGap, sql.ErrConnDone), "native_evidence_gap", ""},
		{errors.Join(bindingMismatch, context.Canceled), "pipeline_evidence_conflict", ""},
		{errors.Join(lakeapi.ErrInvalidLiveProjection, sql.ErrConnDone), "", "invalid_live_projection"},
		{sql.ErrConnDone, "", ""},
		{context.DeadlineExceeded, "", ""},
		{lakeapi.ErrProjectWriteSessionLost, "", ""},
		{lakeapi.ErrProjectWriteFenceStale, "", ""},
		{output.ErrPipelineConflict, "", ""},
		{output.ErrPipelineCapacity, "", ""},
		{output.ErrPipelineDemandTooLarge, "", ""},
		{errors.New("S3 request failed"), "", ""},
	} {
		if project, request := hostedFlushError(test.err); project != test.project || request != test.request {
			t.Errorf("%v classified %q/%q, want %q/%q", test.err, project, request, test.project, test.request)
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
	c.ClassifyFlushError = hostedFlushError
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
	if w.Code != 503 || w.Header().Get("Retry-After") == "" || !strings.Contains(w.Body.String(), `"code":"project_ingestion_quarantined"`) {
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
	if status["status"] != "degraded" || len(quarantined) != 1 || quarantined[broken] != "native_evidence_gap" || ingestion["dropped_requests"] != float64(0) || ingestion["dropped_events"] != float64(0) {
		t.Fatal("incorrect hosted status", status)
	}
	if status = request(t, s, "GET", "/api/v1/status", reader, "", 200); status["hosted_ingestion"] != nil || status["status"] != "ok" {
		t.Fatal("project reader saw instance-admin details", status)
	}
}

func TestDuckLakeS3SharedPostgresHostedQuarantineInvalidProjection(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	c := testBufferConfig()
	c.FlushBytes = 32 << 10
	a, b := sharedNativeJourneyPairWithBuffer(t, nativeS3Fixture(t), c)
	project, ingest := sharedJourneyProject(t, a.host.URL, defaultTenantID, "projection validation")
	_, reader := sharedJourneyKey(t, b.host.URL, project, []string{"events:read"})
	var good []string
	for i := range 10 {
		if i == 5 {
			// Normalizing the offset yields year 10000, which the native
			// projection cannot parse back from RFC3339.
			payload := fmt.Sprintf(`{"api_key":%q,"uuid":"bad","event":"page","distinct_id":"visitor","timestamp":"9999-12-31T23:59:59-01:00"}`, ingest)
			rejection := sharedJourneyHTTP(t, a.host.URL, "POST", "/capture/", "", []byte(payload), nil, 400)
			if rejection["code"] != "invalid_live_projection" {
				t.Fatal("unexpected projection error", rejection)
			}
		}
		padding := ""
		if i == 9 {
			if got := a.service.hosted.buffer.Snapshot().Events; got != 9 {
				t.Fatalf("pending batch contains %d events, want 9", got)
			}
			padding = strings.Repeat("x", int(c.FlushBytes))
		}
		uuid := fmt.Sprintf("good-%d", i)
		good = append(good, uuid)
		payload := fmt.Sprintf(`{"api_key":%q,"uuid":%q,"event":"page","distinct_id":"visitor","properties":{"pad":%q}}`, ingest, uuid, padding)
		sharedJourneyHTTP(t, a.host.URL, "POST", "/capture/", "", []byte(payload), nil, 200)
	}
	waitHostedQuarantineState(t, a, func(s acceptance.BufferSnapshot) bool { return s.Events == 0 })
	assertHostedQuarantineRows(t, b, project, reader, good...)
	t.Logf("one invalid request refused before admission, %d healthy requests queryable from the other worker", len(good))
}

func TestDuckLakeS3SharedPostgresHostedQuarantineEvidenceGap(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	c := testBufferConfig()
	c.FlushBytes = 1
	c.RetryMin, c.RetryMax = 20*time.Millisecond, 100*time.Millisecond
	a, b := sharedNativeJourneyPairWithBuffer(t, nativeS3Fixture(t), c)
	project, ingest := sharedJourneyProject(t, a.host.URL, defaultTenantID, "checkpoint repair")
	_, reader := sharedJourneyKey(t, b.host.URL, project, []string{"events:read"})
	capture := func(n *sharedJourneyNode, uuid string, status int) map[string]any {
		t.Helper()
		return sharedJourneyHTTP(t, n.host.URL, "POST", "/capture/", "", []byte(captureBody(ingest, uuid)), nil, status)
	}
	capture(a, "seed", 200)
	waitHostedQuarantineState(t, a, func(s acceptance.BufferSnapshot) bool { return s.Events == 0 })
	const update = `UPDATE pipeline_effect_streams SET proof_sha=$1 WHERE project_id=$2`
	var original string
	if err := a.service.control.QueryRow(`SELECT proof_sha FROM pipeline_effect_streams WHERE project_id=$1`, project).Scan(&original); err != nil {
		t.Fatal(err)
	}
	if _, err := a.service.control.Exec(update, strings.Repeat("f", 64), project); err != nil {
		t.Fatal(err)
	}
	repair := func() {
		t.Helper()
		if _, err := a.service.control.Exec(update+` AND proof_sha=repeat('f',64)`, original, project); err != nil {
			t.Error("repair proof hash", err)
		}
	}
	t.Cleanup(repair)
	capture(a, "retained", 200)
	state := waitHostedQuarantineState(t, a, func(s acceptance.BufferSnapshot) bool { return s.QuarantinedProjects[project] == "native_evidence_gap" })
	if state.Events != 1 || state.DroppedEvents != 0 || state.DroppedRequests != 0 {
		t.Fatal("evidence gap lost an acknowledged event", state)
	}
	if reply := capture(b, "remote-rejected", 503); reply["code"] != "project_ingestion_quarantined" {
		t.Fatal("remote worker returned an unexpected code", reply)
	}
	status := sharedJourneyHTTP(t, b.host.URL, "GET", "/api/v1/status", adminToken, nil, nil, 200)
	ingestion, _ := status["hosted_ingestion"].(map[string]any)
	quarantined, _ := ingestion["quarantined_projects"].(map[string]any)
	if status["status"] != "degraded" || quarantined[project] != "native_evidence_gap" {
		t.Fatal("remote worker did not report the shared evidence gap", status)
	}
	repair()
	waitHostedQuarantineState(t, a, func(s acceptance.BufferSnapshot) bool { return s.Events == 0 && len(s.QuarantinedProjects) == 0 })
	assertHostedQuarantineRows(t, b, project, reader, "retained", "seed")
	capture(b, "after-repair", 200)
	waitHostedQuarantineState(t, b, func(s acceptance.BufferSnapshot) bool { return s.Events == 0 })
	assertHostedQuarantineRows(t, b, project, reader, "after-repair", "retained", "seed")
	t.Logf("during quarantine %d events retained, %d dropped, and the second worker denied admission. All three accepted events are queryable after repair", state.Events, state.DroppedEvents)
}

func waitHostedQuarantineState(t *testing.T, n *sharedJourneyNode, predicate func(acceptance.BufferSnapshot) bool) acceptance.BufferSnapshot {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		state := n.service.hosted.buffer.Snapshot()
		if predicate(state) {
			return state
		}
		if time.Now().After(deadline) {
			t.Fatal("buffer state deadline", state)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func assertHostedQuarantineRows(t *testing.T, n *sharedJourneyNode, project, reader string, want ...string) {
	t.Helper()
	result := sharedJourneyQuery(t, n.host.URL, project, reader, "SELECT uuid FROM events ORDER BY uuid", 200)
	rows, _ := result["result"].(map[string]any)["data"].([]any)
	var ids []string
	for _, row := range rows {
		ids = append(ids, row.(map[string]any)["uuid"].(string))
	}
	if !slices.Equal(ids, want) {
		t.Fatalf("query UUIDs = %v, want %v", ids, want)
	}
}
