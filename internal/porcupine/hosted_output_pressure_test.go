package porcupine

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/acceptance"
	eventapi "github.com/olup/porcupine/internal/event"
	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/output"
)

func TestDuckLakeS3SharedPostgresHostedOutputPressure(t *testing.T) {
	t.Parallel()
	skipInShort(t)

	for _, tc := range []struct {
		reason, mode string
		projectRefs  int
		interval     int64
	}{
		{reason: "reservation_limit", mode: "batch", projectRefs: 1000, interval: 60},
		{reason: "manifest_limit", mode: "event", projectRefs: 5000},
	} {
		t.Run(tc.reason, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			cfg := hostedJourneyConfig(t, sharedPostgresDSN(t), nativeS3Fixture(t))
			cfg.OAuth = &OAuthConfig{Issuer: "http://127.0.0.1:42999"}
			cfg.MetricsEnabled = true
			cfg.Pipelines = &output.PipelineConfig{Key: bytes.Repeat([]byte{1}, 32), WebhookProjectMaxQueuedEvents: tc.projectRefs}
			bufferCfg := acceptance.DefaultBufferConfig()
			bufferCfg.FlushBytes = 1
			s, host, _ := hostedCleanupWorkerWithBuffer(t, cfg, bufferCfg)
			s.pipelines.Stop()

			admin := accountTestToken(t, s)
			project, ingest := sharedJourneyProjectAs(t, host.URL, admin, defaultTenantID, tc.reason)
			_, creator := sharedJourneyServiceKey(t, host.URL, admin, defaultTenantID, "pressure webhook", project, []string{"pipelines:write", "events:read"})
			pipelines := "/api/v1/projects/" + project + "/pipelines"
			for i := range 2 {
				body, _ := json.Marshal(PipelineCreateBody{Name: fmt.Sprintf("catch-all %d", i), Kind: "webhook", Webhook: &output.WebhookPipelineSpec{
					Endpoint: "https://example.test/unused", DeliveryMode: tc.mode, IntervalSeconds: tc.interval,
				}})
				sharedJourneyHTTP(t, host.URL, "POST", pipelines, creator, body, nil, 201)
			}
			db := s.control
			// The proof for the declined output set must have room even if existing
			// output jobs occupy every dimension of the project budget.
			if _, err := db.ExecContext(ctx, `UPDATE pipeline_capacity SET used_refs=limit_refs,used_bytes=limit_bytes,used_rows=limit_rows,used_artifacts=limit_artifacts,used_proofs=limit_proofs WHERE scope_kind='project' AND scope_id=$1`, project); err != nil {
				t.Fatal(err)
			}

			events := make(eventBatch, 1000)
			for i := range events {
				events[i] = eventInput{UUID: eventapi.NewID(), Event: "pressure", DistinctID: "person"}
			}
			send := func(events eventBatch) {
				t.Helper()
				body, err := json.Marshal(ingestInput{eventInput: eventInput{APIKey: ingest}, Batch: events})
				if err != nil {
					t.Fatal(err)
				}
				ack := sharedJourneyHTTP(t, host.URL, "POST", "/batch/", "", body, nil, 200)
				if ack["durability"] != "buffered" || ack["received"] != float64(len(events)) {
					t.Fatal("request did not receive a buffered acknowledgment", ack)
				}
				deadline := time.Now().Add(12 * time.Second)
				for until := s.hosted.buffer.Snapshot(); until.Events > 0; until = s.hosted.buffer.Snapshot() {
					if time.Now().After(deadline) {
						t.Fatalf("accepted batch is still buffered: %+v", until)
					}
					time.Sleep(10 * time.Millisecond)
				}
			}
			assertCounts := func(wantEvents, wantJobs int) {
				t.Helper()
				result, err := s.lake.Query(ctx, project, "SELECT count(*) AS n FROM events", nil, lakeapi.DefaultQueryExecutionLimits())
				if err != nil {
					t.Fatal(err)
				}
				var jobs, jobIDs int
				if err = db.QueryRowContext(ctx, `SELECT count(*),count(DISTINCT id) FROM pipeline_effect_jobs WHERE project_id=$1`, project).Scan(&jobs, &jobIDs); err != nil {
					t.Fatal(err)
				}
				snapshot := s.hosted.buffer.Snapshot()
				if len(result.Data) != 1 {
					t.Fatal("missing SQL count", result.Data)
				}
				gotEvents := string(result.Data[0]["n"])
				t.Logf("queryable_events=%s dropped_events=%d jobs=%d", gotEvents, snapshot.DroppedEvents, jobs)
				if gotEvents != strconv.Quote(strconv.Itoa(wantEvents)) ||
					snapshot.DroppedEvents != 0 || snapshot.DroppedRequests != 0 || jobs != wantJobs || jobIDs != wantJobs {
					t.Fatalf("expected %d committed events, zero discarded events, and %d unique jobs", wantEvents, wantJobs)
				}
			}

			send(events)
			assertCounts(1000, 0)
			send(events)
			assertCounts(1000, 0)
			metrics := scrape(t, s)
			requireMetric(t, metrics, fmt.Sprintf(`porcupine_webhook_refused_references_total{profile="hosted",reason=%q} 2000`, tc.reason))
			if strings.Contains(metrics, `reason="oversized"`) {
				t.Fatal("output limit was recorded as lost ingestion")
			}
			if _, err := db.ExecContext(ctx, `UPDATE pipeline_capacity SET used_refs=0,used_bytes=0,used_rows=1,used_artifacts=0,used_proofs=0 WHERE scope_kind='project' AND scope_id=$1`, project); err != nil {
				t.Fatal(err)
			}
			ordinary := eventBatch{{UUID: eventapi.NewID(), Event: "pressure", DistinctID: "person"}}
			send(ordinary)
			assertCounts(1001, 2)

			if tc.mode == "event" {
				deliveries := map[string]bool{}
				client := &http.Client{Transport: hostedWebhookRoundTrip(func(r *http.Request) (*http.Response, error) {
					id := r.Header.Get("Idempotency-Key")
					if id == "" || deliveries[id] {
						t.Errorf("duplicate or missing delivery ID %q", id)
					}
					deliveries[id] = true
					return &http.Response{StatusCode: http.StatusNoContent, Body: io.NopCloser(strings.NewReader(""))}, nil
				})}
				f := s.hosted.flusher
				worker := newHostedPipelineDelivery(f, s.pipelines.HostedWebhookDestinationTx, output.HostedWebhookLimits{MaxAttempts: 3, Timeout: 3 * time.Second}, client, hostedOutputBlocked, s.hosted.incarnation.registerTx, f.finishProjectActivity)
				for range 2 {
					if progress, err := worker.Step(ctx); err != nil || !progress {
						t.Fatal("webhook delivery did not progress", progress, err)
					}
				}
				send(ordinary)
				if progress, err := worker.Step(ctx); err != nil || progress || len(deliveries) != 2 {
					t.Fatal("replayed ingestion emitted a new delivery", progress, err, deliveries)
				}
			} else {
				send(ordinary)
			}
			assertCounts(1001, 2)
		})
	}
}
