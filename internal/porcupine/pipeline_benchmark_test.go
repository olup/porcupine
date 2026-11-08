package porcupine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// BenchmarkPipelineCaptureDurable measures the normal HTTP capture handler with
// real SQLite WAL and synchronous=FULL. No delivery workers, HTTP listeners, or
// destination network are started. Client JSON generation and setup are excluded.
// Use -benchtime=100x -count=3 to keep every sample at or below 10,000 new events.
func BenchmarkPipelineCaptureDurable(b *testing.B) {
	for _, backlog := range []int{0, 5000} {
		for _, batchSize := range []int{1, 100} {
			for _, mode := range []string{"disabled", "unmatched", "event", "batch"} {
				b.Run(fmt.Sprintf("backlog_%d/events_%d/%s", backlog, batchSize, mode), func(b *testing.B) {
					benchmarkPipelineCapture(b, mode, backlog, batchSize)
				})
			}
		}
	}
}

type benchmarkNoNetwork struct{ calls atomic.Int64 }

func (n *benchmarkNoNetwork) RoundTrip(*http.Request) (*http.Response, error) {
	n.calls.Add(1)
	return nil, errors.New("network disabled in capture benchmark")
}

func benchmarkPipelineCapture(b *testing.B, mode string, backlog, batchSize int) {
	if b.N > 1000 {
		b.Skip("bounded benchmark: use -benchtime=100x or at most 1,000 capture requests")
	}
	ctx := context.Background()
	clock := time.Date(2026, 11, 8, 12, 0, 1, 0, time.UTC)
	s, err := Open(Config{DBPath: filepath.Join(b.TempDir(), "capture.db"), BootstrapToken: adminToken, QueueMaxEvents: 110000, Clock: func() time.Time { return clock }})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { s.Close() })
	projectID, token := "benchmark-project", "benchmark-ingestion-token"
	if _, err := s.db.Exec(`INSERT INTO projects(id,name,created_at) VALUES(?,?,?)`, projectID, "capture benchmark", clock.Format(time.RFC3339Nano)); err != nil {
		b.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO access_keys(id,name,token_hash,permissions,projects,created_at) VALUES('benchmark-key','benchmark',?,'["events:write"]','["benchmark-project"]',?)`, hash(token), clock.Format(time.RFC3339Nano)); err != nil {
		b.Fatal(err)
	}
	var noNetwork benchmarkNoNetwork
	if mode != "disabled" {
		// Attach after Open so the benchmark cannot start destination workers.
		rt, err := newPipelineRuntime(&PipelineConfig{Key: bytes.Repeat([]byte{0x42}, 32), WebhookMaxQueuedEvents: 110000, WebhookProjectMaxQueuedEvents: 110000, WebhookMaxQueuedBytes: 256 << 20, WebhookProjectMaxQueuedBytes: 256 << 20})
		if err != nil {
			b.Fatal(err)
		}
		rt.webhookClient = &http.Client{Transport: &noNetwork}
		s.pipelines = rt
		spec := WebhookPipelineSpec{DeliveryMode: mode, IncludeEvents: []string{"purchase"}}
		if mode == "unmatched" {
			spec.DeliveryMode, spec.IncludeEvents = "event", []string{"other-event"}
		}
		if mode == "batch" {
			spec.IntervalSeconds = 60
			spec.StartAt = clock.Truncate(time.Minute).Format(time.RFC3339)
		}
		config, err := json.Marshal(spec)
		if err != nil {
			b.Fatal(err)
		}
		var actorID string
		if err := s.db.QueryRow(`SELECT id FROM access_keys WHERE token_hash=?`, hash(adminToken)).Scan(&actorID); err != nil {
			b.Fatal(err)
		}
		if _, err := s.db.Exec(`INSERT INTO pipelines(id,project_id,actor_id,kind,name,state,config,secret,created_at_ms) VALUES('benchmark-pipeline',?,?,'webhook','capture benchmark','active',?,x'00',?)`, projectID, actorID, string(config), clock.UnixMilli()); err != nil {
			b.Fatal(err)
		}
	}
	for _, pragma := range []struct {
		name string
		want string
	}{{"journal_mode", "wal"}, {"synchronous", "2"}} {
		var value string
		if err := s.db.QueryRowContext(ctx, `PRAGMA `+pragma.name).Scan(&value); err != nil || value != pragma.want {
			b.Fatalf("durability mismatch %s=%s, want %s: %v", pragma.name, value, pragma.want, err)
		}
	}
	handler := s.Handler()
	capture := func(payload []byte) {
		// Advance receipt time without sleeping; keep the bounded run in one window
		// while avoiding artificial created_at ties for every queued chunk.
		clock = clock.Add(10 * time.Millisecond)
		r := httptest.NewRequest(http.MethodPost, "/batch/", bytes.NewReader(payload))
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code != http.StatusOK {
			b.Fatalf("capture failed: %d %s", w.Code, w.Body.String())
		}
	}
	for start := 0; start < backlog; start += 100 {
		capture(pipelineBenchmarkPayload(b, token, "seed", start, min(100, backlog-start)))
	}
	payloads := make([][]byte, b.N)
	for i := range payloads {
		payloads[i] = pipelineBenchmarkPayload(b, token, "measured", i*batchSize, batchSize)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(payloads[0])))
	b.ResetTimer()
	for _, payload := range payloads {
		capture(payload)
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N*batchSize)/b.Elapsed().Seconds(), "events/s")
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*batchSize), "ns/event")
	var accepted int
	if err := s.db.QueryRow(`SELECT count(*) FROM events`).Scan(&accepted); err != nil || accepted != backlog+b.N*batchSize {
		b.Fatalf("wrong accepted count %d: %v", accepted, err)
	}
	var queuedRows, queuedEvents int
	if err := s.db.QueryRow(`SELECT count(*),coalesce(sum(event_count),0) FROM webhook_deliveries`).Scan(&queuedRows, &queuedEvents); err != nil {
		b.Fatal(err)
	}
	if mode == "event" || mode == "batch" {
		if queuedEvents != accepted {
			b.Fatalf("matching webhook did not queue every accepted event: %d of %d", queuedEvents, accepted)
		}
		wantRows := accepted
		if mode == "batch" {
			wantRows = (accepted + webhookChunkEvents - 1) / webhookChunkEvents
		}
		if queuedRows != wantRows {
			b.Fatalf("wrong delivery mode grouping: got %d rows, want %d", queuedRows, wantRows)
		}
	} else if queuedRows != 0 {
		b.Fatal("unmatched or disabled pipeline queued events")
	}
	if mode == "batch" && backlog == 5000 && batchSize == 100 {
		benchmarkPipelineQueryPlan(b, s, `SELECT id,bytes,event_count FROM webhook_deliveries WHERE pipeline_id=? AND state='queued' AND frozen=0 AND window_end_ms=? AND event_count<100 AND bytes<1048576 ORDER BY created_at_ms DESC,id DESC LIMIT 1`, "benchmark-pipeline", clock.Truncate(time.Minute).Add(time.Minute).UnixMilli())
		benchmarkPipelineQueryPlan(b, s, `SELECT id,config FROM pipelines WHERE project_id=? AND kind='webhook' AND state='active' ORDER BY id`, projectID)
		benchmarkPipelineQueryPlan(b, s, `SELECT count,bytes FROM webhook_queue_usage WHERE scope=?`, "instance")
	}
	if calls := noNetwork.calls.Load(); calls != 0 {
		b.Fatalf("capture attempted %d destination requests", calls)
	}
}

func pipelineBenchmarkPayload(b *testing.B, token, prefix string, start, count int) []byte {
	b.Helper()
	events := make([]eventInput, 0, count)
	for i := range count {
		events = append(events, eventInput{UUID: fmt.Sprintf("%s-%08d", prefix, start+i), Event: "purchase", DistinctID: "benchmark-visitor", Timestamp: "2026-11-08T12:00:00Z", Properties: map[string]any{"amount": 49.95, "currency": "EUR", "path": "/checkout", "browser": "Firefox", "product": "annual", "context": strings.Repeat("x", 256)}})
	}
	payload, err := json.Marshal(ingestInput{eventInput: eventInput{APIKey: token}, Batch: events})
	if err != nil {
		b.Fatal(err)
	}
	return payload
}

func benchmarkPipelineQueryPlan(b *testing.B, s *Service, statement string, args ...any) {
	b.Helper()
	rows, err := s.db.Query("EXPLAIN QUERY PLAN "+statement, args...)
	if err != nil {
		b.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			b.Fatal(err)
		}
		b.Log(detail)
	}
	if err := rows.Err(); err != nil {
		b.Fatal(err)
	}
}
