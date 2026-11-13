//go:build ducklake

package porcupine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

// Run bounded microbenchmarks with:
// go test -tags ducklake ./internal/porcupine -run '^$' -bench '^BenchmarkDuckLake' -benchtime=10x -benchmem
// Setup, extension loading, payload generation, and final verification are not
// timed. The query benchmark uses a warm, synthetic 10,000-row local lake.
func duckLakeBenchmarkService(b *testing.B) (*Service, string, string) {
	b.Helper()
	s, err := Open(Config{DBPath: filepath.Join(b.TempDir(), "queue.db"), BootstrapToken: adminToken, QueueMaxEvents: 1000})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { s.Close() })
	lake, err := newDuckLake(DuckLakeConfig{Directory: filepath.Join(b.TempDir(), "lake")})
	if err != nil {
		b.Fatal(err)
	}
	s.lake = lake
	req := httptest.NewRequest(http.MethodPost, "/api/v1/projects", bytes.NewBufferString(`{"name":"synthetic benchmark"}`))
	req.Header.Set("Authorization", "Bearer "+adminToken)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, req)
	if w.Code != 201 {
		b.Fatalf("create benchmark project: %d %s", w.Code, w.Body.String())
	}
	var response struct {
		Project struct {
			ID string `json:"id"`
		} `json:"project"`
		Token string `json:"ingest_token"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil || response.Project.ID == "" || response.Token == "" {
		b.Fatal("invalid benchmark project response", err)
	}
	return s, response.Project.ID, response.Token
}

func BenchmarkDuckLakeNativeHTTPBatch100AndProjection(b *testing.B) {
	if b.N > 100 {
		b.Skip("bounded benchmark: use -benchtime=10x, at most 100 iterations")
	}
	s, projectID, token := duckLakeBenchmarkService(b)
	// Open the project engine before timing, so this measures the hot path.
	if _, err := s.lake.Query(context.Background(), projectID, "SELECT count(*) AS total FROM events", nil); err != nil {
		b.Fatal(err)
	}
	payloads := make([][]byte, b.N)
	for i := range payloads {
		batch := make([]map[string]any, 100)
		for j := range batch {
			batch[j] = map[string]any{
				"type": "track", "message_id": fmt.Sprintf("batch-%012d-%03d", i, j),
				"actor_id": fmt.Sprintf("visitor-%03d", j), "name": "purchase", "timestamp": "2026-11-01T00:00:00Z",
				"properties": map[string]any{"amount": 49.95, "currency": "EUR", "plan": "annual", "path": "/checkout"},
				"context":    map[string]any{"session_id": fmt.Sprintf("session-%03d", j), "browser": map[string]any{"name": "Firefox"}, "locale": "fr-FR"},
			}
		}
		var err error
		payloads[i], err = json.Marshal(batch)
		if err != nil {
			b.Fatal(err)
		}
	}
	handler := s.Handler()
	b.ReportAllocs()
	b.SetBytes(int64(len(payloads[0])))
	b.ResetTimer()
	for _, payload := range payloads {
		req := httptest.NewRequest(http.MethodPost, "/ingest/v1/events", bytes.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Porcupine-Key", token)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			b.Fatalf("native batch: %d %s", w.Code, w.Body.String())
		}
		if err := s.flushDelivery(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(100, "events/op")
	b.ReportMetric(float64(b.N*100)/b.Elapsed().Seconds(), "events/s")
	result, err := s.lake.Query(context.Background(), projectID, "SELECT count(*) AS total FROM events", nil)
	if err != nil || len(result.Data) != 1 || string(result.Data[0]["total"]) != fmt.Sprintf("\"%d\"", b.N*100) {
		b.Fatalf("projection count does not match accepted batches: %+v, %v", result, err)
	}
	var pending int
	if err := s.db.QueryRow("SELECT count(*) FROM events").Scan(&pending); err != nil || pending != 0 {
		b.Fatalf("projection left %d accepted events pending: %v", pending, err)
	}
}

func BenchmarkDuckLakeWarmAggregation10000(b *testing.B) {
	s, projectID, _ := duckLakeBenchmarkService(b)
	store := s.lake.(*duckLakeStore)
	p, release, err := store.leaseProject(context.Background(), projectID)
	if err != nil {
		b.Fatal(err)
	}
	// Seed only the analytical fixture using trusted bulk SQL. This setup does
	// not benchmark acceptance, receipt creation, or per-event projection.
	_, err = p.db.ExecContext(context.Background(), `INSERT INTO lake.current_events
WITH fixture AS (SELECT i, TIMESTAMPTZ '2026-11-01 00:00:00+00' + i * INTERVAL '1 second' AS ts FROM range(10000) AS r(i))
SELECT i+1, ?, 'fixture-' || CAST(i AS VARCHAR),
 CASE i%3 WHEN 0 THEN 'visit' WHEN 1 THEN 'signup' ELSE 'purchase' END,
 'visitor-' || CAST(i%100 AS VARCHAR), '{"amount":49.95,"locale":"fr-FR"}',
 strftime(ts,'%Y-%m-%dT%H:%M:%SZ'), epoch_ms(ts), ts,
 strftime(ts,'%Y-%m-%dT%H:%M:%SZ'), epoch_ms(ts), 0,
 'native-benchmark', 'fixture-' || CAST(i AS VARCHAR), 'track', '{"session_id":"benchmark-session"}', 0, '', '{}'
FROM fixture`, projectID)
	release()
	if err != nil {
		b.Fatal(err)
	}
	query := "SELECT event,date_trunc('hour',event_time) AS hour,count(*) AS total,sum(CAST(json_extract_string(properties,'$.amount') AS DOUBLE)) AS amount FROM events GROUP BY event,hour ORDER BY hour,event"
	if _, err := s.lake.Query(context.Background(), projectID, query, nil); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := s.lake.Query(context.Background(), projectID, query, nil)
		if err != nil || result.Rows != 9 {
			b.Fatalf("aggregation result: %d rows, %v", result.Rows, err)
		}
	}
	b.StopTimer()
	b.ReportMetric(10000, "fixture_rows")
}
