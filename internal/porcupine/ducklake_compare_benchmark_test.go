//go:build ducklake

package porcupine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The Docker runner selects one engine at a time within the same resource budget.
func BenchmarkAnalyticsComparison(b *testing.B) {
	engine := os.Getenv("PORCUPINE_BENCH_ENGINE")
	if engine != "ducklake" && engine != "clickhouse" {
		b.Skip("use make ducklake-performance-compare")
	}
	b.Run(engine, func(b *testing.B) {
		b.Run("ingestion100", func(b *testing.B) {
			if b.N > 100 {
				b.Fatal("use a fixed iteration count of at most 100")
			}
			s, project, token := comparisonService(b, engine)
			handler := s.Handler()
			query := "SELECT count(*) AS total FROM events"
			comparisonQuery(b, handler, project, query)
			payloads := comparisonPayloads(b.N)
			b.SetBytes(int64(len(payloads[0])))
			b.ResetTimer()
			for _, payload := range payloads {
				comparisonCapture(b, s, handler, token, payload)
			}
			b.StopTimer()
			b.ReportMetric(float64(b.N*100)/b.Elapsed().Seconds(), "events/s")
			result := comparisonQuery(b, handler, project, query)
			if len(result.Data) != 1 || string(result.Data[0]["total"]) != fmt.Sprintf("\"%d\"", b.N*100) {
				b.Fatalf("incorrect projected count: %+v", result.Data)
			}
		})
		queries := []struct {
			name, duckdb, clickhouse string
			rows                     uint64
		}{
			{"counts10000", "SELECT count(*) AS total,count(DISTINCT distinct_id) AS users FROM events", "SELECT count() AS total,uniqExact(distinct_id) AS users FROM events", 1},
			{"trend10000", "SELECT event,date_trunc('hour',event_time) AS hour,count(*) AS total,sum(CAST(json_extract_string(properties,'$.amount') AS DOUBLE)) AS amount FROM events GROUP BY event,hour ORDER BY hour,event", "SELECT event,toStartOfHour(event_time) AS hour,count() AS total,sum(JSONExtractFloat(properties,'amount')) AS amount FROM events GROUP BY event,hour ORDER BY hour,event", 9},
			{"sessions10000", "SELECT count(DISTINCT json_extract_string(context,'$.session_id')) AS total FROM events", "SELECT uniqExact(JSONExtractString(context,'session_id')) AS total FROM events", 1},
		}
		s, project, token := comparisonService(b, engine)
		handler := s.Handler()
		for _, payload := range comparisonPayloads(100) {
			comparisonCapture(b, s, handler, token, payload)
		}
		totals := comparisonQuery(b, handler, project, "SELECT count(*) AS total FROM events")
		if len(totals.Data) != 1 || string(totals.Data[0]["total"]) != `"10000"` {
			b.Fatal("incomplete analytical fixture")
		}
		for _, query := range queries {
			b.Run(query.name, func(b *testing.B) {
				sql := query.duckdb
				if engine == "clickhouse" {
					sql = query.clickhouse
				}
				result := comparisonQuery(b, handler, project, sql)
				if result.Rows != query.rows {
					b.Fatalf("incorrect row count: %d", result.Rows)
				}
				if query.name == "counts10000" && (string(result.Data[0]["total"]) != `"10000"` || string(result.Data[0]["users"]) != `"100"`) {
					b.Fatal("incorrect fixture totals")
				}
				if query.name == "sessions10000" && string(result.Data[0]["total"]) != `"100"` {
					b.Fatal("incorrect session count")
				}
				b.ResetTimer()
				for range b.N {
					result = comparisonQuery(b, handler, project, sql)
					if result.Rows != query.rows {
						b.Fatal("query result changed")
					}
				}
				b.StopTimer()
			})
		}
	})
}

func comparisonService(b *testing.B, engine string) (*Service, string, string) {
	b.Helper()
	s, err := Open(Config{DBPath: filepath.Join(b.TempDir(), "queue.db"), BootstrapToken: adminToken, QueueMaxEvents: 1000})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := s.Close(); err != nil {
			b.Error(err)
		}
	})
	if engine == "ducklake" {
		s.lake, err = newDuckLake(DuckLakeConfig{Directory: filepath.Join(b.TempDir(), "lake")})
	} else {
		var ch *clickHouse
		// Fixed loopback endpoint supplied only by the disposable Docker runner.
		ch, err = newClickHouse(ClickHouseConfig{URL: "http://127.0.0.1:8123", Database: "bench_" + strings.ReplaceAll(newID(), "-", "")})
		if err == nil {
			configureAnalyticsTest(s, ch)
			ch.projectionGuard = s.projectionGuard
		}
		b.Cleanup(func() {
			if ch != nil {
				_, _ = ch.admin(context.Background(), "DROP DATABASE IF EXISTS "+ch.config.Database+" SYNC")
			}
		})
	}
	if err != nil {
		b.Fatal(err)
	}
	handler := s.Handler()
	w := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/api/v1/projects", strings.NewReader(`{"name":"synthetic comparison"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+adminToken)
	handler.ServeHTTP(w, req)
	var response struct {
		Project struct {
			ID string `json:"id"`
		} `json:"project"`
		Token string `json:"ingest_token"`
	}
	if w.Code != 201 || json.Unmarshal(w.Body.Bytes(), &response) != nil || response.Project.ID == "" || response.Token == "" {
		b.Fatal("project creation failed", w.Code)
	}
	return s, response.Project.ID, response.Token
}

func comparisonPayloads(batches int) [][]byte {
	payloads := make([][]byte, batches)
	for i := range payloads {
		batch := make([]map[string]any, 100)
		for j := range batch {
			n := i*100 + j
			batch[j] = map[string]any{
				"type": "track", "message_id": fmt.Sprintf("event-%06d", n), "actor_id": fmt.Sprintf("visitor-%03d", n%100),
				"name": []string{"visit", "signup", "purchase"}[n%3], "timestamp": time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(n) * time.Second).Format(time.RFC3339),
				"properties": map[string]any{"amount": 49.95, "currency": "EUR", "plan": "annual", "path": "/checkout"},
				"context":    map[string]any{"session_id": fmt.Sprintf("session-%03d", n%100), "browser": map[string]any{"name": "Firefox"}, "locale": "fr-FR"},
			}
		}
		payloads[i], _ = json.Marshal(batch)
	}
	return payloads
}

func comparisonCapture(b *testing.B, s *Service, handler http.Handler, token string, payload []byte) {
	b.Helper()
	req := httptest.NewRequest("POST", "/ingest/v1/events", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Porcupine-Key", token)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 200 {
		b.Fatalf("capture failed: %d %s", w.Code, w.Body.String())
	}
	if err := s.flushDelivery(context.Background()); err != nil {
		b.Fatal(err)
	}
	var pending int
	if err := s.db.QueryRow("SELECT count(*) FROM events").Scan(&pending); err != nil || pending != 0 {
		b.Fatalf("pending=%d error=%v", pending, err)
	}
}

func comparisonQuery(b *testing.B, handler http.Handler, project, sql string) AnalyticsResult {
	b.Helper()
	body, err := json.Marshal(map[string]any{"sql": sql, "use_cache": false})
	if err != nil {
		b.Fatal(err)
	}
	req := httptest.NewRequest("POST", "/api/v1/projects/"+project+"/query", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+adminToken)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	var response AnalyticsQuery
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &response) != nil {
		b.Fatalf("query failed: %d %s", w.Code, w.Body.String())
	}
	return response.Result
}
