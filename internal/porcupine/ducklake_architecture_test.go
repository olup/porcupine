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
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type architectureQuery struct{ name, duckdb, clickhouse string }

var architectureQueries = []architectureQuery{
	{"counts", "SELECT count(*) AS total,count(DISTINCT distinct_id) AS actors FROM events", "SELECT count() AS total,uniqExact(distinct_id) AS actors FROM events"},
	{"dimensions", "SELECT CAST(event_time AS DATE) AS day,event,json_extract_string(properties,'$.country') AS country,count(*) AS total FROM events GROUP BY day,event,country ORDER BY day,event,country", "SELECT toDate(event_time) AS day,event,JSONExtractString(properties,'country') AS country,count() AS total FROM events GROUP BY day,event,country ORDER BY day,event,country"},
	{"sessions", "SELECT count(DISTINCT json_extract_string(context,'$.session_id')) AS total FROM events", "SELECT uniqExact(JSONExtractString(context,'session_id')) AS total FROM events"},
	{"ordered_funnel", `WITH visits AS (SELECT distinct_id,min(event_time) AS t FROM events WHERE event='page_viewed' GROUP BY distinct_id), signups AS (SELECT e.distinct_id,min(e.event_time) AS t FROM events e JOIN visits v ON e.distinct_id=v.distinct_id WHERE e.event='signup_completed' AND e.event_time>=v.t AND e.event_time<=v.t+INTERVAL '10 minutes' GROUP BY e.distinct_id), purchases AS (SELECT e.distinct_id FROM events e JOIN signups s ON e.distinct_id=s.distinct_id WHERE e.event='purchase' AND e.event_time>=s.t AND e.event_time<=s.t+INTERVAL '10 minutes' GROUP BY e.distinct_id) SELECT count(*) AS total FROM purchases`, `WITH visits AS (SELECT distinct_id,min(event_time) AS t FROM events WHERE event='page_viewed' GROUP BY distinct_id), signups AS (SELECT e.distinct_id,min(e.event_time) AS t FROM events e JOIN visits v ON e.distinct_id=v.distinct_id WHERE e.event='signup_completed' AND e.event_time>=v.t AND e.event_time<=addMinutes(v.t,10) GROUP BY e.distinct_id), purchases AS (SELECT e.distinct_id FROM events e JOIN signups s ON e.distinct_id=s.distinct_id WHERE e.event='purchase' AND e.event_time>=s.t AND e.event_time<=addMinutes(s.t,10) GROUP BY e.distinct_id) SELECT count() AS total FROM purchases`},
	{"next_day_retention", `WITH cohort AS (SELECT distinct_id,min(CAST(event_time AS DATE)) AS first_day FROM events WHERE event='page_viewed' GROUP BY distinct_id) SELECT count(DISTINCT e.distinct_id) AS total FROM events e JOIN cohort c ON e.distinct_id=c.distinct_id WHERE e.event='page_viewed' AND CAST(e.event_time AS DATE)=c.first_day+INTERVAL '1 day'`, `WITH cohort AS (SELECT distinct_id,min(toDate(event_time)) AS first_day FROM events WHERE event='page_viewed' GROUP BY distinct_id) SELECT uniqExact(e.distinct_id) AS total FROM events e JOIN cohort c ON e.distinct_id=c.distinct_id WHERE e.event='page_viewed' AND toDate(e.event_time)=addDays(c.first_day,1)`},
}

func (q architectureQuery) sql(engine string) string {
	if engine == "clickhouse" {
		return q.clickhouse
	}
	return q.duckdb
}

func architectureEmit(phase, engine string, data map[string]any) {
	data["phase"], data["engine"] = phase, engine
	data["time"] = time.Now().UTC().Format(time.RFC3339)
	raw, _ := json.Marshal(data)
	fmt.Printf("ARCH_RESULT %s\n", raw)
}

func architectureResources(roots ...string) map[string]any {
	result := map[string]any{}
	for _, name := range []string{"memory.current", "memory.peak", "cpu.stat", "memory.events"} {
		if raw, err := os.ReadFile("/sys/fs/cgroup/" + name); err == nil {
			result[name] = strings.TrimSpace(string(raw))
		}
	}
	var size, files int64
	for _, root := range roots {
		_ = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
			if err == nil && !entry.IsDir() {
				if info, err := entry.Info(); err == nil {
					size += info.Size()
					files++
				}
			}
			return nil
		})
	}
	result["managed_disk_bytes"], result["managed_files"] = size, files
	return result
}

func architectureHTTP(handler http.Handler, method, path, token string, body []byte, ingest bool) (int, []byte, time.Duration) {
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if ingest {
		req.Header.Set("X-Porcupine-Key", token)
	} else {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	w := httptest.NewRecorder()
	start := time.Now()
	handler.ServeHTTP(w, req)
	return w.Code, w.Body.Bytes(), time.Since(start)
}

func architectureRunQuery(handler http.Handler, token, project, sql string) (int, AnalyticsResult, time.Duration) {
	body, _ := json.Marshal(map[string]any{"sql": sql, "use_cache": false})
	status, raw, elapsed := architectureHTTP(handler, "POST", "/api/v1/projects/"+project+"/query", token, body, false)
	var response AnalyticsQuery
	if status == 200 && json.Unmarshal(raw, &response) != nil {
		return 599, AnalyticsResult{}, elapsed
	}
	return status, response.Result, elapsed
}

func architectureCount(t *testing.T, handler http.Handler, project string) int {
	t.Helper()
	code, result, _ := architectureRunQuery(handler, adminToken, project, "SELECT count(*) AS total FROM events")
	if code != 200 || len(result.Data) != 1 {
		t.Fatalf("count failed: HTTP %d", code)
	}
	var text string
	if err := json.Unmarshal(result.Data[0]["total"], &text); err != nil {
		t.Fatal(err)
	}
	n, err := strconv.Atoi(text)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func architectureLatency(samples []time.Duration) map[string]any {
	if len(samples) == 0 {
		return map[string]any{"samples": 0}
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	return map[string]any{"samples": len(samples), "p50_ms": float64(samples[(len(samples)-1)/2]) / float64(time.Millisecond), "p95_ms": float64(samples[(95*len(samples)+99)/100-1]) / float64(time.Millisecond), "max_ms": float64(samples[len(samples)-1]) / float64(time.Millisecond)}
}

// Bulk fixture setup is intentionally distinct from measured durable ingestion.
// Both dialects synthesize the same observable event fields and properties.
func architectureSeed(t *testing.T, s *Service, project string, from, to int) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	start := time.Now()
	var err error
	if s.lake != nil {
		store := s.lake.(*duckLakeStore)
		p, release, leaseErr := store.leaseProject(ctx, project)
		if leaseErr != nil {
			t.Fatal(leaseErr)
		}
		defer release()
		for offset := from; offset < to && err == nil; offset += 10000 {
			end := min(offset+10000, to)
			tx, beginErr := p.db.BeginTx(ctx, nil)
			if beginErr != nil {
				err = beginErr
				break
			}
			_, err = tx.ExecContext(ctx, `INSERT INTO lake.current_events
WITH base AS (SELECT i, TIMESTAMPTZ '2026-11-01 00:00:00+00'+i*INTERVAL '1 second' AS ts,
 CASE i%3 WHEN 0 THEN 'page_viewed' WHEN 1 THEN 'signup_completed' ELSE 'purchase' END AS ev FROM range(?,?) r(i)),
fixture AS (SELECT *, 'fixture-'||CAST(i AS VARCHAR) AS uid,'actor-'||CAST(CAST(floor(i/3) AS BIGINT)%10000 AS VARCHAR) AS actor,
 CAST(json_object('country','country-'||CAST(i%7 AS VARCHAR),'plan',CASE WHEN i%2=0 THEN 'free' ELSE 'pro' END,'amount',49.95,'duration_ms',i%5000) AS VARCHAR) AS props,
 CAST(json_object('session_id','session-'||CAST(CAST(floor(i/3) AS BIGINT) AS VARCHAR),'locale','fr-FR') AS VARCHAR) AS context_json FROM base)
SELECT 1000000000+i,?,uid,ev,actor,props,strftime(ts,'%Y-%m-%dT%H:%M:%SZ'),epoch_ms(ts),ts,
 strftime(ts,'%Y-%m-%dT%H:%M:%SZ'),epoch_ms(ts),0,'architecture-fixture',uid,'track',context_json,1,'',
 CAST(json_object('id',1000000000+i,'project_id',?,'uuid',uid,'event',ev,'distinct_id',actor,'properties',json(props),
 'timestamp',strftime(ts,'%Y-%m-%dT%H:%M:%SZ'),'received_at',strftime(ts,'%Y-%m-%dT%H:%M:%SZ'),'source_id','architecture-fixture',
 'message_id',uid,'event_type','track','context',json(context_json),'Revision',0,'IsDeleted',false,
 'OriginalSnapshot',CAST(json_object('event',ev,'distinct_id',actor,'properties',json(props),'timestamp',strftime(ts,'%Y-%m-%dT%H:%M:%SZ')) AS VARCHAR)) AS VARCHAR)
FROM fixture`, offset, end, project, project)
			if err == nil {
				_, err = tx.ExecContext(ctx, `INSERT INTO lake.heads SELECT uuid,id,revision,false,received_at FROM lake.current_events WHERE id>=? AND id<?`, 1000000000+offset, 1000000000+end)
			}
			if err == nil {
				err = tx.Commit()
			} else {
				_ = tx.Rollback()
			}
		}
	} else {
		// Project is a generated hex identifier, still quote it explicitly.
		for offset := from; offset < to && err == nil; offset += 10000 {
			sql := fmt.Sprintf(`INSERT INTO %s.%s (event_id,project_id,uuid,event,distinct_id,properties,timestamp,timestamp_ms,received_at,received_at_ms,revision,is_deleted,original_snapshot,identity_snapshot,source_id,message_id,event_type,context,is_imported)
WITH number+%d AS i,addSeconds(toDateTime('2026-11-01 00:00:00','UTC'),i) AS ts,
 multiIf(i%%3=0,'page_viewed',i%%3=1,'signup_completed','purchase') AS ev,
 concat('fixture-',toString(i)) AS uid,concat('actor-',toString(intDiv(i,3)%%10000)) AS actor,
 concat('{"country":"country-',toString(i%%7),'","plan":"',if(i%%2=0,'free','pro'),'","amount":49.95,"duration_ms":',toString(i%%5000),'}') AS props,
 formatDateTime(ts,'%%Y-%%m-%%dT%%H:%%i:%%SZ','UTC') AS stamp
SELECT 1000000000+i,'%s',uid,ev,actor,props,stamp,toInt64(toUnixTimestamp(ts))*1000,stamp,toInt64(toUnixTimestamp(ts))*1000,0,0,
 concat('{"event":"',ev,'","distinct_id":"',actor,'","properties":',props,',"timestamp":"',stamp,'"}'),'',
 'architecture-fixture',uid,'track',concat('{"session_id":"session-',toString(intDiv(i,3)),'","locale":"fr-FR"}'),1 FROM numbers(%d)
SETTINGS max_threads=2,max_memory_usage=536870912`, s.ch.config.Database, eventTable, offset, project, min(10000, to-offset))
			_, err = s.ch.admin(ctx, sql)
		}
	}
	if err != nil {
		architectureEmit("seed", architectureEngine(s), map[string]any{"from": from, "to": to, "verdict": "NOT VERIFIED", "error": err.Error(), "elapsed_seconds": time.Since(start).Seconds()})
		t.Error("historical fixture setup did not complete")
		return false
	}
	architectureEmit("seed", architectureEngine(s), map[string]any{"from": from, "to": to, "elapsed_seconds": time.Since(start).Seconds(), "verdict": "VERIFIED"})
	return true
}

func architectureEngine(s *Service) string {
	if s.lake != nil {
		return "ducklake"
	}
	return "clickhouse"
}

func architectureFixtureChecks(t *testing.T, s *Service, project string, rows int) {
	t.Helper()
	h := s.Handler()
	engine := architectureEngine(s)
	if n := architectureCount(t, h, project); n != rows {
		t.Fatalf("fixture count %d != %d", n, rows)
	}
	for _, q := range architectureQueries {
		var samples []time.Duration
		codes := map[int]int{}
		var result AnalyticsResult
		for range 5 {
			code, r, elapsed := architectureRunQuery(h, adminToken, project, q.sql(engine))
			codes[code]++
			samples = append(samples, elapsed)
			if code == 200 {
				result = r
			}
		}
		verdict := "VERIFIED"
		if codes[200] != 5 {
			verdict = "NOT VERIFIED"
		}
		if codes[200] > 0 && q.name == "counts" {
			if string(result.Data[0]["total"]) != fmt.Sprintf("\"%d\"", rows) {
				t.Fatal("query counts do not match fixture")
			}
			if string(result.Data[0]["actors"]) != fmt.Sprintf("\"%d\"", min((rows+2)/3, 10000)) {
				t.Fatal("actor cardinality does not match fixture")
			}
		}
		if codes[200] > 0 && q.name == "dimensions" {
			expected := map[int]int{}
			for i := 0; i < rows; i++ {
				expected[(i/86400)*21+(i%3)*7+i%7]++
			}
			for _, r := range result.Data {
				var day, event, country string
				_ = json.Unmarshal(r["day"], &day)
				_ = json.Unmarshal(r["event"], &event)
				_ = json.Unmarshal(r["country"], &country)
				date, err := time.Parse(time.DateOnly, day)
				if err != nil {
					t.Fatal(err)
				}
				category, known := map[string]int{"page_viewed": 0, "signup_completed": 1, "purchase": 2}[event]
				if !known {
					t.Fatal("unexpected event in dimension result")
				}
				var dimension int
				if _, err := fmt.Sscanf(country, "country-%d", &dimension); err != nil {
					t.Fatal(err)
				}
				if dimension < 0 || dimension > 6 || country != fmt.Sprintf("country-%d", dimension) {
					t.Fatal("unexpected country in dimension result")
				}
				k := int(date.Sub(time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC))/(24*time.Hour))*21 + category*7 + dimension
				if string(r["total"]) != fmt.Sprintf("\"%d\"", expected[k]) {
					t.Fatal("custom dimension count does not match fixture")
				}
				delete(expected, k)
			}
			if len(expected) != 0 {
				t.Fatal("custom dimension rows missing")
			}
		}
		if codes[200] > 0 {
			expected := -1
			switch q.name {
			case "sessions":
				expected = (rows + 2) / 3
			case "ordered_funnel":
				expected = min(rows/3, 10000)
			case "next_day_retention":
				returned := map[int]bool{}
				for i := 86400; i < min(rows, 172800); i++ {
					if i%3 == 0 {
						returned[(i/3)%10000] = true
					}
				}
				expected = len(returned)
			}
			if expected >= 0 && (len(result.Data) != 1 || string(result.Data[0]["total"]) != fmt.Sprintf("\"%d\"", expected)) {
				t.Fatalf("%s fixture mismatch: %v", q.name, result.Data)
			}
		}

		architectureEmit("history", engine, map[string]any{"history_rows": rows, "query": q.name, "status_counts": codes, "latency": architectureLatency(samples), "result_rows": result.Rows, "verdict": verdict})
	}
}

func architectureLivePayload(offset, count, padding int) []byte {
	rows := make([]map[string]any, count)
	for i := range rows {
		n := offset + i
		rows[i] = map[string]any{"type": "track", "message_id": fmt.Sprintf("live-%09d-%d", n, padding), "actor_id": fmt.Sprintf("actor-%d", n%10000), "name": "live_event", "timestamp": "2026-11-13T00:00:00Z", "properties": map[string]any{"country": "FR", "plan": "pro", "amount": 49.95, "duration_ms": n % 5000, "padding": strings.Repeat("x", padding)}, "context": map[string]any{"session_id": fmt.Sprintf("session-%d", n%500)}}
	}
	raw, _ := json.Marshal(rows)
	return raw
}

func architectureDrain(t *testing.T, s *Service, limit time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(limit)
	for {
		count, _ := queueUsage(t, s)
		var mutations int64
		if err := s.db.QueryRow("SELECT count(*) FROM mutation_outbox").Scan(&mutations); err != nil {
			t.Fatal(err)
		}
		count += mutations
		if count == 0 || time.Now().After(deadline) {
			return int(count)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func architectureMixed(t *testing.T, s *Service, project, token string, rate, readers, offset int, maintenance bool) {
	t.Helper()
	h := s.Handler()
	engine := architectureEngine(s)
	before := architectureCount(t, h, project)
	var mu sync.Mutex
	var querySamples []time.Duration
	queryCodes := map[int]int{}
	var workers sync.WaitGroup
	maintenanceSuccess := true
	ctx, cancel := context.WithCancel(context.Background())
	defer func() { cancel(); workers.Wait() }()
	for worker := 0; worker < readers; worker++ {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			for ctx.Err() == nil {
				q := architectureQueries[(worker+2)%len(architectureQueries)]
				code, _, elapsed := architectureRunQuery(h, adminToken, project, q.sql(engine))
				mu.Lock()
				querySamples = append(querySamples, elapsed)
				queryCodes[code]++
				mu.Unlock()
				select {
				case <-ctx.Done():
					return
				case <-time.After(100 * time.Millisecond):
				}
			}
		}(worker)
	}
	if maintenance && s.lake != nil {
		workers.Add(1)
		go func() {
			defer workers.Done()
			maxCompactions := 8
			policy := os.Getenv("PORCUPINE_MAINT_POLICY")
			if os.Getenv("PORCUPINE_ARCH_PHASE") == "maintenance" {
				select {
				case <-ctx.Done():
					return
				case <-time.After(2 * time.Second):
				}
				if policy == "bounded" {
					maxCompactions = 1
				}
			}
			mctx, stop := context.WithTimeout(ctx, 15*time.Second)
			defer stop()
			steps, err := s.lake.(*duckLakeStore).maintainMeasured(mctx, maxCompactions)
			maintenanceSuccess = err == nil
			progress := int64(0)
			for _, step := range steps {
				progress += step.FilesProcessed
			}
			if os.Getenv("PORCUPINE_ARCH_PHASE") == "maintenance" && progress == 0 {
				maintenanceSuccess = false
			}
			architectureEmit("maintenance", engine, map[string]any{"success": err == nil, "policy": policy, "steps": steps, "progress": progress})
		}()
	}
	duration := 10 * time.Second
	if os.Getenv("PORCUPINE_ARCH_PHASE") == "maintenance" {
		duration = 20 * time.Second
	}
	batch := 100
	requests := rate * int(duration/time.Second) / batch
	payloads := make([][]byte, requests)
	for i := range payloads {
		payloads[i] = architectureLivePayload(offset+i*batch, batch, 0)
	}
	start := time.Now()
	accepted, rejected, duplicate := 0, 0, 0
	var acceptSamples []time.Duration
	maxPending := 0
	for i, body := range payloads {
		wait := time.Until(start.Add(time.Duration(i) * duration / time.Duration(requests)))
		if wait > 0 {
			time.Sleep(wait)
		}
		code, raw, elapsed := architectureHTTP(h, "POST", "/ingest/v1/events", token, body, true)
		acceptSamples = append(acceptSamples, elapsed)
		if code == 200 {
			var response struct{ Accepted, Duplicates int }
			if json.Unmarshal(raw, &response) != nil {
				t.Fatal("invalid acceptance response")
			}
			accepted += response.Accepted
			duplicate += response.Duplicates
		} else {
			rejected += batch
		}
		count, _ := queueUsage(t, s)
		maxPending = max(maxPending, int(count))
	}
	arrivalElapsed := time.Since(start)
	pendingAtStop, _ := queueUsage(t, s)
	cancel()
	workers.Wait()
	drainStart := time.Now()
	remaining := architectureDrain(t, s, 30*time.Second)
	visible := architectureCount(t, h, project) - before
	if (remaining == 0 && visible != accepted) || visible > accepted {
		t.Fatalf("accepted=%d but visible=%d pending=%d", accepted, visible, remaining)
	}
	verdict := "VERIFIED"
	if accepted != requests*batch || rejected > 0 || duplicate > 0 || remaining > 0 || pendingAtStop > int64(2*rate) || maxPending > 2*rate || arrivalElapsed > duration+time.Second || (readers > 0 && queryCodes[200] != len(querySamples)) || !maintenanceSuccess {
		verdict = "NOT VERIFIED"
	}
	architectureEmit("mixed", engine, map[string]any{"offered_rate": rate, "offered_events": requests * batch, "readers": readers, "maintenance": maintenance, "accepted": accepted, "rejected": rejected, "duplicates": duplicate, "arrival_elapsed_seconds": arrivalElapsed.Seconds(), "accept_latency": architectureLatency(acceptSamples), "query_latency": architectureLatency(querySamples), "query_status_counts": queryCodes, "max_pending": maxPending, "pending_at_stop": pendingAtStop, "remaining_after_drain": remaining, "visible": visible, "drain_seconds": time.Since(drainStart).Seconds(), "verdict": verdict})
}

func TestAnalyticsArchitectureExperiment(t *testing.T) {
	phase := os.Getenv("PORCUPINE_ARCH_PHASE")
	if phase == "" {
		t.Skip("use make ducklake-architecture-test")
	}
	engine := os.Getenv("PORCUPINE_ARCH_ENGINE")
	if engine != "ducklake" && engine != "clickhouse" {
		t.Fatal("invalid experiment engine")
	}
	s, project, token := comparisonServiceMemory(t, engine, 512)
	s.queueMax, s.queueMaxBytes = 100000, 256<<20
	s.deliveryMaxEvents, s.deliveryMaxBytes = 1000, 4<<20
	h := s.Handler()
	if s.ch != nil {
		_, _, _, err := s.analyticsReaderForCache(context.Background(), project, false)
		if err != nil {
			if failure, ok := err.(*clickHouseError); ok {
				t.Fatalf("ClickHouse reader provisioning failed: code=%d HTTP=%d", failure.Code, failure.Status)
			}
			t.Fatalf("ClickHouse reader provisioning failed: %v", err)
		}
	}
	comparisonQuery(t, h, project, "SELECT count(*) AS total FROM events")
	defer func() {
		roots := []string{filepath.Dir(s.projectionGuard.path)}
		if s.lake != nil {
			roots = append(roots, s.lake.(*duckLakeStore).root)
		} else {
			roots = append(roots, "/benchmark-data/clickhouse")
		}
		architectureEmit("resources", engine, architectureResources(roots...))
	}()
	switch phase {
	case "history":
		from := 0
		for _, rows := range []int{10000, 100000, 1000000} {
			if !architectureSeed(t, s, project, from, rows) {
				return
			}
			architectureFixtureChecks(t, s, project, rows)
			from = rows
		}
		architectureLifecycle(t, s, project, token)
	case "mixed":
		if !architectureSeed(t, s, project, 0, 1000000) {
			return
		}
		s.startDelivery()
		architectureMixed(t, s, project, token, 1000, 0, 0, false)
		architectureMixed(t, s, project, token, 1000, 2, 100000, false)
		architectureMixed(t, s, project, token, 3000, 2, 200000, false)
		if s.lake != nil {
			architectureMixed(t, s, project, token, 1000, 2, 300000, true)
		}
	case "projects":
		architectureProjects(t, s)
	case "crash":
		architectureCrash(t)
	case "maintenance":
		if s.lake == nil {
			t.Fatal("native maintenance experiment requires DuckLake")
		}
		policy := os.Getenv("PORCUPINE_MAINT_POLICY")
		if policy != "baseline" && policy != "bounded" {
			t.Fatal("choose baseline or bounded maintenance policy")
		}
		if !architectureSeed(t, s, project, 0, 1000000) {
			return
		}
		store := s.lake.(*duckLakeStore)
		p, release, err := store.leaseProject(context.Background(), project)
		if err != nil {
			t.Fatal(err)
		}
		if policy == "bounded" {
			_, err = p.db.Exec("CALL ducklake_set_option('lake','target_file_size','8MB')")
		}
		var nativeVersion string
		if err == nil {
			err = p.db.QueryRow("SELECT version()").Scan(&nativeVersion)
		}
		release()
		if err != nil {
			t.Fatal(err)
		}
		architectureEmit("maintenance_environment", engine, map[string]any{"policy": policy, "native_version": nativeVersion})
		s.startDelivery()
		architectureMixed(t, s, project, token, 1000, 2, 0, true)
	case "maintenance-soak":
		if s.lake == nil {
			t.Fatal("native maintenance experiment requires DuckLake")
		}
		if !architectureSeed(t, s, project, 0, 1000000) {
			return
		}
		architectureMaintenanceSoak(t, s, project, token)
	case "payloads":
		s.startDelivery()
		for _, size := range []int{0, 2048, 16384, 65536} {
			before := architectureCount(t, h, project)
			start := time.Now()
			accepted := 0
			for i := 0; i < 1000; i += 4 {
				body := architectureLivePayload(size*10000+i, 4, size)
				code, raw, _ := architectureHTTP(h, "POST", "/ingest/v1/events", token, body, true)
				if code != 200 {
					t.Fatalf("payload acceptance HTTP %d", code)
				}
				var response struct{ Accepted, Duplicates int }
				if json.Unmarshal(raw, &response) != nil || response.Accepted != 4 || response.Duplicates != 0 {
					t.Fatal("payload batch was not completely accepted")
				}
				accepted += response.Accepted
			}
			left := architectureDrain(t, s, 30*time.Second)
			if accepted != 1000 {
				t.Fatal("payload fixture did not accept 1000 events")
			}
			count := architectureCount(t, h, project) - before
			if (left == 0 && count != accepted) || count > accepted {
				t.Fatal("payload events lost")
			}
			verdict := "VERIFIED"
			if left != 0 {
				verdict = "NOT VERIFIED"
			}
			architectureEmit("payloads", engine, map[string]any{"padding_bytes": size, "accepted": accepted, "projected": count, "remaining": left, "elapsed_seconds": time.Since(start).Seconds(), "verdict": verdict})
		}
	case "guard":
		if !architectureSeed(t, s, project, 0, 1000000) {
			return
		}
		sql := "SELECT sum(length(a.uuid||b.uuid)) AS total FROM events a JOIN events b ON a.event=b.event"
		if engine == "clickhouse" {
			sql = "SELECT sum(length(concat(a.uuid,b.uuid))) AS total FROM events a JOIN events b ON a.event=b.event"
		}
		code, _, elapsed := architectureRunQuery(h, adminToken, project, sql)
		healthStart := time.Now()
		count := architectureCount(t, h, project)
		verdict := "VERIFIED"
		if code != 429 || elapsed > 10*time.Second || count != 1000000 {
			verdict = "NOT VERIFIED"
		}
		architectureEmit("guard", engine, map[string]any{"query_status": code, "query_seconds": elapsed.Seconds(), "recovery_query_seconds": time.Since(healthStart).Seconds(), "verdict": verdict})
	default:
		t.Fatal("unknown architecture phase")
	}
}

func architectureLifecycle(t *testing.T, s *Service, projectID, ingest string) {
	t.Helper()
	h := s.Handler()
	engine := architectureEngine(s)
	before := architectureCount(t, h, projectID)
	_, manager := key(t, s, []string{"events:read", "events:edit", "events:delete"}, []string{projectID})
	page := []map[string]any{{"uuid": "scale-page", "event": "page_viewed", "distinct_id": "scale-anon", "timestamp": "2026-11-13T00:00:00Z", "properties": map[string]any{"country": "FR"}}}
	start := time.Now()
	identityCapture(t, s, ingest, page, 200)
	nativeRequest(t, s, ingest, `{"type":"identify","message_id":"scale-identity","anonymous_id":"scale-anon","user_id":"scale-user","set":{"plan":"pro"}}`, 200)
	s.startDelivery()
	if architectureDrain(t, s, 30*time.Second) != 0 {
		t.Fatal("identity did not project")
	}
	code, result, _ := architectureRunQuery(h, manager, projectID, "SELECT person_id,person_properties FROM events WHERE uuid='scale-page'")
	if code != 200 || len(result.Data) != 1 || string(result.Data[0]["person_id"]) != `"scale-user"` {
		t.Fatalf("large-history identity failed: HTTP %d", code)
	}
	var traitsJSON string
	var traits map[string]any
	if json.Unmarshal(result.Data[0]["person_properties"], &traitsJSON) != nil || json.Unmarshal([]byte(traitsJSON), &traits) != nil || traits["plan"] != "pro" {
		t.Fatal("identified properties were not projected")
	}
	identityTime := time.Since(start)
	start = time.Now()
	body := []byte(`{"expected_revision":"0","idempotency_key":"scale-edit","snapshot":{"event":"purchase","distinct_id":"scale-anon","timestamp":"2026-11-13T00:00:00Z","properties":{"amount":42}}}`)
	code, _, _ = architectureHTTP(h, "PATCH", "/api/v1/projects/"+projectID+"/events/scale-page", manager, body, false)
	if code != 202 || architectureDrain(t, s, 30*time.Second) != 0 {
		t.Fatalf("large-history edit HTTP %d", code)
	}
	code, result, _ = architectureRunQuery(h, manager, projectID, "SELECT event,revision FROM events WHERE uuid='scale-page'")
	if code != 200 || len(result.Data) != 1 || string(result.Data[0]["event"]) != `"purchase"` || string(result.Data[0]["revision"]) != `"1"` {
		t.Fatal("large-history edit did not become visible")
	}
	editTime := time.Since(start)
	start = time.Now()
	body = []byte(`{"expected_revision":"1","idempotency_key":"scale-delete"}`)
	code, _, _ = architectureHTTP(h, "DELETE", "/api/v1/projects/"+projectID+"/events/scale-page", manager, body, false)
	if code != 202 || architectureDrain(t, s, 30*time.Second) != 0 {
		t.Fatalf("large-history delete HTTP %d", code)
	}
	retry := identityCapture(t, s, ingest, page, 200)
	if retry["duplicates"] != float64(1) {
		t.Fatal("late original was not deduplicated")
	}
	if architectureDrain(t, s, 30*time.Second) != 0 {
		t.Fatal("late retry did not settle")
	}
	if architectureCount(t, h, projectID) != before {
		t.Fatal("deletion or late retry changed history count")
	}
	architectureEmit("lifecycle", engine, map[string]any{"history_rows": before, "identify_seconds": identityTime.Seconds(), "edit_seconds": editTime.Seconds(), "delete_and_retry_seconds": time.Since(start).Seconds(), "verdict": "VERIFIED"})
}

func architectureProjects(t *testing.T, s *Service) {
	engine := architectureEngine(s)
	h := s.Handler()
	type access struct{ project, ingest, reader string }
	var projects []access
	for i := 0; i < 16; i++ {
		p, ingest := project(t, s)
		_, reader := key(t, s, []string{"events:read"}, []string{p})
		projects = append(projects, access{p, ingest, reader})
		comparisonAccept(t, h, ingest, architectureLivePayload(0, 100, 0))
		if err := s.flushDelivery(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	for _, workingSet := range []int{4, 16} {
		var samples []time.Duration
		start := time.Now()
		for i := 0; i < workingSet*2; i++ {
			p := projects[i%workingSet]
			code, result, elapsed := architectureRunQuery(h, p.reader, p.project, "SELECT project_id,count(*) AS total FROM events GROUP BY project_id")
			if code != 200 || len(result.Data) != 1 || string(result.Data[0]["project_id"]) != fmt.Sprintf("%q", p.project) || string(result.Data[0]["total"]) != `"100"` {
				t.Fatalf("project scope or counts incorrect: HTTP %d", code)
			}
			samples = append(samples, elapsed)
		}
		resident := 0
		if s.lake != nil {
			store := s.lake.(*duckLakeStore)
			_ = store.mu.Lock(context.Background())
			resident = len(store.projects)
			store.mu.Unlock()
			if resident > 4 {
				t.Fatal("resident catalog limit exceeded")
			}
		}
		architectureEmit("projects", engine, map[string]any{"working_set": workingSet, "resident_catalogs": resident, "latency": architectureLatency(samples), "elapsed_seconds": time.Since(start).Seconds(), "verdict": "VERIFIED"})
	}
	code, _, _ := architectureRunQuery(h, projects[0].reader, projects[1].project, "SELECT count(*) FROM events")
	if code != 403 {
		t.Fatal("project-scoped key crossed boundary")
	}
}
