//go:build ducklake

package porcupine

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func lakeMaintenanceFileCount(ctx context.Context, p *projectLake, table string) (int, error) {
	var count int
	err := p.db.QueryRowContext(ctx, "SELECT count(*) FROM ducklake_list_files('lake',?)", table).Scan(&count)
	return count, err
}

func maintenanceOpenService(t *testing.T) (*Service, string, string) {
	t.Helper()
	directory := t.TempDir()
	s, err := Open(Config{DBPath: filepath.Join(directory, "queue.db"), BootstrapToken: adminToken, QueueMaxEvents: 100000, DuckLake: &DuckLakeConfig{Directory: filepath.Join(directory, "lake"), MemoryLimitMB: 512, MaintenanceInterval: 24 * time.Hour}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	// Measure the same native policy explicitly below. Keep Open's actual
	// delivery worker and row/byte defaults; only its timer is replaced.
	s.lakeMaintenanceCancel()
	<-s.lakeMaintenanceDone
	p, ingest := project(t, s)
	return s, p, ingest
}

func lakeMaintenancePages(prefix string) string {
	return fmt.Sprintf(`[{"type":"track","message_id":%q,"actor_id":%q,"name":"lifecycle-page"},{"type":"track","message_id":%q,"actor_id":%q,"name":"lifecycle-remove"}]`, prefix+"-edit", prefix+"-anon", prefix+"-delete", prefix+"-anon")
}

func lakeMaintenanceQuery(t *testing.T, s *Service, project, sql string, parameters map[string]string) []any {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"sql": sql, "parameters": parameters, "use_cache": false})
	deadline := time.Now().Add(10 * time.Second)
	for {
		code, raw, _ := architectureHTTP(s.Handler(), "POST", "/api/v1/projects/"+project+"/query", adminToken, body, false)
		if code == 429 && time.Now().Before(deadline) {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		var result map[string]any
		if code != 200 || json.Unmarshal(raw, &result) != nil {
			t.Fatalf("lifecycle query failed: HTTP %d %s", code, raw)
		}
		return result["result"].(map[string]any)["data"].([]any)
	}
}

type maintenanceLifecycleState struct {
	prefix, revision string
	receipts         [2]map[string]any
}

func lakeMaintenanceLifecycle(t *testing.T, s *Service, project, ingest, prefix string, verifyNow bool) maintenanceLifecycleState {
	t.Helper()
	h := s.Handler()
	page := lakeMaintenancePages(prefix)
	first := nativeRequest(t, s, ingest, page, 200)
	editReceipt, deleteReceipt := receiptFrom(t, first, 0), receiptFrom(t, first, 1)
	identityReceipt := receiptFrom(t, nativeRequest(t, s, ingest, fmt.Sprintf(`{"type":"identify","message_id":%q,"anonymous_id":%q,"user_id":%q,"set":{"plan":"team"}}`, prefix+"-identify", prefix+"-anon", prefix+"-person"), 200), 0)
	edit := fmt.Sprintf(`{"expected_revision":"0","idempotency_key":%q,"snapshot":{"event":"lifecycle-corrected","distinct_id":%q,"timestamp":"2026-11-13T00:00:00Z","properties":{"amount":42}}}`, prefix+"-edit", prefix+"-anon")
	code, editRaw, _ := architectureHTTP(h, "PATCH", "/api/v1/projects/"+project+"/events/"+editReceipt["uuid"].(string), adminToken, []byte(edit), false)
	if code != 202 {
		t.Fatalf("concurrent edit failed: HTTP %d", code)
	}
	var editResponse struct {
		Revision string `json:"revision"`
	}
	if json.Unmarshal(editRaw, &editResponse) != nil || editResponse.Revision == "" {
		t.Fatal("invalid edit response")
	}
	code, _, _ = architectureHTTP(h, "DELETE", "/api/v1/projects/"+project+"/events/"+deleteReceipt["uuid"].(string), adminToken, []byte(fmt.Sprintf(`{"expected_revision":"0","idempotency_key":%q}`, prefix+"-delete")), false)
	if code != 202 {
		t.Fatalf("concurrent delete failed: HTTP %d", code)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	expectedRevision, err := strconv.ParseUint(editResponse.Revision, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	for {
		edited, editErr := s.lake.Load(ctx, project, editReceipt["uuid"].(string))
		deleted, deleteErr := s.lake.Load(ctx, project, deleteReceipt["uuid"].(string))
		identified, identityErr := s.lake.Load(ctx, project, identityReceipt["uuid"].(string))
		if editErr == nil && deleteErr == nil && identityErr == nil && identified.Event.Event == "$identify" && edited.Revision == expectedRevision && deleted.IsDeleted {
			break
		}
		select {
		case <-ctx.Done():
			status, _ := s.deliveryStatus(context.Background(), project)
			t.Fatalf("targets not visible: edit=%v revision=%d/%d delete=%v deleted=%v identity=%v snapshot_bytes=%d delivery=%+v", editErr, edited.Revision, expectedRevision, deleteErr, deleted.IsDeleted, identityErr, len(identified.IdentitySnapshot), status)
		case <-time.After(50 * time.Millisecond):
		}
	}
	state := maintenanceLifecycleState{prefix: prefix, revision: editResponse.Revision, receipts: [2]map[string]any{editReceipt, deleteReceipt}}
	retry := nativeRequest(t, s, ingest, page, 200)
	if retry["duplicates"] != float64(2) || receiptFrom(t, retry, 0)["event_id"] != editReceipt["event_id"] || receiptFrom(t, retry, 1)["event_id"] != deleteReceipt["event_id"] {
		t.Fatal("concurrent retry changed receipt IDs")
	}
	if verifyNow {
		lakeMaintenanceVerify(t, s, project, state)
	}
	return state
}

func lakeMaintenanceVerify(t *testing.T, s *Service, project string, state maintenanceLifecycleState) {
	t.Helper()
	prefix, editReceipt, deleteReceipt := state.prefix, state.receipts[0], state.receipts[1]
	rows := lakeMaintenanceQuery(t, s, project, "SELECT event_id,event,revision,person_id,person_properties,properties FROM events WHERE uuid=$uuid", map[string]string{"uuid": editReceipt["uuid"].(string)})
	if len(rows) != 1 {
		t.Fatal("concurrent correction missing")
	}
	row := rows[0].(map[string]any)
	var traits, properties map[string]any
	if json.Unmarshal([]byte(row["person_properties"].(string)), &traits) != nil || json.Unmarshal([]byte(row["properties"].(string)), &properties) != nil || properties["amount"] != float64(42) || row["event_id"] != editReceipt["event_id"] || row["event"] != "lifecycle-corrected" || row["revision"] != state.revision || row["person_id"] != prefix+"-person" || traits["plan"] != "team" {
		t.Fatalf("concurrent identity/edit changed: %v", row)
	}
	deleted := lakeMaintenanceQuery(t, s, project, "SELECT count(*) AS total FROM events WHERE uuid=$uuid", map[string]string{"uuid": deleteReceipt["uuid"].(string)})
	if deleted[0].(map[string]any)["total"] != "0" {
		t.Fatal("concurrent deletion still visible")
	}
}

// A long run is opt-in; the ordinary native race suite exercises the same
// lifecycle helper concurrently with real file maintenance below.
func architectureMaintenanceSoak(t *testing.T, s *Service, projectID, ingest string) {
	t.Helper()
	policy := os.Getenv("PORCUPINE_MAINT_POLICY")
	if policy != "locked" && policy != "concurrent" && policy != "locked-small" && policy != "concurrent-small" {
		t.Fatal("choose locked/concurrent, optionally with -small size filtering")
	}
	maxFileSize := int64(0)
	if strings.HasSuffix(policy, "-small") {
		maxFileSize = 1 << 20
	}
	seconds := 180
	rate := 1000
	if value := os.Getenv("PORCUPINE_SOAK_RATE"); value != "" {
		var err error
		rate, err = strconv.Atoi(value)
		if err != nil || rate < 100 || rate > 3000 || rate%100 != 0 {
			t.Fatal("invalid soak event rate")
		}
	}
	if value := os.Getenv("PORCUPINE_SOAK_SECONDS"); value != "" {
		var err error
		seconds, err = strconv.Atoi(value)
		if err != nil || seconds < 10 || seconds > 900 {
			t.Fatal("invalid soak duration")
		}
	}
	store := s.lake.(*duckLakeStore)
	p, release, err := store.leaseProject(context.Background(), projectID)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if _, err := p.db.Exec("CALL ducklake_set_option('lake','target_file_size','8MB')"); err != nil {
		t.Fatal(err)
	}
	initialFiles, err := lakeMaintenanceFileCount(context.Background(), p, "current_events")
	if err != nil {
		t.Fatal(err)
	}
	var version string
	if err := p.db.QueryRow("SELECT version()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	architectureEmit("soak_environment", "ducklake", map[string]any{"policy": policy, "seconds": seconds, "offered_rate": rate, "peak_queue_budget": 2 * rate, "native_version": version, "engine_memory_mib": store.config.MemoryLimitMB, "engine_threads": store.config.Threads, "delivery_max_events": s.deliveryMaxEvents, "delivery_max_bytes": s.deliveryMaxBytes, "maintenance_gap_seconds": 2, "target_file_size": "8MB", "max_candidate_groups_per_table": 1, "max_input_file_bytes": maxFileSize, "open_delivery_defaults": os.Getenv("PORCUPINE_SOAK_OPEN_DEFAULTS") == "1", "lifecycle_views_during_load": os.Getenv("PORCUPINE_SOAK_OPEN_DEFAULTS") != "1", "initial_active_event_files": initialFiles})
	other, otherIngest := project(t, s)
	nativeRequest(t, s, otherIngest, `{"type":"track","message_id":"foreign","actor_id":"foreign","name":"foreign"}`, 200)
	if s.deliveryCancel == nil {
		s.startDelivery()
	}
	if architectureDrain(t, s, 30*time.Second) != 0 {
		t.Fatal("initial foreign event did not settle")
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	defer func() { cancel(); wg.Wait() }()
	var mu sync.Mutex
	var queries, accepts []time.Duration
	codes := map[int]int{}
	queryFailures := map[string]int{}
	passes, completed, failed, progress, lifecycle := 0, 0, 0, int64(0), 0
	var lifecycleResults []maintenanceLifecycleState
	verifyDuringLoad := os.Getenv("PORCUPINE_SOAK_OPEN_DEFAULTS") != "1"
	var maxHold float64
	for reader := range 2 {
		wg.Add(1)
		go func(reader int) {
			defer wg.Done()
			for ctx.Err() == nil {
				body, _ := json.Marshal(map[string]any{"sql": architectureQueries[reader+2].duckdb, "use_cache": false})
				code, raw, elapsed := architectureHTTP(s.Handler(), "POST", "/api/v1/projects/"+projectID+"/query", adminToken, body, false)
				failure := ""
				if code == 200 {
					var response AnalyticsQuery
					if json.Unmarshal(raw, &response) != nil || len(response.Result.Data) != 1 || string(response.Result.Data[0]["total"]) != []string{`"333334"`, `"10000"`}[reader] {
						code = 599
						failure = "unexpected analytical result"
					}
				} else {
					failure = fmt.Sprintf("HTTP%d %s", code, raw)
				}
				mu.Lock()
				queries, codes[code] = append(queries, elapsed), codes[code]+1
				if failure != "" {
					queryFailures[failure]++
				}
				mu.Unlock()
				select {
				case <-ctx.Done():
					return
				case <-time.After(100 * time.Millisecond):
				}
			}
		}(reader)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for ctx.Err() == nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
			mctx, stop := context.WithTimeout(ctx, 5*time.Second)
			steps, err := store.maintainSized(mctx, 1, strings.HasPrefix(policy, "locked"), maxFileSize)
			stop()
			if ctx.Err() != nil {
				return
			}
			mu.Lock()
			passes++
			if err == nil {
				completed++
			} else {
				failed++
			}
			for _, step := range steps {
				if step.Completed {
					progress += step.FilesProcessed
				}
				maxHold = max(maxHold, step.LockHeldMS)
			}
			mu.Unlock()
			failure := ""
			if err != nil {
				failure = err.Error()
			}
			architectureEmit("soak_maintenance", "ducklake", map[string]any{"policy": policy, "steps": steps, "success": err == nil, "error": failure})
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for cycle := 0; cycle < 6; cycle++ {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Duration(max(1, seconds/8)) * time.Second):
			}
			result := lakeMaintenanceLifecycle(t, s, projectID, ingest, fmt.Sprintf("soak-cycle-%d", cycle), verifyDuringLoad)
			mu.Lock()
			lifecycle++
			lifecycleResults = append(lifecycleResults, result)
			mu.Unlock()
		}
	}()
	expected := make(map[string]string, seconds*rate)
	maxPending, maxOldestMS := int64(0), int64(0)
	maxFiles := initialFiles
	start := time.Now()
	lastSample := start
	requests := seconds * rate / 100
	for batch := 0; batch < requests; batch++ {
		if delay := time.Until(start.Add(time.Duration(batch) * 100 * time.Second / time.Duration(rate))); delay > 0 {
			time.Sleep(delay)
		}
		code, raw, elapsed := architectureHTTP(s.Handler(), "POST", "/ingest/v1/events", ingest, architectureLivePayload(batch*100, 100, 0), true)
		accepts = append(accepts, elapsed)
		var response struct {
			Accepted, Duplicates int
			Receipts             []struct {
				EventID string `json:"event_id"`
			}
		}
		if code != 200 || json.Unmarshal(raw, &response) != nil || response.Accepted != 100 || response.Duplicates != 0 || len(response.Receipts) != 100 {
			t.Fatalf("soak acceptance failed: HTTP %d %s", code, raw)
		}
		for i, r := range response.Receipts {
			expected[fmt.Sprintf("live-%09d-0", batch*100+i)] = r.EventID
		}
		status, err := s.deliveryStatus(context.Background(), projectID)
		if err != nil {
			t.Fatal(err)
		}
		maxPending = max(maxPending, status.Pending)
		if status.OldestPendingReceivedAt != "" {
			oldest, err := time.Parse(time.RFC3339Nano, status.OldestPendingReceivedAt)
			if err != nil {
				t.Fatal(err)
			}
			maxOldestMS = max(maxOldestMS, time.Since(oldest).Milliseconds())
		}
		if time.Since(lastSample) >= 10*time.Second {
			files, err := lakeMaintenanceFileCount(context.Background(), p, "current_events")
			if err != nil {
				t.Fatal(err)
			}
			maxFiles = max(maxFiles, files)
			heads, err := lakeMaintenanceFileCount(context.Background(), p, "heads")
			if err != nil {
				t.Fatal(err)
			}
			architectureEmit("soak_window", "ducklake", map[string]any{"policy": policy, "elapsed_seconds": time.Since(start).Seconds(), "accepted": len(expected), "pending": status.Pending, "oldest_pending_received_at": status.OldestPendingReceivedAt, "active_event_files": files, "active_head_files": heads})
			lastSample = time.Now()
		}
	}
	arrival := time.Since(start)
	cancel()
	wg.Wait()
	if !verifyDuringLoad {
		for _, state := range lifecycleResults {
			lakeMaintenanceVerify(t, s, projectID, state)
		}
	}
	remaining := architectureDrain(t, s, 60*time.Second)
	if remaining != 0 {
		t.Fatalf("soak did not drain: %d", remaining)
	}
	for _, offset := range []int{0, (requests - 1) * 100} {
		retry := nativeRequest(t, s, ingest, string(architectureLivePayload(offset, 100, 0)), 200)
		if retry["duplicates"] != float64(100) || retry["accepted"] != float64(0) {
			t.Fatal("soak retry changed acceptance")
		}
		for i := 0; i < 100; i++ {
			if receiptFrom(t, retry, i)["event_id"] != expected[fmt.Sprintf("live-%09d-0", offset+i)] {
				t.Fatal("soak retry changed stable event ID")
			}
		}
	}
	rows, err := p.db.Query("SELECT message_id,CAST(id AS VARCHAR) FROM events WHERE event='live_event'")
	if err != nil {
		t.Fatal(err)
	}
	visible := 0
	for rows.Next() {
		var message, id string
		if err := rows.Scan(&message, &id); err != nil {
			t.Fatal(err)
		}
		if expected[message] != id {
			t.Fatalf("unexpected or changed event %s %s", message, id)
		}
		delete(expected, message)
		visible++
	}
	err = rows.Err()
	rows.Close()
	if err != nil || len(expected) != 0 {
		t.Fatalf("missing live events: %d %v", len(expected), err)
	}
	if architectureCount(t, s.Handler(), other) != 1 {
		t.Fatal("concurrency crossed project boundary")
	}
	for i, expected := range []string{`"333334"`, `"10000"`} {
		code, result, _ := architectureRunQuery(s.Handler(), adminToken, projectID, architectureQueries[i+2].duckdb)
		if code != 200 || len(result.Data) != 1 || string(result.Data[0]["total"]) != expected {
			t.Fatalf("final known analytics result changed: query=%s HTTP%d result=%v", architectureQueries[i+2].name, code, result.Data)
		}
	}
	finalFiles, err := lakeMaintenanceFileCount(context.Background(), p, "current_events")
	if err != nil {
		t.Fatal(err)
	}
	finalHeads, err := lakeMaintenanceFileCount(context.Background(), p, "heads")
	if err != nil {
		t.Fatal(err)
	}
	verdict := "VERIFIED"
	if completed < 3 || progress == 0 || lifecycle != 6 || codes[200] != len(queries) || arrival > time.Duration(seconds)*time.Second+time.Second || maxPending > int64(2*rate) || maxOldestMS > 3000 || finalFiles > initialFiles+128 {
		verdict = "NOT VERIFIED"
	}
	architectureEmit("maintenance_soak", "ducklake", map[string]any{"policy": policy, "seconds": seconds, "offered_rate": rate, "peak_queue_budget": 2 * rate, "open_delivery_defaults": os.Getenv("PORCUPINE_SOAK_OPEN_DEFAULTS") == "1", "lifecycle_views_during_load": os.Getenv("PORCUPINE_SOAK_OPEN_DEFAULTS") != "1", "arrival_seconds": arrival.Seconds(), "accepted": visible, "visible_exact_ids": visible, "remaining": remaining, "passes": passes, "completed_passes": completed, "failed_passes": failed, "files_processed": progress, "max_write_lock_ms": maxHold, "initial_active_event_files": initialFiles, "max_active_event_files": maxFiles, "final_active_event_files": finalFiles, "final_active_head_files": finalHeads, "max_pending": maxPending, "max_oldest_pending_ms": maxOldestMS, "query_status_counts": codes, "query_failures": queryFailures, "query_latency": architectureLatency(queries), "accept_latency": architectureLatency(accepts), "lifecycle_cycles": lifecycle, "verdict": verdict})
}

func TestDuckLakeMaintenanceConcurrentLifecycle(t *testing.T) {
	s, a, ingest := comparisonServiceMemory(t, "ducklake", 256)
	s.deliveryMaxEvents = 1000
	s.queueMax = 10000
	store := s.lake.(*duckLakeStore)
	p, release, err := store.leaseProject(context.Background(), a)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.db.Exec("CALL ducklake_set_option('lake','target_file_size','1MB')"); err != nil {
		t.Fatal(err)
	}
	release()
	if !architectureSeed(t, s, a, 0, 10000) {
		return
	}
	p, release, err = store.leaseProject(context.Background(), a)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lakeMaintenanceFileCount(context.Background(), p, "current_events"); err != nil {
		t.Fatal(err)
	}
	release()
	s.startDelivery()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var progress int64
	go func() {
		defer close(done)
		for ctx.Err() == nil {
			steps, err := store.maintainSized(ctx, 1, false, 1<<20)
			if err == nil {
				for _, step := range steps {
					progress += step.FilesProcessed
				}
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Millisecond):
			}
		}
	}()
	defer func() { cancel(); <-done }()
	var first [2]map[string]any
	for cycle := 0; cycle < 8; cycle++ {
		receipts := lakeMaintenanceLifecycle(t, s, a, ingest, fmt.Sprintf("concurrent-%d", cycle), true)
		if cycle == 0 {
			first = receipts.receipts
		}
	}
	cancel()
	<-done
	if progress == 0 {
		t.Fatal("concurrent maintenance made no compaction progress")
	}
	if count := architectureCount(t, s.Handler(), a); count != 10008 {
		t.Fatalf("unexpected lifecycle count %d", count)
	}
	s.deliveryCancel()
	<-s.deliveryDone
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	lake, err := newDuckLake(store.config)
	if err != nil {
		t.Fatal(err)
	}
	s.lake = lake
	s.startDelivery()
	retry := nativeRequest(t, s, ingest, lakeMaintenancePages("concurrent-0"), 200)
	if retry["duplicates"] != float64(2) || receiptFrom(t, retry, 0)["event_id"] != first[0]["event_id"] || receiptFrom(t, retry, 1)["event_id"] != first[1]["event_id"] {
		t.Fatal("restart changed stable retry IDs")
	}
	if architectureCount(t, s.Handler(), a) != 10008 {
		t.Fatal("restart/retry changed visible count")
	}
	rows := duckLakeJourneyQuery(t, s, a, adminToken, "SELECT event,person_id FROM events WHERE uuid=$uuid", map[string]string{"uuid": first[0]["uuid"].(string)})
	if len(rows) != 1 || rows[0].(map[string]any)["event"] != "lifecycle-corrected" || rows[0].(map[string]any)["person_id"] != "concurrent-0-person" {
		t.Fatal("restart lost correction or identity")
	}
	architectureEmit("concurrent_lifecycle", "ducklake", map[string]any{"cycles": 8, "files_processed": progress, "verdict": "VERIFIED"})
}

func TestDuckLakeMaintenanceDoesNotTakeProjectionLock(t *testing.T) {
	s := openLakeStorageTest(t, t.TempDir(), 1)
	for batch := 0; batch < 20; batch++ {
		var projections []eventProjection
		for row := 0; row < 25; row++ {
			i := batch*25 + row
			projections = append(projections, lakeStorageProjection(t, "unlocked", fmt.Sprintf("unlocked-%d", i), int64(i+1)))
		}
		if err := s.Apply(context.Background(), projections); err != nil {
			t.Fatal(err)
		}
	}
	p, release, err := s.leaseProject(context.Background(), "unlocked")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	before, err := lakeMaintenanceFileCount(context.Background(), p, "current_events")
	if err != nil || before < 2 {
		t.Fatalf("missing compaction fixture %d %v", before, err)
	}
	if err := p.write.Lock(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer p.write.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.Maintain(ctx); err != nil {
		t.Fatalf("maintenance blocked by projection lock: %v", err)
	}
	after, err := lakeMaintenanceFileCount(ctx, p, "current_events")
	if err != nil || after >= before {
		t.Fatalf("default maintenance made no progress: %d -> %d %v", before, after, err)
	}
	var count int
	if err := p.db.QueryRowContext(ctx, "SELECT count(*) FROM events").Scan(&count); err != nil || count != 500 {
		t.Fatalf("maintenance changed rows: %d %v", count, err)
	}
}

type delayedMaintenanceStore struct {
	lakeStore
	started, finished chan time.Time
	release           chan struct{}
}

func (s *delayedMaintenanceStore) Maintain(ctx context.Context) error {
	select {
	case s.started <- time.Now():
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-s.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case s.finished <- time.Now():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func TestDuckLakeMaintenanceWaitsBetweenCompletedPasses(t *testing.T) {
	s := service(t, 100)
	lake, err := newDuckLake(DuckLakeConfig{Directory: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	observed := &delayedMaintenanceStore{lakeStore: lake, started: make(chan time.Time, 4), finished: make(chan time.Time, 4), release: make(chan struct{})}
	s.lake = observed
	interval := 20 * time.Millisecond
	s.startLakeMaintenance(interval)
	defer func() { s.lakeMaintenanceCancel(); <-s.lakeMaintenanceDone }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	select {
	case <-observed.started:
	case <-ctx.Done():
		t.Fatal("first maintenance did not start")
	}
	time.Sleep(3 * interval)
	close(observed.release)
	var finish, next time.Time
	select {
	case finish = <-observed.finished:
	case <-ctx.Done():
		t.Fatal("first maintenance did not finish")
	}
	select {
	case next = <-observed.started:
	case <-ctx.Done():
		t.Fatal("second maintenance did not start")
	}
	if next.Sub(finish) < interval {
		t.Fatalf("maintenance had no completion gap: %s", next.Sub(finish))
	}
}
