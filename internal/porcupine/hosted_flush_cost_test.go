package porcupine

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/acceptance"
	eventapi "github.com/olup/porcupine/internal/event"
	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/output"
)

type hostedFlushSample struct {
	ms        float64
	requests  map[string]int
	parquet   map[string]map[string]int
	snapshots int64
}

type hostedFlushCostRow struct {
	Phase          string `json:"phase"`
	Encrypted      bool   `json:"encrypted"`
	Webhook        bool   `json:"webhook"`
	HistoryBefore  int64  `json:"history_before"`
	EventsPerFlush int    `json:"events_per_flush"`
	Flushes        int    `json:"flushes"`
	LatencyMS      struct {
		P50  float64 `json:"p50"`
		P95  float64 `json:"p95"`
		Max  float64 `json:"max"`
		Mean float64 `json:"mean"`
	} `json:"latency_ms"`
	S3ByMethod        map[string]float64            `json:"s3_per_flush_by_method"`
	S3Requests        map[string]float64            `json:"s3_per_flush"`
	S3ParquetByTable  map[string]map[string]float64 `json:"s3_parquet_per_flush_by_table"`
	SnapshotsPerFlush float64                       `json:"ducklake_snapshots_per_flush"`
}

func summarizeHostedFlushes(phase string, encrypted, webhook bool, history int64, events int, samples []hostedFlushSample) hostedFlushCostRow {
	row := hostedFlushCostRow{Phase: phase, Encrypted: encrypted, Webhook: webhook, HistoryBefore: history, EventsPerFlush: events, Flushes: len(samples),
		S3ByMethod: map[string]float64{}, S3Requests: map[string]float64{}, S3ParquetByTable: map[string]map[string]float64{}}
	if len(samples) == 0 {
		return row
	}
	n := float64(len(samples))
	latencies := make([]float64, 0, len(samples))
	for _, s := range samples {
		latencies = append(latencies, s.ms)
		row.LatencyMS.Mean += s.ms / n
		for request, count := range s.requests {
			row.S3Requests[request] += float64(count) / n
			method, _, _ := strings.Cut(request, "/")
			row.S3ByMethod[method] += float64(count) / n
		}
		for table, requests := range s.parquet {
			if row.S3ParquetByTable[table] == nil {
				row.S3ParquetByTable[table] = map[string]float64{}
			}
			for request, count := range requests {
				row.S3ParquetByTable[table][request] += float64(count) / n
			}
		}
		row.SnapshotsPerFlush += float64(s.snapshots) / n
	}
	slices.Sort(latencies)
	rank := func(p float64) float64 { // nearest rank
		return latencies[max(0, int(math.Ceil(p*float64(len(latencies))))-1)]
	}
	row.LatencyMS.P50, row.LatencyMS.P95, row.LatencyMS.Max = rank(0.5), rank(0.95), latencies[len(latencies)-1]
	return row
}

func hostedFlushCostEnv(t *testing.T, name string, fallback string) string {
	t.Helper()
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

func hostedFlushCostInt(t *testing.T, raw string) int {
	t.Helper()
	v, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || v < 0 {
		t.Fatalf("invalid flush cost parameter %q", raw)
	}
	return v
}

// TestDuckLakeS3SharedPostgresHostedFlushCost records the cost of one hosted
// live flush; see docs/design/archive/hosted-flush-cost.md. It is opt-in so CI cost
// does not change. Set PORCUPINE_MEASURE_HOSTED_FLUSH_ENCRYPTED=true to use
// DuckLake Parquet encryption.
func TestDuckLakeS3SharedPostgresHostedFlushCost(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	if os.Getenv("PORCUPINE_MEASURE_HOSTED_FLUSH") != "1" {
		t.Skip("opt in with PORCUPINE_MEASURE_HOSTED_FLUSH=1; see docs/design/archive/hosted-flush-cost.md")
	}
	flushes := hostedFlushCostInt(t, hostedFlushCostEnv(t, "PORCUPINE_MEASURE_HOSTED_FLUSH_N", "20"))
	events := hostedFlushCostInt(t, hostedFlushCostEnv(t, "PORCUPINE_MEASURE_HOSTED_FLUSH_EVENTS", "100"))
	encrypted, err := strconv.ParseBool(hostedFlushCostEnv(t, "PORCUPINE_MEASURE_HOSTED_FLUSH_ENCRYPTED", "false"))
	if err != nil {
		t.Fatal("PORCUPINE_MEASURE_HOSTED_FLUSH_ENCRYPTED must be a boolean")
	}
	var histories []int64
	for _, raw := range strings.Split(hostedFlushCostEnv(t, "PORCUPINE_MEASURE_HOSTED_FLUSH_HISTORY", "0,200"), ",") {
		histories = append(histories, int64(hostedFlushCostInt(t, raw)))
	}
	if flushes == 0 || events == 0 || !slices.IsSorted(histories) {
		t.Fatal("flush cost parameters need positive N and events and ascending histories")
	}
	ctx := t.Context()
	fixture := nativeS3Fixture(t)
	traffic := economicsS3Proxy(t, &fixture)
	control := sharedPostgresDSN(t)
	config := hostedJourneyConfig(t, control, fixture)
	config.DuckLake.Encrypted = encrypted
	config.OAuth = &OAuthConfig{Issuer: "http://127.0.0.1:42999", AllowLegacyKeys: true}
	config.Pipelines = &output.PipelineConfig{Key: bytes.Repeat([]byte{1}, 32)}
	service, host, _ := hostedCleanupWorker(t, config)
	service.pipelines.Stop()
	service.usage.cancel()
	<-service.usage.done
	direct, err := sql.Open("pgx", control)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { direct.Close() })

	flusher := service.hosted.flusher
	store := flusher.native
	project, _ := sharedJourneyProject(t, host.URL, defaultTenantID, "flush cost")
	key := lakeapi.ProjectWriteKey{SourceID: store.Config.SharedSourceID, ProjectID: project}
	if _, err := store.InitializeHostedProject(ctx, key); err != nil {
		t.Fatal(err)
	}
	permissions := []string{"pipelines:write", "events:read"}
	accountBody, _ := json.Marshal(TenantServiceAccountSpec{Name: "flush cost webhook", Projects: []string{project}, Permissions: permissions})
	account := sharedJourneyHTTP(t, host.URL, "POST", "/api/v1/tenants/default/service-accounts", adminToken, accountBody, nil, 201)
	keyBody, _ := json.Marshal(ServiceKeySpec{Name: "flush cost webhook key", Audience: "api", Projects: []string{project}, Permissions: permissions})
	creator := sharedJourneyHTTP(t, host.URL, "POST", "/api/v1/tenants/default/service-accounts/"+account["client_id"].(string)+"/api-keys", adminToken, keyBody, nil, 201)["token"].(string)
	body, _ := json.Marshal(PipelineCreateBody{Name: "all events", Kind: "webhook", Webhook: &output.WebhookPipelineSpec{
		Endpoint: "https://example.test/unused", DeliveryMode: "event",
	}})
	pipelines := "/api/v1/projects/" + project + "/pipelines"
	webhookID := sharedJourneyHTTP(t, host.URL, "POST", pipelines, creator, body, nil, 201)["id"].(string)
	webhook := func(action string) {
		t.Helper()
		sharedJourneyHTTP(t, host.URL, "POST", pipelines+"/"+webhookID+"/"+action, creator, nil, nil, 200)
	}
	webhook("pause")

	// Keep all measured jobs without a background sender consuming the queue.
	// Pipeline creation initialized all capacity rows for this isolated database.
	if _, err := direct.ExecContext(ctx, `UPDATE pipeline_capacity SET
		limit_refs=1000000, limit_rows=4000000, limit_bytes=1099511627776, limit_artifacts=1099511627776`); err != nil {
		t.Fatal(err)
	}

	schema := lakeapi.SharedNativeSchema(lakeapi.SharedNativeCatalogID(key.SourceID, project))
	readSnapshot := func() int64 {
		t.Helper()
		var id int64
		if err := direct.QueryRowContext(ctx, `SELECT coalesce(max(snapshot_id),0) FROM "`+schema+`".ducklake_snapshot`).Scan(&id); err != nil {
			t.Fatal(err)
		}
		return id
	}
	history := func() int64 {
		t.Helper()
		var seq int64
		err := direct.QueryRowContext(ctx, `SELECT sequence FROM pipeline_effect_streams WHERE project_id=$1`, project).Scan(&seq)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			t.Fatal(err)
		}
		return seq
	}
	drain := func() (map[string]int, map[string]map[string]int) {
		traffic.mu.Lock()
		defer traffic.mu.Unlock()
		calls, parquet := traffic.calls, traffic.parquetCalls
		traffic.calls, traffic.parquetCalls = map[string]int{}, map[string]map[string]int{}
		return calls, parquet
	}
	at := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	flush := func(count int) hostedFlushSample {
		t.Helper()
		batch := acceptance.FlushBatch{ID: eventapi.NewID(), ProjectID: project}
		for i := range count {
			at = at.Add(time.Millisecond)
			instant := at.Format(time.RFC3339Nano)
			digest := sha256.Sum256([]byte(instant))
			digest[6] = digest[6]&0x0f | 0x40
			digest[8] = digest[8]&0x3f | 0x80
			uuid := fmt.Sprintf("%x-%x-%x-%x-%x", digest[:4], digest[4:6], digest[6:8], digest[8:10], digest[10:16])
			input, err := acceptance.ValidateInput(eventapi.Event{
				ProjectID: project, UUID: uuid, Event: "page", DistinctID: fmt.Sprintf("visitor-%d", i%100),
				Properties: json.RawMessage(`{"page":"/pricing","plan":"team"}`), Context: json.RawMessage(`{}`), Timestamp: instant, ReceivedAt: instant,
			})
			if err != nil {
				t.Fatal(err)
			}
			batch.Events = append(batch.Events, acceptance.BufferedEvent{Input: input, AdmittedAt: time.Now().UTC()})
		}
		drain()
		before := readSnapshot()
		started := time.Now()
		if err := flusher.FlushLive(ctx, batch); err != nil {
			t.Fatalf("flush of %d events: %v", count, err)
		}
		sample := hostedFlushSample{ms: float64(time.Since(started)) / float64(time.Millisecond)}
		sample.requests, sample.parquet = drain()
		sample.snapshots = readSnapshot() - before
		return sample
	}
	jobs := func() int {
		t.Helper()
		var count int
		if err := direct.QueryRowContext(ctx, `SELECT count(*) FROM pipeline_effect_jobs WHERE project_id=$1 AND pipeline_id=$2`, project, webhookID).Scan(&count); err != nil {
			t.Fatal(err)
		}
		return count
	}
	report := func(phase string, hooked bool, count, n int) {
		t.Helper()
		start, jobsBefore := history(), jobs()
		samples := make([]hostedFlushSample, 0, n)
		for range n {
			samples = append(samples, flush(count))
		}
		if after := history(); after-start != int64(n) {
			t.Fatalf("published %d batches, want %d", after-start, n)
		}
		wantJobs := 0
		if hooked {
			wantJobs = n * count
		}
		if got := jobs() - jobsBefore; got != wantJobs {
			t.Fatalf("published %d webhook jobs, want %d", got, wantJobs)
		}
		raw, err := json.Marshal(summarizeHostedFlushes(phase, encrypted, hooked, start, count, samples))
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("hosted-flush-cost %s", raw)
	}

	for _, k := range histories {
		// Fill with one-event flushes, in chunks so latency growth stays visible.
		for fill := k - history(); fill > 0; fill = k - history() {
			report("history_fill", false, 1, int(min(fill, 500)))
		}
		report("flush", false, events, flushes)
		webhook("resume")
		report("flush", true, events, flushes)
		webhook("pause")
	}
	session, err := flusher.coordinator.Acquire(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	drain()
	stats, err := store.MaintainHostedProject(ctx, session)
	if err := errors.Join(err, session.Release()); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(struct {
		Encrypted bool `json:"encrypted"`
		lakeapi.DuckLakeMaintenanceStats
	}{encrypted, stats})
	t.Logf("hosted-flush-cost-maintenance %s", raw)
	report("flush_after_maintenance", false, events, flushes)
}
