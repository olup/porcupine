package porcupine

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/olup/porcupine/internal/acceptance"
	eventapi "github.com/olup/porcupine/internal/event"
	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/output"
)

type economicsS3Traffic struct {
	mu               sync.Mutex
	calls            map[string]int
	parquetCalls     map[string]map[string]int
	upload, download atomic.Int64
}

type economicsS3Body struct {
	io.ReadCloser
	bytes *atomic.Int64
}

func (s *economicsS3Traffic) puts() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	puts := 0
	for request, count := range s.calls {
		if strings.HasPrefix(request, "PUT/") || strings.HasPrefix(request, "UPLOAD_PART/") {
			puts += count
		}
	}
	return puts
}

func (b economicsS3Body) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.bytes.Add(int64(n))
	return n, err
}

func economicsS3Proxy(t *testing.T, fixture *duckLakeS3Fixture) *economicsS3Traffic {
	t.Helper()
	target, err := url.Parse(fixture.storage.Endpoint)
	if err != nil {
		t.Fatal(err)
	}
	traffic := &economicsS3Traffic{calls: make(map[string]int), parquetCalls: make(map[string]map[string]int)}
	record := func(r *http.Request, status int) {
		op := r.Method
		switch {
		case r.URL.Query().Has("list-type"):
			op = "LIST"
		case r.Method == http.MethodPost && r.URL.Query().Has("uploads"):
			op = "CREATE_MULTIPART"
		case r.Method == http.MethodPost && r.URL.Query().Has("uploadId"):
			op = "COMPLETE_MULTIPART"
		case r.Method == http.MethodPut && r.URL.Query().Has("partNumber"):
			op = "UPLOAD_PART"
		}
		traffic.mu.Lock()
		request := fmt.Sprintf("%s/%d", op, status)
		traffic.calls[request]++
		if path.Ext(r.URL.Path) == ".parquet" {
			dir := path.Dir(r.URL.Path)
			for strings.Contains(path.Base(dir), "=") {
				dir = path.Dir(dir)
			}
			directory := path.Base(dir)
			if traffic.parquetCalls[directory] == nil {
				traffic.parquetCalls[directory] = make(map[string]int)
			}
			traffic.parquetCalls[directory][request]++
		}
		traffic.mu.Unlock()
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.Transport = fixture.client.Options().HTTPClient.(*http.Client).Transport
	proxy.ModifyResponse = func(response *http.Response) error {
		record(response.Request, response.StatusCode)
		response.Body = economicsS3Body{response.Body, &traffic.download}
		return nil
	}
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, _ error) {
		record(r, http.StatusBadGateway)
		w.WriteHeader(http.StatusBadGateway)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Body != nil {
			r.Body = economicsS3Body{r.Body, &traffic.upload}
		}
		proxy.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	fixture.storage.Endpoint, fixture.storage.CACertFile = server.URL, ""
	return traffic
}

type economicsInventory struct {
	ParquetObjects int   `json:"parquet_objects"`
	ParquetBytes   int64 `json:"parquet_bytes"`
	OtherObjects   int   `json:"other_objects"`
	OtherBytes     int64 `json:"other_bytes"`
}

func economicsList(t *testing.T, fixture duckLakeS3Fixture) economicsInventory {
	t.Helper()
	var inventory economicsInventory
	pages := s3.NewListObjectsV2Paginator(fixture.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(fixture.storage.Bucket), Prefix: aws.String(fixture.storage.Prefix + "/"),
	})
	for pages.HasMorePages() {
		page, err := pages.NextPage(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		for _, object := range page.Contents {
			if path.Ext(aws.ToString(object.Key)) == ".parquet" {
				inventory.ParquetObjects++
				inventory.ParquetBytes += aws.ToInt64(object.Size)
			} else {
				inventory.OtherObjects++
				inventory.OtherBytes += aws.ToInt64(object.Size)
			}
		}
	}
	return inventory
}

func reportHostedEconomics(t *testing.T, fixture duckLakeS3Fixture, traffic *economicsS3Traffic, phase string, projects, events, flushes int, started time.Time, details any) {
	t.Helper()
	elapsed := time.Since(started).Seconds()
	traffic.mu.Lock()
	calls := traffic.calls
	traffic.calls = make(map[string]int)
	parquetCalls := traffic.parquetCalls
	traffic.parquetCalls = make(map[string]map[string]int)
	traffic.mu.Unlock()
	row, err := json.Marshal(map[string]any{
		"phase": phase, "projects": projects, "events": events, "flushes": flushes,
		"elapsed_seconds": elapsed, "requests": calls, "parquet_requests_by_directory": parquetCalls, "upload_body_bytes": traffic.upload.Swap(0),
		"download_body_bytes": traffic.download.Swap(0), "inventory": economicsList(t, fixture),
		"details": details,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("hosted-economics %s", row)
}

func TestDuckLakeS3SharedPostgresHostedEconomics(t *testing.T) {
	t.Parallel()
	if os.Getenv("PORCUPINE_TEST_HOSTED_ECONOMICS") != "1" {
		t.Skip("opt in with PORCUPINE_TEST_HOSTED_ECONOMICS=1; see docs/design/archive/hosted-economics.md")
	}
	dsn, err := url.Parse(os.Getenv("PORCUPINE_TEST_POSTGRES_DSN"))
	if err != nil || !net.ParseIP(dsn.Hostname()).IsLoopback() || os.Getenv("PORCUPINE_TEST_DUCKLAKE_S3_ENDPOINT") == "" {
		t.Fatal("hosted economics requires the disposable loopback PostgreSQL and S3 fixtures")
	}
	ctx := t.Context()
	fixture := nativeS3Fixture(t)
	traffic := economicsS3Proxy(t, &fixture)
	a, b := sharedNativeJourneyPair(t, fixture, func(c *Config) {
		c.OAuth = &OAuthConfig{Issuer: "http://127.0.0.1:42999"}
		c.Pipelines = &output.PipelineConfig{Key: bytes.Repeat([]byte{1}, 32)}
	})
	nodes := []*sharedJourneyNode{a, b}
	var flushers []*hostedLiveFlusher
	for _, node := range nodes {
		node.service.pipelines.Stop()
		node.service.usage.cancel()
		<-node.service.usage.done
		flushers = append(flushers, node.service.hosted.flusher)
	}
	store := a.service.lake
	reader := b.service.lake
	const quietProjects, windows, flushWindowSeconds = 12, 4, 2

	admin := accountTestToken(t, a.service)
	started := time.Now()
	projects := make([][]string, 2)
	for cohort := range projects {
		for i := range quietProjects {
			id, _ := sharedJourneyProjectAs(t, a.host.URL, admin, defaultTenantID, fmt.Sprintf("quiet %d/%d", cohort, i))
			key := lakeapi.ProjectWriteKey{SourceID: store.Config.SharedSourceID, ProjectID: id}
			if _, err := store.InitializeHostedProject(ctx, key); err != nil {
				t.Fatal(err)
			}
			projects[cohort] = append(projects[cohort], id)
		}
	}
	noisyTenant := sharedJourneyHTTP(t, a.host.URL, "POST", "/api/v1/tenants", admin, []byte(`{"name":"noisy"}`), nil, 201)["tenant"].(map[string]any)["id"].(string)
	noisyProject, _ := sharedJourneyProjectAs(t, a.host.URL, admin, noisyTenant, "noisy")
	noisyKey := lakeapi.ProjectWriteKey{SourceID: store.Config.SharedSourceID, ProjectID: noisyProject}
	webhookProject, _ := sharedJourneyProjectAs(t, a.host.URL, admin, noisyTenant, "noisy webhook")
	_, creator := sharedJourneyServiceKey(t, a.host.URL, admin, noisyTenant, "economics webhook", webhookProject, []string{"pipelines:write", "events:read"})
	webhookBody, _ := json.Marshal(PipelineCreateBody{Name: "all events", Kind: "webhook", Webhook: &output.WebhookPipelineSpec{
		Endpoint: "https://example.test/unused", DeliveryMode: "batch", IntervalSeconds: 60,
		StartAt: time.Now().UTC().Add(-time.Hour).Truncate(time.Minute).Format(time.RFC3339),
	}})
	webhookID := sharedJourneyHTTP(t, a.host.URL, "POST", "/api/v1/projects/"+webhookProject+"/pipelines", creator, webhookBody, nil, 201)["id"].(string)
	const noisyEventsPerWindow = 500
	publisher := flushers[0].publisher
	if err := publisher.transaction(ctx, func(tx *sql.Tx) error {
		limits := output.PipelineDemand{References: 10_000, QueuedBytes: 32 << 20, MetadataRows: 20_000, ArtifactBytes: 1 << 30, ProofSlots: 4}
		scopes := [][2]string{{"tenant", defaultTenantID}, {"tenant", noisyTenant}, {"project", noisyProject}, {"project", webhookProject}}
		for _, cohort := range projects {
			for _, project := range cohort {
				scopes = append(scopes, [2]string{"project", project})
			}
		}
		for _, scope := range scopes {
			if err := publisher.effects.ConfigureCapacityTx(ctx, tx, scope[0], scope[1], limits); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	const totalProjects = quietProjects*2 + 2
	reportHostedEconomics(t, fixture, traffic, "setup", totalProjects, 0, 0, started, runtime.GOOS+"/"+runtime.GOARCH)

	snapshotCount := func() int64 {
		t.Helper()
		var total int64
		for _, cohort := range append(projects, []string{noisyProject, webhookProject}) {
			for _, project := range cohort {
				schema := lakeapi.SharedNativeSchema(lakeapi.SharedNativeCatalogID(store.Config.SharedSourceID, project))
				var id int64
				if err := store.SharedCatalog.QueryRowContext(ctx, "SELECT max(snapshot_id) FROM "+schema+".ducklake_snapshot").Scan(&id); err != nil {
					t.Fatal(err)
				}
				total += id
			}
		}
		return total
	}
	for _, node := range nodes {
		node.service.hosted.stopRecovery()
		<-node.service.hosted.recoveryDone
	}
	before := snapshotCount()
	started = time.Now()
	for _, node := range nodes {
		node.service.startHostedRecovery(2 * time.Second)
	}
	time.Sleep(time.Minute)
	for _, node := range nodes {
		node.service.hosted.stopRecovery()
		<-node.service.hosted.recoveryDone
	}
	snapshots := snapshotCount() - before
	puts := traffic.puts()
	hours := time.Since(started).Hours()
	reportHostedEconomics(t, fixture, traffic, "idle_recovery", totalProjects, 0, 0, started,
		map[string]any{"write_commits": snapshots, "snapshots": snapshots, "s3_puts": puts,
			"write_commits_per_hour": float64(snapshots) / hours, "snapshots_per_hour": float64(snapshots) / hours, "s3_puts_per_hour": float64(puts) / hours})
	defer func() {
		if snapshots != 0 || puts != 0 {
			t.Errorf("idle recovery wrote %d DuckLake snapshots and issued %d S3 PUTs", snapshots, puts)
		}
	}()
	for _, node := range nodes {
		node.service.startHostedRecovery(2 * time.Second)
	}

	epoch := time.Now().UTC().Truncate(time.Minute).Add(-windows * time.Minute)
	var flushLatencies []float64
	flush := func(flusher *hostedLiveFlusher, project string, window, count int, span, step time.Duration) {
		t.Helper()
		batch := acceptance.FlushBatch{ID: eventapi.NewID(), ProjectID: project}
		at := epoch.Add(time.Duration(window) * span)
		for i := range count {
			instant := at.Add(time.Duration(i) * step).Format(time.RFC3339Nano)
			input, err := acceptance.ValidateInput(eventapi.Event{
				ProjectID: project, UUID: eventapi.NewID(), Event: "page",
				DistinctID: fmt.Sprintf("visitor-%d", i%100),
				Properties: json.RawMessage(`{"page":"/pricing","plan":"team"}`),
				Context:    json.RawMessage(`{}`), Timestamp: instant, ReceivedAt: instant,
			})
			if err != nil {
				t.Fatal(err)
			}
			batch.Events = append(batch.Events, acceptance.BufferedEvent{Input: input, AdmittedAt: time.Now().UTC()})
		}
		start := time.Now()
		if err := flusher.FlushLive(ctx, batch); err != nil {
			t.Fatalf("flush %s window %d (%d events): %v", project, window, count, err)
		}
		flushLatencies = append(flushLatencies, float64(time.Since(start))/float64(time.Millisecond))
	}

	for cohort, scenario := range []struct {
		phase                        string
		eventsPerWindow, spanSeconds int
	}{
		{"quiet_1_per_minute", 1, 60},
		{"quiet_1_per_second", flushWindowSeconds, flushWindowSeconds},
	} {
		started = time.Now()
		for window := range windows {
			for i, project := range projects[cohort] {
				flush(flushers[(window+i)%len(flushers)], project, window, scenario.eventsPerWindow, time.Duration(scenario.spanSeconds)*time.Second, time.Second)
			}
		}
		if cohort == len(projects)-1 {
			for _, node := range nodes {
				node.service.hosted.stopRecovery()
				<-node.service.hosted.recoveryDone
			}
		}
		reportHostedEconomics(t, fixture, traffic, scenario.phase,
			quietProjects, quietProjects*windows*scenario.eventsPerWindow, quietProjects*windows, started,
			map[string]int{"modeled_seconds": windows * scenario.spanSeconds, "modeled_flush_window_seconds": flushWindowSeconds})
	}

	slices.Sort(flushLatencies)
	t.Logf("hosted-economics concurrent recovery, %d flushes, p95 %.2f ms", len(flushLatencies), flushLatencies[(95*len(flushLatencies)+99)/100-1])

	for _, scenario := range []struct{ phase, project string }{
		{"noisy_flush", noisyProject}, {"noisy_flush_webhook", webhookProject},
	} {
		key := lakeapi.ProjectWriteKey{SourceID: store.Config.SharedSourceID, ProjectID: scenario.project}
		schema := lakeapi.SharedNativeSchema(lakeapi.SharedNativeCatalogID(key.SourceID, key.ProjectID))
		var startSnapshot int64
		if err := store.SharedCatalog.QueryRowContext(ctx, "SELECT max(snapshot_id) FROM "+schema+".ducklake_snapshot").Scan(&startSnapshot); err != nil {
			t.Fatal(err)
		}
		started = time.Now()
		for window := range windows {
			flush(flushers[0], key.ProjectID, window, noisyEventsPerWindow, flushWindowSeconds*time.Second, flushWindowSeconds*time.Second/noisyEventsPerWindow)
			// Run the normal recovery selection after each modeled tick to make
			// the cost independent of how long the fixture takes to flush.
			pending, err := hostedRecoveryProjects(ctx, b.service.control, "")
			if err != nil {
				t.Fatal(err)
			}
			if slices.Contains(pending, key.ProjectID) {
				if err := flushers[1].recoverProject(ctx, key); err != nil {
					t.Fatal(err)
				}
			}
		}
		var commits, snapshots int
		if err := store.SharedCatalog.QueryRowContext(ctx,
			"SELECT count(DISTINCT xmin::text),count(*) FROM "+schema+".ducklake_snapshot WHERE snapshot_id>$1", startSnapshot).Scan(&commits, &snapshots); err != nil {
			t.Fatal(err)
		}
		puts := traffic.puts()
		minutes := float64(windows*flushWindowSeconds) / 60
		reportHostedEconomics(t, fixture, traffic, scenario.phase, 1, windows*noisyEventsPerWindow, windows, started,
			map[string]any{"write_commits": commits, "snapshots": snapshots, "s3_puts": puts,
				"modeled_seconds": windows * flushWindowSeconds, "write_commits_per_minute": float64(commits) / minutes,
				"snapshots_per_minute": float64(snapshots) / minutes, "s3_puts_per_minute": float64(puts) / minutes})
		if snapshots != windows || commits != windows {
			t.Errorf("%s wrote %d DuckLake commits and %d snapshots for %d flushes", scenario.phase, commits, snapshots, windows)
		}
	}
	var webhookEvents int
	if err := a.service.control.QueryRowContext(ctx, `SELECT coalesce(sum(refs),0) FROM pipeline_effect_jobs WHERE project_id=$1 AND pipeline_id=$2`,
		webhookProject, webhookID).Scan(&webhookEvents); err != nil || webhookEvents != windows*noisyEventsPerWindow {
		t.Fatalf("webhook queued %d event references, want %d: %v", webhookEvents, windows*noisyEventsPerWindow, err)
	}

	query := func(project string, want int) (float64, error) {
		start := time.Now()
		result, err := reader.Query(ctx, project, "SELECT count(*) AS n FROM events WHERE event=$name", map[string]string{"name": "page"}, lakeapi.DefaultQueryExecutionLimits())
		ms := float64(time.Since(start)) / float64(time.Millisecond)
		if err != nil {
			return ms, err
		}
		if len(result.Data) != 1 || string(result.Data[0]["n"]) != strconv.Quote(strconv.Itoa(want)) {
			return ms, fmt.Errorf("query returned %v; expected %d events", result.Data, want)
		}
		return ms, nil
	}
	maintain := func(ctx context.Context, key lakeapi.ProjectWriteKey) (lakeapi.DuckLakeMaintenanceStats, error) {
		session, err := flushers[0].coordinator.Acquire(ctx, key)
		if err != nil {
			return lakeapi.DuckLakeMaintenanceStats{}, err
		}
		stats, err := store.MaintainHostedProject(ctx, session)
		return stats, errors.Join(err, session.Release())
	}

	started = time.Now()
	noisyCount := windows * noisyEventsPerWindow
	baselineMS, err := query(noisyProject, noisyCount)
	if err != nil {
		t.Fatal(err)
	}
	reportHostedEconomics(t, fixture, traffic, "query_before_maintenance", 1, 0, 0, started, baselineMS)

	started = time.Now()
	type maintenanceResult struct {
		stats      lakeapi.DuckLakeMaintenanceStats
		start, end time.Time
		err        error
	}
	done := make(chan maintenanceResult, 1)
	go func() {
		maintenanceStart := time.Now()
		stats, err := maintain(ctx, noisyKey)
		done <- maintenanceResult{stats: stats, start: maintenanceStart, end: time.Now(), err: err}
	}()
	queryStart := time.Now()
	duringMS, queryErr := query(noisyProject, noisyCount)
	queryEnd := time.Now()
	maintenance := <-done
	if err := errors.Join(queryErr, maintenance.err); err != nil {
		t.Fatal(err)
	}
	overlap := max(0, min(queryEnd.Sub(started), maintenance.end.Sub(started))-max(queryStart.Sub(started), maintenance.start.Sub(started)))
	reportHostedEconomics(t, fixture, traffic, "noisy_maintenance_and_query", 1, 0, 0, started,
		map[string]any{"query_ms": duringMS, "call_overlap_ms": float64(overlap) / float64(time.Millisecond), "maintenance": maintenance.stats})

	started = time.Now()
	afterMS, err := query(noisyProject, noisyCount)
	if err != nil {
		t.Fatal(err)
	}
	reportHostedEconomics(t, fixture, traffic, "query_after_maintenance", 1, 0, 0, started, afterMS)

	started = time.Now()
	quietFiles := make([]lakeapi.DuckLakeMaintenanceStats, 0, len(projects))
	for _, cohort := range projects {
		id := cohort[0]
		stats, err := maintain(ctx, lakeapi.ProjectWriteKey{SourceID: store.Config.SharedSourceID, ProjectID: id})
		if err != nil {
			t.Fatal(err)
		}
		quietFiles = append(quietFiles, stats)
	}
	reportHostedEconomics(t, fixture, traffic, "quiet_maintenance", len(projects), 0, 0, started, quietFiles)

	started = time.Now()
	var quietQueryMS []float64
	for _, item := range []struct {
		id   string
		want int
	}{
		{projects[0][0], windows}, {projects[1][0], windows * flushWindowSeconds},
	} {
		ms, err := query(item.id, item.want)
		if err != nil {
			t.Fatal(err)
		}
		quietQueryMS = append(quietQueryMS, ms)
	}
	reportHostedEconomics(t, fixture, traffic, "quiet_query_after_maintenance", len(quietQueryMS), 0, 0, started, quietQueryMS)
}
