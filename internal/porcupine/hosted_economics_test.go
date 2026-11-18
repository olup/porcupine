package porcupine

import (
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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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
	a, b := sharedNativeJourneyPair(t, fixture)
	nodes := []*sharedJourneyNode{a, b}
	var flushers []*hostedLiveFlusher
	for _, node := range nodes {
		node.service.usage.cancel()
		<-node.service.usage.done
		store := node.service.lake.(*lakeapi.DuckLakeStore)
		coordinator, err := lakeapi.NewPostgresProjectCoordinator(node.service.control, lakeapi.DefaultProjectCoordinatorConfig())
		if err != nil {
			t.Fatal(err)
		}
		flusher, err := newHostedLiveFlusher(node.service.control, store, coordinator, node.service.usage.store)
		if err != nil {
			t.Fatal(err)
		}
		flushers = append(flushers, flusher)
	}
	store := a.service.lake.(*lakeapi.DuckLakeStore)
	reader := b.service.lake.(*lakeapi.DuckLakeStore)
	const quietProjects, windows, flushWindowSeconds = 12, 4, 2

	started := time.Now()
	projects := make([][]string, 2)
	for cohort := range projects {
		for i := range quietProjects {
			id, _ := sharedJourneyProject(t, a.host.URL, defaultTenantID, fmt.Sprintf("quiet %d/%d", cohort, i))
			key := lakeapi.ProjectWriteKey{SourceID: store.Config.SharedSourceID, ProjectID: id}
			if _, err := store.InitializeHostedProject(ctx, key); err != nil {
				t.Fatal(err)
			}
			projects[cohort] = append(projects[cohort], id)
		}
	}
	noisyTenant := sharedJourneyHTTP(t, a.host.URL, "POST", "/api/v1/tenants", adminToken, []byte(`{"name":"noisy"}`), nil, 201)["tenant"].(map[string]any)["id"].(string)
	noisyProject, _ := sharedJourneyProject(t, a.host.URL, noisyTenant, "noisy")
	noisyKey := lakeapi.ProjectWriteKey{SourceID: store.Config.SharedSourceID, ProjectID: noisyProject}
	if _, err := store.InitializeHostedProject(ctx, noisyKey); err != nil {
		t.Fatal(err)
	}
	publisher := flushers[0].publisher
	if err := publisher.transaction(ctx, func(tx *sql.Tx) error {
		limits := output.PipelineDemand{References: 100, QueuedBytes: 1 << 20, MetadataRows: 1000, ArtifactBytes: 128 << 20, ProofSlots: 4}
		scopes := [][2]string{{"tenant", defaultTenantID}, {"tenant", noisyTenant}, {"project", noisyProject}}
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
	reportHostedEconomics(t, fixture, traffic, "setup", quietProjects*2+1, 0, 0, started, runtime.GOOS+"/"+runtime.GOARCH)

	epoch := time.Now().UTC().Add(-windows * time.Minute)
	flush := func(flusher *hostedLiveFlusher, project string, window, count int, span, step time.Duration) {
		t.Helper()
		batch := bufferedFlushBatch{id: eventapi.NewID(), projectID: project}
		at := epoch.Add(time.Duration(window) * span)
		for i := range count {
			instant := at.Add(time.Duration(i) * step).Format(time.RFC3339Nano)
			input, err := validateLiveEvent(eventapi.Event{
				ProjectID: project, UUID: eventapi.NewID(), Event: "page",
				DistinctID: fmt.Sprintf("visitor-%d", i%100),
				Properties: json.RawMessage(`{"page":"/pricing","plan":"team"}`),
				Context:    json.RawMessage(`{}`), Timestamp: instant, ReceivedAt: instant,
			})
			if err != nil {
				t.Fatal(err)
			}
			batch.events = append(batch.events, bufferedLiveEvent{input: input, admittedAt: time.Now().UTC()})
		}
		if err := flusher.FlushLive(ctx, batch); err != nil {
			t.Fatalf("flush %s window %d (%d events): %v", project, window, count, err)
		}
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
		reportHostedEconomics(t, fixture, traffic, scenario.phase,
			quietProjects, quietProjects*windows*scenario.eventsPerWindow, quietProjects*windows, started,
			map[string]int{"modeled_seconds": windows * scenario.spanSeconds, "modeled_flush_window_seconds": flushWindowSeconds})
	}

	const noisyEventsPerWindow = 500
	started = time.Now()
	for window := range windows {
		flush(flushers[0], noisyProject, window, noisyEventsPerWindow, flushWindowSeconds*time.Second, flushWindowSeconds*time.Second/noisyEventsPerWindow)
	}
	reportHostedEconomics(t, fixture, traffic, "noisy_flush", 1, windows*noisyEventsPerWindow, windows, started, nil)

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
