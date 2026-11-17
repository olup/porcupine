package porcupine

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	lakeapi "github.com/olup/porcupine/internal/lake"
)

func TestDuckLakeS3SharedPostgresHostedBufferedVisibility(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	if os.Getenv("PORCUPINE_TEST_HOSTED_BUFFERED_VISIBILITY") != "1" {
		t.Skip("opt in to the timed hosted visibility and loss experiment")
	}
	fixture := nativeS3Fixture(t)
	dsn := sharedPostgresDSN(t)
	target, err := url.Parse(fixture.storage.Endpoint)
	if err != nil {
		t.Fatal(err)
	}
	forward := httputil.NewSingleHostReverseProxy(target)
	forward.Transport = fixture.client.Options().HTTPClient.(*http.Client).Transport
	var blocked atomic.Bool
	var rejected atomic.Int64
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if blocked.Load() && r.Method == http.MethodPut {
			w.WriteHeader(http.StatusServiceUnavailable)
			rejected.Add(1)
			return
		}
		forward.ServeHTTP(w, r)
	}))
	t.Cleanup(proxy.Close)
	storage := fixture.storage
	storage.Endpoint, storage.CACertFile = proxy.URL, ""
	start := func(t *testing.T, interval time.Duration) *sharedJourneyProcess {
		lake := lakeapi.DuckLakeConfig{
			PostgresURL: dsn, Directory: t.TempDir(), S3: &storage, MemoryLimitMB: 128, Threads: 1,
			MaxOpenProjects: 2, CleanupGrace: 10 * 24 * time.Hour,
			RestoreWindow: 24 * time.Hour, RestoreMaxReaderTime: time.Hour, RestoreSafetyMargin: time.Hour,
		}
		return startSharedJourneyProcess(t, sharedJourneyProcessConfig{
			DatabaseURL: dsn, HostedLake: &lake, HostedFlushInterval: interval,
		})
	}
	for _, setting := range []struct {
		name     string
		override time.Duration
	}{
		{"default-2s", 0},
		{"test-60s", time.Minute},
	} {
		t.Run(setting.name, func(t *testing.T) {
			a, b := start(t, setting.override), start(t, setting.override)
			projectA := newHostedVisibilityProject(t, a, b, setting.name+"-a")
			projectB := newHostedVisibilityProject(t, b, a, setting.name+"-b")
			// Warm the opposite workers' query path while both projects are empty.
			for _, project := range []hostedVisibilityProject{projectA, projectB} {
				if project.count(t, project.reader, "normal") != 0 {
					t.Fatal("unexpected pre-existing events")
				}
			}
			type receipt struct {
				project int
				at      time.Time
				latency time.Duration
			}
			projects := []hostedVisibilityProject{projectA, projectB}
			receipts := make(map[string]receipt)
			const perProject = 6
			for i := 0; i < perProject; i++ {
				for j, project := range projects {
					uuid, at := project.receive(t, fmt.Sprintf("normal-%d-%d", j, i), "normal")
					receipts[uuid] = receipt{project: j, at: at}
				}
				if i < perProject-1 {
					time.Sleep(200 * time.Millisecond)
				}
			}
			if len(receipts) != perProject*len(projects) {
				t.Fatalf("expected distinct receipts for every input, got %d", len(receipts))
			}
			deadline := time.Now().Add(setting.override + 20*time.Second)
			var latencies []time.Duration
			for len(latencies) < len(receipts) && time.Now().Before(deadline) {
				for j, project := range projects {
					rows := project.rows(t, project.reader, "normal")
					observedAt := time.Now()
					for _, raw := range rows {
						uuid := raw.(map[string]any)["uuid"].(string)
						r, ok := receipts[uuid]
						if !ok || r.project != j {
							t.Fatalf("unexpected UUID %s in project %d", uuid, j)
						}
						if r.latency == 0 {
							r.latency = observedAt.Sub(r.at)
							receipts[uuid] = r
							latencies = append(latencies, r.latency)
						}
					}
				}
				if len(latencies) < len(receipts) {
					time.Sleep(250 * time.Millisecond)
				}
			}
			if len(latencies) != len(receipts) {
				t.Fatalf("opposite workers saw %d/%d receipts", len(latencies), len(receipts))
			}
			slices.Sort(latencies)
			t.Logf("observed receive-to-query samples=%d min=%s median=%s max=%s all=%v",
				len(latencies), latencies[0], (latencies[len(latencies)/2-1]+latencies[len(latencies)/2])/2, latencies[len(latencies)-1], latencies)

			// The opposite worker already observed publication of the normal rows.
			// Kill the writer with a fresh receipt still inside its timer window.
			_, receivedAt := projectA.receive(t, "before-kill", "before-kill")
			a.stopProcess(t, true)
			killElapsed := time.Since(receivedAt)
			cold := start(t, setting.override)
			for _, reader := range []*sharedJourneyProcess{b, cold} {
				if got := projectA.count(t, reader, "normal"); got != perProject {
					t.Fatalf("after kill, published rows=%d want=%d", got, perProject)
				}
				if got := projectA.count(t, reader, "before-kill"); got != 0 {
					t.Fatalf("after kill, unpublished rows=%d want=0", got)
				}
			}
			t.Logf("SIGKILL completed %s after fresh receipt. Cold and surviving readers each saw %d/%d previously queryable rows and 0/1 from the fresh receipt", killElapsed, perProject, perProject)

			if setting.override != 0 {
				return
			}
			projectA.writer = cold
			t.Cleanup(func() { blocked.Store(false) })
			interruptWrite := func(name string) time.Time {
				t.Helper()
				prior := rejected.Load()
				blocked.Store(true)
				_, acknowledged := projectA.receive(t, name, name)
				deadline := acknowledged.Add(12 * time.Second)
				// Hold the fault beyond the healthy timer and observe real rejected writes.
				for rejected.Load() == prior || time.Since(acknowledged) < 5*time.Second {
					if time.Now().After(deadline) {
						t.Fatal("S3 interruption did not reject a write")
					}
					time.Sleep(100 * time.Millisecond)
				}
				if got := projectA.count(t, b, name); got != 0 {
					t.Fatalf("query during interrupted writes found %d rows", got)
				}
				return acknowledged
			}
			acknowledged := interruptWrite("provider-recovery")
			blocked.Store(false)
			restored := time.Now()
			deadline = restored.Add(20 * time.Second)
			for projectA.count(t, b, "provider-recovery") != 1 {
				if time.Now().After(deadline) {
					t.Fatal("writer did not publish the buffered receipt after S3 writes recovered")
				}
				time.Sleep(250 * time.Millisecond)
			}
			t.Logf("S3 write interruption with live worker. Unavailable after receipt for %s, recovered query visibility %s after writes restored (%s since receipt)",
				restored.Sub(acknowledged), time.Since(restored), time.Since(acknowledged))

			acknowledged = interruptWrite("provider-kill")
			cold.stopProcess(t, true)
			killAge := time.Since(acknowledged)
			blocked.Store(false)
			restarted := start(t, setting.override)
			if got := projectA.count(t, restarted, "provider-kill"); got != 0 {
				t.Fatalf("cold reader saw %d rows buffered during interrupted writes", got)
			}
			if got := projectA.count(t, restarted, "provider-recovery"); got != 1 {
				t.Fatalf("cold reader saw %d rows recovered before the kill", got)
			}
			t.Logf("SIGKILL during S3 write interruption. Cold reader saw 0/1 interrupted receipts (oldest %s at kill) and 1/1 from the earlier recovered write", killAge)
		})
	}
}

type hostedVisibilityProject struct {
	id, ingest, key string
	writer, reader  *sharedJourneyProcess
}

func newHostedVisibilityProject(t *testing.T, writer, reader *sharedJourneyProcess, name string) hostedVisibilityProject {
	t.Helper()
	id, ingest := sharedJourneyProject(t, writer.origin, defaultTenantID, name)
	_, key := sharedJourneyKey(t, reader.origin, id, []string{"events:read"})
	return hostedVisibilityProject{id: id, ingest: ingest, key: key, writer: writer, reader: reader}
}

func (p hostedVisibilityProject) receive(t *testing.T, id, name string) (string, time.Time) {
	t.Helper()
	payload := fmt.Sprintf(`{"type":"track","message_id":%q,"name":%q,"actor_id":"visibility-probe"}`, id, name)
	ack := sharedJourneyNative(t, p.writer.origin, p.ingest, payload, 200)
	at := time.Now()
	if ack["durability"] != "buffered" || ack["received"] != float64(1) {
		t.Fatalf("expected a buffered receipt, got %v", ack)
	}
	return ack["receipts"].([]any)[0].(map[string]any)["uuid"].(string), at
}

func (p hostedVisibilityProject) rows(t *testing.T, reader *sharedJourneyProcess, name string) []any {
	t.Helper()
	sql := "SELECT uuid FROM events WHERE event='" + strings.ReplaceAll(name, "'", "''") + "'"
	result := sharedJourneyQuery(t, reader.origin, p.id, p.key, sql, 200)
	rows, _ := result["result"].(map[string]any)["data"].([]any)
	return rows
}

func (p hostedVisibilityProject) count(t *testing.T, reader *sharedJourneyProcess, name string) int {
	t.Helper()
	return len(p.rows(t, reader, name))
}
