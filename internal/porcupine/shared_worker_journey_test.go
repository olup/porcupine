package porcupine

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/exportstore"
	"github.com/olup/porcupine/internal/importer"
	ingestfmt "github.com/olup/porcupine/internal/ingest"
)

const sharedJourneySecret = "shared-worker-journey-routing-secret-test-only"

type sharedJourneyNode struct {
	service *Service
	host    *httptest.Server
}

func sharedJourneyPair(t *testing.T, wrap func(int, http.Handler) http.Handler) (*sharedJourneyNode, *sharedJourneyNode) {
	t.Helper()
	dsn := sharedPostgresDSN(t)
	var nodes []*sharedJourneyNode
	for i, name := range []string{"journey-a", "journey-b"} {
		host := httptest.NewUnstartedServer(http.NotFoundHandler())
		origin := "http://" + host.Listener.Addr().String()
		s, err := Open(Config{DBPath: filepath.Join(t.TempDir(), "queue.db"), BootstrapToken: adminToken, QueueMaxEvents: 2000,
			Shared: &SharedConfig{DatabaseURL: dsn, WorkerID: name, RouteURL: origin, RoutingSecret: []byte(sharedJourneySecret)}})
		if err != nil {
			host.Close()
			t.Fatal("open shared worker", name, err)
		}
		handler := s.Handler()
		if wrap != nil {
			handler = wrap(i, handler)
		}
		host.Config.Handler = handler
		host.Start()
		t.Cleanup(func() { host.Close(); s.Close() })
		nodes = append(nodes, &sharedJourneyNode{service: s, host: host})
	}
	return nodes[0], nodes[1]
}

func sharedJourneyHTTP(t *testing.T, origin, method, path, credential string, body []byte, headers map[string]string, want int) map[string]any {
	t.Helper()
	r, err := http.NewRequestWithContext(t.Context(), method, origin+path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if len(body) > 0 {
		r.Header.Set("Content-Type", "application/json")
	}
	if credential != "" {
		r.Header.Set("Authorization", "Bearer "+credential)
	}
	for name, value := range headers {
		r.Header.Set(name, value)
	}
	client := &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Do(r)
	if err != nil {
		t.Fatal("shared HTTP request", method, path, err)
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	if err != nil || response.StatusCode != want {
		t.Fatalf("%s %s: status=%d want=%d error=%v body=%s", method, path, response.StatusCode, want, err, raw)
	}
	result := map[string]any{}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &result); err != nil {
			t.Fatalf("%s %s returned non-JSON: %s %v", method, path, raw, err)
		}
	}
	return result
}

func sharedJourneyProject(t *testing.T, origin, tenant, name string) (string, string) {
	t.Helper()
	raw, _ := json.Marshal(map[string]any{"name": name, "tenant_id": tenant})
	created := sharedJourneyHTTP(t, origin, "POST", "/api/v1/projects", adminToken, raw, nil, 201)
	return created["project"].(map[string]any)["id"].(string), created["ingest_token"].(string)
}

func sharedJourneyKey(t *testing.T, origin, project string, permissions []string) (string, string) {
	t.Helper()
	raw, _ := json.Marshal(map[string]any{"name": "shared journey reader", "permissions": permissions, "projects": []string{project}})
	created := sharedJourneyHTTP(t, origin, "POST", "/api/v1/access-keys", adminToken, raw, nil, 201)
	return created["id"].(string), created["token"].(string)
}

func sharedJourneyNative(t *testing.T, origin, token, body string, want int) map[string]any {
	t.Helper()
	return sharedJourneyHTTP(t, origin, "POST", "/ingest/v1/events", "", []byte(body), map[string]string{"X-Porcupine-Key": token}, want)
}

func sharedJourneyReceipt(t *testing.T, result map[string]any) string {
	t.Helper()
	receipts, ok := result["receipts"].([]any)
	if !ok || len(receipts) == 0 {
		t.Fatalf("missing canonical receipt: %v", result)
	}
	return receipts[0].(map[string]any)["event_id"].(string)
}

func sharedJourneyCount(t *testing.T, s *Service, project string) int {
	t.Helper()
	var count int
	if err := s.db.QueryRow(`SELECT count(*) FROM events WHERE project_id=$1`, project).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func TestSharedWorkerHTTPAndMCPOwnerJourney(t *testing.T) {
	t.Parallel()
	a, b := sharedJourneyPair(t, nil)
	tenant := sharedJourneyHTTP(t, a.host.URL, "POST", "/api/v1/tenants", adminToken, []byte(`{"name":"Shared customer"}`), nil, 201)["tenant"].(map[string]any)["id"].(string)
	sharedJourneyHTTP(t, b.host.URL, "GET", "/api/v1/tenants/"+tenant, adminToken, nil, nil, 200)
	projectA, ingestA := sharedJourneyProject(t, a.host.URL, tenant, "Owner A")
	projectB, ingestB := sharedJourneyProject(t, b.host.URL, tenant, "Owner B")
	for project, worker := range map[string]string{projectA: "journey-a", projectB: "journey-b"} {
		var actual string
		if err := a.service.control.QueryRow(`SELECT worker_id FROM shared_project_owners WHERE project_id=$1`, project).Scan(&actual); err != nil || actual != worker {
			t.Fatalf("project owner changed: got=%q want=%q error=%v", actual, worker, err)
		}
	}
	body := `{"type":"track","message_id":"same-message","name":"page_viewed","actor_id":"visitor","properties":{"private":"event-payload-stays-local"}}`
	first := sharedJourneyNative(t, b.host.URL, ingestA, body, 200)
	retry := sharedJourneyNative(t, a.host.URL, ingestA, body, 200)
	if first["accepted"] != float64(1) || retry["duplicates"] != float64(1) || sharedJourneyReceipt(t, first) != sharedJourneyReceipt(t, retry) {
		t.Fatal("cross-edge retry lost stable receipt", first, retry)
	}
	other := sharedJourneyNative(t, a.host.URL, ingestB, body, 200)
	if sharedJourneyReceipt(t, first) == sharedJourneyReceipt(t, other) {
		t.Fatal("independent owner volumes reused a global event ID")
	}
	if sharedJourneyCount(t, a.service, projectA) != 1 || sharedJourneyCount(t, a.service, projectB) != 0 || sharedJourneyCount(t, b.service, projectA) != 0 || sharedJourneyCount(t, b.service, projectB) != 1 {
		t.Fatal("forwarding copied events outside their durable owner")
	}
	keyID, reader := sharedJourneyKey(t, a.host.URL, projectA, []string{"events:read", "management:read"})
	rows := sharedJourneyHTTP(t, b.host.URL, "GET", "/api/v1/projects/"+projectA+"/events", reader, nil, nil, 200)["events"].([]any)
	if len(rows) != 1 {
		t.Fatal("owner-bound HTTP event listing omitted accepted events", rows)
	}
	sharedJourneyHTTP(t, a.host.URL, "GET", "/api/v1/projects/"+projectB+"/events", reader, nil, nil, 403)
	ctx, agent := mcpTestSession(t, b.host.URL, reader, "")
	mcpTestTool(t, ctx, agent, "porcupine_docs", mcpDocsInput{OperationID: "list-project-events"}, false)
	listed := mcpTestCall(t, ctx, agent, mcpCallInput{OperationID: "list-project-events", Path: map[string]string{"id": projectA}}, 200)
	if len(listed["events"].([]any)) != 1 {
		t.Fatal("MCP call did not reach the durable project owner", listed)
	}
	mcpTestCall(t, ctx, agent, mcpCallInput{OperationID: "list-project-events", Path: map[string]string{"id": projectB}}, 403)
	sharedJourneyHTTP(t, a.host.URL, "DELETE", "/api/v1/access-keys/"+keyID, adminToken, nil, nil, 204)
	sharedJourneyHTTP(t, b.host.URL, "GET", "/api/v1/projects/"+projectA+"/events", reader, nil, nil, 401)
	if _, err := agent.ListTools(ctx, nil); err == nil || !strings.Contains(err.Error(), "Unauthorized") {
		t.Fatal("shared revocation retained an existing remote MCP connection", err)
	}
	budget := []byte(`{"ingestion_monthly_events":2,"hot_query_max_concurrent":1,"hot_query_timeout_ms":1000,"hot_query_max_rows":100,"hot_query_max_result_bytes":65536}`)
	sharedJourneyHTTP(t, b.host.URL, "PUT", "/api/v1/tenants/"+tenant+"/budgets", adminToken, budget, nil, 200)
	for _, node := range []*sharedJourneyNode{a, b, a, b} {
		if err := node.service.syncSharedIngestion(t.Context()); err != nil {
			t.Fatal("shared cumulative usage publication", err)
		}
	}
	usage := sharedJourneyHTTP(t, a.host.URL, "GET", "/api/v1/tenants/"+tenant+"/usage", adminToken, nil, nil, 200)
	if usage["accepted_events"] != "2" || usage["remaining_events"] != "0" {
		t.Fatal("shared usage was lost or counted twice", usage)
	}
	denied := sharedJourneyNative(t, b.host.URL, ingestA, `{"type":"track","message_id":"over-budget","name":"rejected","actor_id":"visitor"}`, 429)
	if denied["code"] != "ingestion_budget_exceeded" || denied["receipts"] != nil {
		t.Fatal("refreshed quota accepted or fabricated an event", denied)
	}
	if replay := sharedJourneyNative(t, b.host.URL, ingestA, body, 200); sharedJourneyReceipt(t, replay) != sharedJourneyReceipt(t, first) {
		t.Fatal("exhausted budget invalidated an existing receipt", replay)
	}
	for _, table := range []string{"events", "event_receipts", "event_heads", "identity_profiles", "identity_links", "mutation_outbox", "import_uploads", "webhook_deliveries"} {
		var absent bool
		if err := a.service.control.QueryRow(`SELECT to_regclass($1) IS NULL`, table).Scan(&absent); err != nil || !absent {
			t.Fatal("event payload state entered the operational PostgreSQL database", table, err)
		}
	}
}

func sharedJourneyGzip(t *testing.T, raw string) []byte {
	t.Helper()
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write([]byte(raw)); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return compressed.Bytes()
}

func TestSharedWorkerAdaptersGzipAndCookielessForwarding(t *testing.T) {
	t.Parallel()
	a, b := sharedJourneyPair(t, nil)
	project, ingest := sharedJourneyProject(t, a.host.URL, defaultTenantID, "Adapter owner")
	native := `{"type":"track","message_id":"native-gzip","name":"backend_saved","actor_id":"visitor","timestamp":"2020-01-01T23:15:00.123456789-02:00"}`
	first := sharedJourneyHTTP(t, b.host.URL, "POST", "/ingest/v1/events", "", sharedJourneyGzip(t, native), map[string]string{"X-Porcupine-Key": ingest, "Content-Encoding": "gzip"}, 200)
	if replay := sharedJourneyNative(t, a.host.URL, ingest, native, 200); sharedJourneyReceipt(t, first) != sharedJourneyReceipt(t, replay) || replay["duplicates"] != float64(1) {
		t.Fatal("decoded native forwarding changed retry identity", first, replay)
	}
	posthog := `{"api_key":"` + ingest + `","uuid":"posthog-gzip","event":"$pageview","distinct_id":"browser","properties":{"$current_url":"https://example.test/pricing"}}`
	ph := sharedJourneyHTTP(t, b.host.URL, "POST", "/e/?compression=gzip-js", "", sharedJourneyGzip(t, posthog), map[string]string{"Content-Type": "text/plain"}, 200)
	if ph["accepted"] != float64(1) {
		t.Fatal("PostHog gzip forwarding did not accept event", ph)
	}
	if again := sharedJourneyHTTP(t, a.host.URL, "POST", "/capture/", "", []byte(posthog), nil, 200); again["duplicates"] != float64(1) {
		t.Fatal("PostHog retry crossed receipt namespaces", again)
	}
	segment := []byte(`{"batch":[{"type":"track","messageId":"segment-track","event":"Checkout Started","userId":"customer"},{"type":"page","messageId":"segment-page","name":"Pricing","anonymousId":"browser","properties":{"url":"https://example.test/pricing"}}]}`)
	seg := sharedJourneyHTTP(t, b.host.URL, "POST", "/v1/batch", "", segment, segmentHeaders(ingest), 200)
	again := sharedJourneyHTTP(t, a.host.URL, "POST", "/v1/batch", "", segment, segmentHeaders(ingest), 200)
	if seg["accepted"] != float64(2) || again["duplicates"] != float64(2) || sharedJourneyReceipt(t, seg) != sharedJourneyReceipt(t, again) {
		t.Fatal("Segment forwarding did not preserve stable receipts", seg, again)
	}
	for i, node := range []*sharedJourneyNode{b, a} {
		body := fmt.Sprintf(`{"api_key":%q,"uuid":"cookieless-%d","event":"$pageview","distinct_id":"$posthog_cookieless"}`, ingest, i)
		sharedJourneyHTTP(t, node.host.URL, "POST", "/capture/", "", []byte(body), map[string]string{"User-Agent": "shared-browser-fixture"}, 200)
	}
	var left, right, occurred string
	if err := a.service.db.QueryRow(`SELECT distinct_id FROM events WHERE uuid='cookieless-0'`).Scan(&left); err != nil {
		t.Fatal(err)
	}
	if err := a.service.db.QueryRow(`SELECT distinct_id FROM events WHERE uuid='cookieless-1'`).Scan(&right); err != nil || left != right || left == ingestfmt.CookielessSentinel {
		t.Fatal("edge hop changed the daily visitor identity", left, right, err)
	}
	if err := a.service.db.QueryRow(`SELECT timestamp FROM events WHERE ingestion_metadata LIKE '%native-gzip%'`).Scan(&occurred); err != nil || occurred != "2020-01-02T01:15:00.123456789Z" {
		t.Fatal("forwarding changed occurrence time", occurred, err)
	}
	if sharedJourneyCount(t, a.service, project) != 6 || sharedJourneyCount(t, b.service, project) != 0 {
		t.Fatal("adapter forwarding materialized payloads on the edge")
	}
}

func TestSharedWorkerIdentifyRetryAndSuspension(t *testing.T) {
	t.Parallel()
	a, b := sharedJourneyPair(t, nil)
	project, ingest := sharedJourneyProject(t, a.host.URL, defaultTenantID, "Identity owner")
	body := `[{"type":"pageview","message_id":"anonymous-page","actor_id":"anonymous","context":{"page":{"url":"https://example.test"}}},{"type":"identify","message_id":"identify-once","user_id":"known","anonymous_id":"anonymous","set":{"plan":"pro"},"set_once":{"origin":"docs"}}]`
	first := sharedJourneyNative(t, b.host.URL, ingest, body, 200)
	retry := sharedJourneyNative(t, a.host.URL, ingest, body, 200)
	if first["accepted"] != float64(2) || retry["duplicates"] != float64(2) || sharedJourneyReceipt(t, first) != sharedJourneyReceipt(t, retry) {
		t.Fatal("identity retry did not use the owner's receipt ledger", first, retry)
	}
	sharedJourneyNative(t, b.host.URL, ingest, `{"type":"identify","message_id":"identify-twice","user_id":"known","set":{"plan":"team"},"set_once":{"origin":"wrong"}}`, 200)
	var traits, identified string
	if err := a.service.db.QueryRow(`SELECT traits FROM identity_profiles WHERE project_id=$1 AND identified_id='known'`, project).Scan(&traits); err != nil {
		t.Fatal(err)
	}
	var properties map[string]any
	if err := json.Unmarshal([]byte(traits), &properties); err != nil || properties["plan"] != "team" || properties["origin"] != "docs" {
		t.Fatal("owner serialization lost identify set/set_once semantics", traits, err)
	}
	if err := a.service.db.QueryRow(`SELECT identified_id FROM identity_links WHERE project_id=$1 AND anonymous_id='anonymous'`, project).Scan(&identified); err != nil || identified != "known" {
		t.Fatal("cross-edge identify did not link the anonymous actor", identified, err)
	}
	sharedJourneyHTTP(t, b.host.URL, "PATCH", "/api/v1/tenants/default", adminToken, []byte(`{"ingestion_allowed":false}`), nil, 200)
	for _, node := range []*sharedJourneyNode{a, b, a} {
		if err := node.service.syncSharedIngestion(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	blocked := sharedJourneyNative(t, b.host.URL, ingest, `{"type":"track","message_id":"blocked","name":"blocked","actor_id":"known"}`, 429)
	if blocked["code"] != "ingestion_suspended" || blocked["receipts"] != nil {
		t.Fatal("suspension did not block new acceptance", blocked)
	}
	if replay := sharedJourneyNative(t, b.host.URL, ingest, body, 200); replay["duplicates"] != float64(2) || sharedJourneyReceipt(t, replay) != sharedJourneyReceipt(t, first) {
		t.Fatal("suspension broke existing identity/event retries", replay)
	}
	ph := `{"api_key":"` + ingest + `","uuid":"blocked-ph","event":"blocked","distinct_id":"known"}`
	if response := sharedJourneyHTTP(t, b.host.URL, "POST", "/capture/", "", []byte(ph), nil, 200); response["code"] != "ingestion_suspended" || response["quota_limited"] == nil || response["accepted"] != float64(0) {
		t.Fatal("PostHog suspension was not an explicit terminal quota response", response)
	}
	if sharedJourneyCount(t, a.service, project) != 3 || sharedJourneyCount(t, b.service, project) != 0 || sharedUsageCount(t, a.service) != 3 {
		t.Fatal("suspension/retries changed owner data or shared usage")
	}
}

func TestSharedWorkerNativeCLIInterruptedImportResume(t *testing.T) {
	t.Parallel()
	var cut atomic.Bool
	cut.Store(true)
	committed := make(chan struct{})
	a, b := sharedJourneyPair(t, func(index int, next http.Handler) http.Handler {
		if index != 1 {
			return next
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == "PUT" && strings.Contains(r.URL.Path, "/imports/") && cut.CompareAndSwap(true, false) {
				record := httptest.NewRecorder()
				next.ServeHTTP(record, r)
				if record.Code != 200 {
					t.Errorf("initial forwarded chunk: %d %s", record.Code, record.Body.String())
				}
				close(committed)
				<-r.Context().Done()
				return
			}
			next.ServeHTTP(w, r)
		})
	})
	project, _ := sharedJourneyProject(t, a.host.URL, defaultTenantID, "Historical owner")
	_, token := sharedJourneyKey(t, b.host.URL, project, []string{"imports:write", "events:write", "events:read"})
	binary := filepath.Join(t.TempDir(), "porcupine")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", binary, "./cmd/porcupine")
	build.Dir, build.Env = "../../cli", append(os.Environ(), "CGO_ENABLED=0")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatal("build standalone CLI", string(output), err)
	}
	var data strings.Builder
	for i := range 300 {
		fmt.Fprintf(&data, `{"type":"track","message_id":"historical-%d","name":"historical_event","actor_id":"visitor","timestamp":"2020-01-01T23:15:00.123456789-02:00","properties":{"padding":%q}}`+"\n", i, strings.Repeat("x", 1024))
	}
	filename := filepath.Join(t.TempDir(), "history.jsonl.gz")
	if err := os.WriteFile(filename, sharedJourneyGzip(t, data.String()), 0600); err != nil {
		t.Fatal(err)
	}
	command := func(extra ...string) *exec.Cmd {
		args := append([]string{"--url", b.host.URL, "import", "--project", project, "--format", "native", "--no-wait"}, extra...)
		args = append(args, filename)
		cmd := exec.CommandContext(t.Context(), binary, args...)
		cmd.Env = append(os.Environ(), "PORCUPINE_API_KEY="+token)
		return cmd
	}
	first := command()
	var initialLog bytes.Buffer
	first.Stderr = &initialLog
	if err := first.Start(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-committed:
	case <-time.After(15 * time.Second):
		first.Process.Kill()
		first.Wait()
		t.Fatalf("no durable forwarded checkpoint: %s", initialLog.String())
	}
	first.Process.Kill()
	first.Wait()
	var offset, size int64
	var jobID string
	if err := a.service.db.QueryRow(`SELECT u.id,f.offset_bytes,f.size_bytes FROM import_uploads u JOIN import_upload_files f ON f.job_id=u.id`).Scan(&jobID, &offset, &size); err != nil || offset <= 0 || offset >= size {
		t.Fatal("interruption did not leave one partial owner checkpoint", jobID, offset, size, err)
	}
	resume := command("--resume", jobID)
	var output, diagnostic bytes.Buffer
	resume.Stdout, resume.Stderr = &output, &diagnostic
	if err := resume.Run(); err != nil {
		t.Fatal("cross-edge CLI resume failed", err, diagnostic.String())
	}
	var job importer.ImportUploadJob
	if err := json.Unmarshal(output.Bytes(), &job); err != nil || job.State != "completed" || job.Counts.Accepted != 300 || job.Counts.Invalid != 0 || job.ID != jobID {
		t.Fatal("CLI did not complete the same historical owner job", output.String(), err)
	}
	var occurred string
	if err := a.service.db.QueryRow(`SELECT min(timestamp) FROM events WHERE project_id=$1`, project).Scan(&occurred); err != nil || occurred != "2020-01-02T01:15:00.123456789Z" {
		t.Fatal("historical import lost original date precision", occurred, err)
	}
	replay := command("--resume", jobID)
	if result, err := replay.CombinedOutput(); err != nil {
		t.Fatal("completed import retry failed", err, string(result))
	}
	if sharedJourneyCount(t, a.service, project) != 300 || sharedJourneyCount(t, b.service, project) != 0 {
		t.Fatal("resuming an owner-bound import duplicated or misplaced events")
	}
	for _, node := range []*sharedJourneyNode{a, b, a} {
		if err := node.service.syncSharedIngestion(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if count := sharedUsageCount(t, a.service); count != 300 {
		t.Fatal("import resume was billed more than once", count)
	}
}

type sharedJourneyProcessConfig struct {
	DatabaseURL string
	DBPath      string
	WorkerID    string
	ReadyFile   string
}

func TestSharedWorkerProcessHelper(t *testing.T) {
	t.Parallel()
	config := os.Getenv("PORCUPINE_SHARED_JOURNEY_HELPER")
	if config == "" {
		t.Skip("subprocess helper")
	}
	var input sharedJourneyProcessConfig
	if err := json.Unmarshal([]byte(config), &input); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	origin := "http://" + listener.Addr().String()
	s, err := Open(Config{DBPath: input.DBPath, BootstrapToken: adminToken,
		Shared: &SharedConfig{DatabaseURL: input.DatabaseURL, WorkerID: input.WorkerID, RouteURL: origin, RoutingSecret: []byte(sharedJourneySecret)}})
	if err != nil {
		listener.Close()
		t.Fatal("start shared subprocess", err)
	}
	defer s.Close()
	server := &http.Server{Handler: s.Handler(), ReadHeaderTimeout: 5 * time.Second}
	defer server.Close()
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	if err := os.WriteFile(input.ReadyFile, []byte(origin), 0600); err != nil {
		t.Fatal(err)
	}
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	defer signal.Stop(interrupt)
	select {
	case <-interrupt:
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		server.Shutdown(ctx)
	case err := <-done:
		t.Fatal("shared subprocess HTTP listener stopped", err)
	}
}

type sharedJourneyProcess struct {
	origin string
	cmd    *exec.Cmd
	done   chan struct{}
	stop   atomic.Bool
}

func startSharedJourneyProcess(t *testing.T, config sharedJourneyProcessConfig) *sharedJourneyProcess {
	t.Helper()
	config.ReadyFile = filepath.Join(t.TempDir(), "ready")
	raw, _ := json.Marshal(config)
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(binary, "-test.run=^TestSharedWorkerProcessHelper$", "-test.timeout=60s")
	command.Env = append(os.Environ(), "PORCUPINE_SHARED_JOURNEY_HELPER="+string(raw))
	logfile, err := os.Create(filepath.Join(t.TempDir(), "worker.log"))
	if err != nil {
		t.Fatal(err)
	}
	command.Stdout, command.Stderr = logfile, logfile
	if err := command.Start(); err != nil {
		logfile.Close()
		t.Fatal(err)
	}
	worker := &sharedJourneyProcess{cmd: command, done: make(chan struct{})}
	go func() { command.Wait(); logfile.Close(); close(worker.done) }()
	t.Cleanup(func() { worker.stopProcess(t, false) })
	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	for {
		if ready, err := os.ReadFile(config.ReadyFile); err == nil && strings.HasPrefix(string(ready), "http://127.0.0.1:") {
			worker.origin = string(ready)
			sharedJourneyHTTP(t, worker.origin, "GET", "/readyz", "", nil, nil, 200)
			return worker
		}
		select {
		case <-worker.done:
			diagnostic, _ := os.ReadFile(logfile.Name())
			t.Fatal("shared subprocess exited before readiness", string(diagnostic))
		case <-deadline.C:
			t.Fatal("shared subprocess did not become ready")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func (worker *sharedJourneyProcess) stopProcess(t *testing.T, crash bool) {
	t.Helper()
	if !worker.stop.CompareAndSwap(false, true) {
		return
	}
	if crash {
		worker.cmd.Process.Kill()
	} else {
		worker.cmd.Process.Signal(os.Interrupt)
	}
	select {
	case <-worker.done:
	case <-time.After(5 * time.Second):
		worker.cmd.Process.Kill()
		<-worker.done
	}
}

func TestSharedWorkerTwoProcessCrashAndOwnerRestart(t *testing.T) {
	t.Parallel()
	dsn := sharedPostgresDSN(t)
	configA := sharedJourneyProcessConfig{DatabaseURL: dsn, DBPath: filepath.Join(t.TempDir(), "queue.db"), WorkerID: "process-a"}
	configB := sharedJourneyProcessConfig{DatabaseURL: dsn, DBPath: filepath.Join(t.TempDir(), "queue.db"), WorkerID: "process-b"}
	a, b := startSharedJourneyProcess(t, configA), startSharedJourneyProcess(t, configB)
	project, ingest := sharedJourneyProject(t, a.origin, defaultTenantID, "Persistent owner")
	body := `{"type":"track","message_id":"persistent-event","name":"queued_before_crash","actor_id":"visitor"}`
	first := sharedJourneyNative(t, b.origin, ingest, body, 200)
	a.stopProcess(t, true)
	sharedJourneyNative(t, b.origin, ingest, body, 503)
	a = startSharedJourneyProcess(t, configA)
	replay := sharedJourneyNative(t, b.origin, ingest, body, 200)
	if replay["duplicates"] != float64(1) || sharedJourneyReceipt(t, replay) != sharedJourneyReceipt(t, first) {
		t.Fatal("owner crash/restart lost the durable receipt", first, replay)
	}
	rows := sharedJourneyHTTP(t, b.origin, "GET", "/api/v1/projects/"+project+"/events", adminToken, nil, nil, 200)["events"].([]any)
	if len(rows) != 1 {
		t.Fatal("owner restart did not retain exactly one queued event", rows)
	}
	usage := sharedJourneyHTTP(t, a.origin, "GET", "/api/v1/tenants/default/usage", adminToken, nil, nil, 200)
	if usage["accepted_events"] != "1" {
		t.Fatal("restart did not publish the durable usage watermark", usage)
	}
	ctx, agent := mcpTestSession(t, b.origin, adminToken, "")
	listed := mcpTestCall(t, ctx, agent, mcpCallInput{OperationID: "list-project-events", Path: map[string]string{"id": project}}, 200)
	if len(listed["events"].([]any)) != 1 {
		t.Fatal("MCP could not route to the restarted owner", listed)
	}
}

func sharedNativeJourneyPair(t *testing.T, fixture duckLakeS3Fixture) (*sharedJourneyNode, *sharedJourneyNode) {
	t.Helper()
	control, catalogs := sharedPostgresDSN(t), sharedPostgresDSN(t)
	var nodes []*sharedJourneyNode
	for _, worker := range []string{"native-journey-a", "native-journey-b"} {
		host := httptest.NewUnstartedServer(http.NotFoundHandler())
		directory := t.TempDir()
		config := Config{DBPath: filepath.Join(directory, "queue.db"), BootstrapToken: adminToken,
			Shared:   &SharedConfig{DatabaseURL: control, WorkerID: worker, RouteURL: "http://" + host.Listener.Addr().String(), RoutingSecret: []byte(sharedJourneySecret)},
			DuckLake: &DuckLakeConfig{Directory: filepath.Join(directory, "lake"), PostgresURL: catalogs, S3: &fixture.storage, MemoryLimitMB: 128, Threads: 1, MaxOpenProjects: 2, DisableCleanup: true, MaintenanceInterval: 24 * time.Hour}}
		if fixture.storage.CACertFile == "" {
			config.Exports = &ExportConfig{S3: exportstore.Config{Endpoint: fixture.storage.Endpoint, Region: fixture.storage.Region, Bucket: fixture.storage.Bucket,
				Prefix: fixture.storage.Prefix + "/exports", AccessKeyID: fixture.storage.AccessKeyID, SecretAccessKey: fixture.storage.SecretAccessKey, UsePathStyle: true},
				MaxExecutionSeconds: 10, BandwidthBytesPerSecond: 10 << 20}
		}
		s, err := Open(config)
		if err != nil {
			host.Close()
			t.Fatal("open shared native worker", worker, err)
		}
		// Keep delivery explicit so each cross-node read follows the exact
		// acceptance/projection boundary exercised below.
		s.deliveryCancel()
		<-s.deliveryDone
		s.lakeMaintenanceCancel()
		<-s.lakeMaintenanceDone
		host.Config.Handler = s.Handler()
		host.Start()
		t.Cleanup(func() { host.Close(); s.Close() })
		nodes = append(nodes, &sharedJourneyNode{service: s, host: host})
	}
	return nodes[0], nodes[1]
}

func sharedJourneyQuery(t *testing.T, origin, project, credential, sql string, want int) map[string]any {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"sql": sql})
	return sharedJourneyHTTP(t, origin, "POST", "/api/v1/projects/"+project+"/query", credential, body, nil, want)
}

func TestDuckLakeS3SharedPostgresWorkerLifecycle(t *testing.T) {
	t.Parallel()
	fixture := nativeS3Fixture(t)
	a, b := sharedNativeJourneyPair(t, fixture)
	projectA, ingestA := sharedJourneyProject(t, a.host.URL, defaultTenantID, "Shared native owner A")
	projectB, ingestB := sharedJourneyProject(t, b.host.URL, defaultTenantID, "Shared native owner B")
	_, manager := sharedJourneyKey(t, b.host.URL, projectA, []string{"events:read", "events:edit", "events:delete"})
	_, foreign := sharedJourneyKey(t, a.host.URL, projectB, []string{"events:read"})
	page := `{"type":"track","message_id":"shared-page","name":"original","actor_id":"browser","timestamp":"2020-01-01T12:00:00Z","context":{"page":{"url":"https://example.test/pricing"}},"properties":{"amount":1}}`
	first := sharedJourneyNative(t, b.host.URL, ingestA, page, 200)
	receipt := first["receipts"].([]any)[0].(map[string]any)
	uuid, eventID := receipt["uuid"].(string), receipt["event_id"].(string)
	sharedJourneyNative(t, a.host.URL, ingestB, `{"type":"track","message_id":"other-project","actor_id":"other","name":"unrelated"}`, 200)
	duckLakeJourneyDrain(t, a.service)
	duckLakeJourneyDrain(t, b.service)
	querySQL := "SELECT event_id,event,person_id,person_properties,properties FROM events"
	rows := sharedJourneyQuery(t, b.host.URL, projectA, manager, querySQL, 200)["result"].(map[string]any)["data"].([]any)
	if len(rows) != 1 || rows[0].(map[string]any)["event_id"] != eventID || rows[0].(map[string]any)["person_id"] != "browser" {
		t.Fatal("nonowner native engine did not read the owner's projected S3 event", rows)
	}
	sharedJourneyQuery(t, b.host.URL, projectA, foreign, "SELECT count(*) FROM events", 403)
	sharedJourneyNative(t, b.host.URL, ingestA, `{"type":"identify","message_id":"shared-identify","user_id":"known","anonymous_id":"browser","set":{"plan":"team"}}`, 200)
	duckLakeJourneyDrain(t, a.service)
	rows = sharedJourneyQuery(t, b.host.URL, projectA, manager, querySQL, 200)["result"].(map[string]any)["data"].([]any)
	if len(rows) != 1 || rows[0].(map[string]any)["person_id"] != "known" || !strings.Contains(rows[0].(map[string]any)["person_properties"].(string), `"plan":"team"`) {
		t.Fatal("shared native query did not reflect the owner's identify projection", rows)
	}
	editPath := "/api/v1/projects/" + projectA + "/events/" + uuid
	edit := sharedJourneyHTTP(t, b.host.URL, "PATCH", editPath, manager, []byte(`{"expected_revision":"0","idempotency_key":"shared-edit","snapshot":{"event":"corrected","distinct_id":"browser","properties":{"amount":42},"timestamp":"2020-01-01T12:00:00Z"}}`), nil, 202)
	duckLakeJourneyDrain(t, a.service)
	rows = sharedJourneyQuery(t, b.host.URL, projectA, manager, querySQL, 200)["result"].(map[string]any)["data"].([]any)
	if len(rows) != 1 || rows[0].(map[string]any)["event_id"] != eventID || rows[0].(map[string]any)["event"] != "corrected" || !strings.Contains(rows[0].(map[string]any)["properties"].(string), `"amount":42`) {
		t.Fatal("owner-bound correction did not reach another worker's native query", rows)
	}
	if a.service.exports != nil {
		sharedNativeJourneyExport(t, a, b, projectA, manager, eventID)
	}
	deleted, _ := json.Marshal(map[string]any{"expected_revision": edit["revision"], "idempotency_key": "shared-delete-event"})
	sharedJourneyHTTP(t, b.host.URL, "DELETE", editPath, manager, deleted, nil, 202)
	duckLakeJourneyDrain(t, a.service)
	rows = sharedJourneyQuery(t, b.host.URL, projectA, manager, "SELECT count(*) AS total FROM events", 200)["result"].(map[string]any)["data"].([]any)
	if rows[0].(map[string]any)["total"] != "0" {
		t.Fatal("owner-bound deletion remained visible on another native engine", rows)
	}
	if replay := sharedJourneyNative(t, b.host.URL, ingestA, page, 200); replay["duplicates"] != float64(1) || sharedJourneyReceipt(t, replay) != eventID {
		t.Fatal("native deletion changed the permanent retry receipt", replay)
	}
	storeA, storeB := a.service.lake.(*duckLakeStore), b.service.lake.(*duckLakeStore)
	prefixA, prefixB := fixture.projectPrefix(t, storeA, projectA), fixture.projectPrefix(t, storeB, projectB)
	if len(fixture.keys(t, prefixA)) == 0 || len(fixture.keys(t, prefixB)) == 0 {
		t.Fatal("shared native events were not written into isolated S3 project prefixes")
	}
	var inlined int
	source, err := storeA.SourceID(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	schema := sharedNativeSchema(sharedNativeCatalogID(source, projectA))
	if err := storeA.SharedCatalog.QueryRowContext(t.Context(), "SELECT count(*) FROM "+schema+".ducklake_inlined_data_tables").Scan(&inlined); err != nil || inlined != 0 {
		t.Fatal("event rows were inlined into PostgreSQL rather than Parquet/S3", inlined, err)
	}
	for _, node := range []*sharedJourneyNode{a, b, a} {
		if err := node.service.syncSharedIngestion(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	budgets := []byte(`{"ingestion_monthly_events":null,"hot_query_max_concurrent":1,"hot_query_timeout_ms":5000,"hot_query_max_rows":1000,"hot_query_max_result_bytes":1048576}`)
	sharedJourneyHTTP(t, b.host.URL, "PUT", "/api/v1/tenants/default/budgets", adminToken, budgets, nil, 200)
	_, _, release, err := a.service.beginInteractiveQuery(t.Context(), projectA)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if limited := sharedJourneyQuery(t, b.host.URL, projectA, manager, "SELECT count(*) AS total FROM events", 429); limited["code"] != "tenant_query_concurrency_limit" {
		t.Fatal("another HTTP worker bypassed shared hot query admission", limited)
	}
	deleteResult := make(chan error, 1)
	go func() {
		r, _ := http.NewRequestWithContext(t.Context(), "DELETE", b.host.URL+"/api/v1/projects/"+projectA, nil)
		r.Header.Set("Authorization", "Bearer "+adminToken)
		client := &http.Client{Timeout: 15 * time.Second}
		response, err := client.Do(r)
		if err == nil {
			defer response.Body.Close()
			if response.StatusCode != 204 {
				raw, _ := io.ReadAll(response.Body)
				err = fmt.Errorf("project deletion: status=%d body=%s", response.StatusCode, raw)
			}
		}
		deleteResult <- err
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var fenced bool
		if err := a.service.control.QueryRow(`SELECT EXISTS(SELECT 1 FROM project_deletions WHERE project_id=$1 AND completed=0)`, projectA).Scan(&fenced); err != nil {
			t.Fatal(err)
		}
		if fenced {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("project deletion did not publish its shared fence")
		}
		time.Sleep(10 * time.Millisecond)
	}
	sharedJourneyQuery(t, b.host.URL, projectA, adminToken, "SELECT count(*) AS total FROM events", 404)
	release()
	if err := <-deleteResult; err != nil {
		t.Fatal(err)
	}
	for _, node := range []*sharedJourneyNode{a, b} {
		sharedJourneyHTTP(t, node.host.URL, "DELETE", "/api/v1/projects/"+projectA, adminToken, nil, nil, 204)
	}
	if len(fixture.keys(t, prefixA)) != 0 || len(fixture.keys(t, prefixB)) == 0 {
		t.Fatal("project deletion failed its S3 cleanup or removed another project's data")
	}
	rows = sharedJourneyQuery(t, a.host.URL, projectB, adminToken, "SELECT event FROM events", 200)["result"].(map[string]any)["data"].([]any)
	if len(rows) != 1 || rows[0].(map[string]any)["event"] != "unrelated" {
		t.Fatal("shared project cleanup damaged the other owner's native data", rows)
	}
	if usage := sharedJourneyHTTP(t, b.host.URL, "GET", "/api/v1/tenants/default/usage", adminToken, nil, nil, 200); usage["accepted_events"] != "3" {
		t.Fatal("project or event deletion refunded shared consumption", usage)
	}
}

func sharedNativeJourneyExport(t *testing.T, owner, edge *sharedJourneyNode, project, credential, eventID string) {
	t.Helper()
	base := "/api/v1/projects/" + project + "/exports"
	created := sharedJourneyHTTP(t, edge.host.URL, "POST", base, credential, []byte(`{"idempotency_key":"shared-native-export","sql":"SELECT event_id,event FROM events","format":"jsonl"}`), nil, 202)
	id := created["id"].(string)
	deadline := time.Now().Add(15 * time.Second)
	for {
		job := sharedJourneyHTTP(t, edge.host.URL, "GET", base+"/"+id, credential, nil, nil, 200)
		state := job["state"].(string)
		if state == "succeeded" {
			if job["rows"] != float64(1) {
				t.Fatal("shared export returned wrong row count", job)
			}
			break
		}
		if state != "queued" && state != "running" || time.Now().After(deadline) {
			t.Fatal("owner-bound export failed or stalled", job)
		}
		time.Sleep(20 * time.Millisecond)
	}
	files := sharedJourneyHTTP(t, edge.host.URL, "GET", base+"/"+id+"/files", credential, nil, nil, 200)["files"].([]any)
	if len(files) != 1 {
		t.Fatal("shared export did not publish one downloadable S3 file", files)
	}
	client := &http.Client{Timeout: 10 * time.Second}
	response, err := client.Get(files[0].(map[string]any)["url"].(string))
	if err != nil {
		t.Fatal("download shared S3 export", err)
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatal("shared export download was not successful", response.StatusCode)
	}
	reader, err := gzip.NewReader(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	var row map[string]any
	if err := json.NewDecoder(reader).Decode(&row); err != nil || row["event_id"] != eventID || row["event"] != "corrected" {
		t.Fatal("shared export content differs from the native query", row, err)
	}
	var owned, misplaced int
	if err := owner.service.db.QueryRow(`SELECT count(*) FROM export_jobs WHERE id=$1`, id).Scan(&owned); err != nil {
		t.Fatal(err)
	}
	if err := edge.service.db.QueryRow(`SELECT count(*) FROM export_jobs WHERE id=$1`, id).Scan(&misplaced); err != nil || owned != 1 || misplaced != 0 {
		t.Fatal("export job escaped its durable project owner", owned, misplaced, err)
	}
}
