package porcupine

import (
	"database/sql"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	ingestfmt "github.com/olup/porcupine/internal/ingest"
)

func TestDuckLakeS3SharedPostgresHostedProfileProcesses(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	fixture := nativeS3Fixture(t)
	dsn := sharedPostgresDSN(t)
	serverBinary := filepath.Join(t.TempDir(), "porcupine-server")
	buildServer := exec.CommandContext(t.Context(), "go", "build", "-o", serverBinary, "./cmd/porcupine")
	buildServer.Dir = "../.."
	buildServer.Env = append(os.Environ(), "CGO_ENABLED=1")
	if result, err := buildServer.CombinedOutput(); err != nil {
		t.Fatal("build server", err, string(result))
	}
	launch := func() (*sharedJourneyProcess, string) {
		t.Helper()
		scratch := t.TempDir()
		return startHostedProfileBinary(t, serverBinary, dsn, scratch, fixture), scratch
	}
	a, scratchA := launch()
	b, scratchB := launch()
	project, ingest := sharedJourneyProject(t, a.origin, defaultTenantID, "stateless project")
	other, _ := sharedJourneyProject(t, b.origin, defaultTenantID, "other stateless project")
	_, reader := sharedJourneyKey(t, a.origin, project, []string{"events:read"})
	_, restricted := sharedJourneyKey(t, b.origin, other, []string{"events:read"})

	status := sharedJourneyHTTP(t, b.origin, "GET", "/api/v1/status", adminToken, nil, nil, 200)
	checks := status["checks"].(map[string]any)
	configuration := status["configuration"].(map[string]any)
	if status["status"] != "ok" || checks["sqlite"] != "not_configured" || configuration["shared_control"] != true {
		t.Fatalf("incorrect hosted instance status: %v", status)
	}
	schema := sharedJourneyHTTP(t, b.origin, "GET", "/ingest/v1/schema", "", nil, nil, 200)
	if schema["x-porcupine-acknowledgment"].(map[string]any)["durability"] != "buffered" {
		t.Fatal("public schema did not discover buffered acknowledgment")
	}
	mcpCtx, session := mcpTestSession(t, b.origin, reader, "")
	contract := mcpTestCall(t, mcpCtx, session, mcpCallInput{OperationID: "get-event-input-contract", Path: map[string]string{"id": project}}, 200)
	if contract["live_acknowledgment"].(map[string]any)["durability"] != "buffered" {
		t.Fatal("MCP did not discover buffered acknowledgment")
	}
	binary := filepath.Join(t.TempDir(), "porcupine-cli")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", binary, "./cmd/porcupine")
	build.Dir, build.Env = "../../cli", append(os.Environ(), "CGO_ENABLED=0")
	if result, err := build.CombinedOutput(); err != nil {
		t.Fatal("build standalone CLI", err, string(result))
	}
	discover := exec.CommandContext(t.Context(), binary, "--url", b.origin, "get-event-input-contract", "--id", project)
	discover.Env = append(os.Environ(), "PORCUPINE_API_KEY="+reader)
	if result, err := discover.CombinedOutput(); err != nil || !strings.Contains(string(result), `"buffered"`) {
		t.Fatalf("standalone CLI discovery failed: %v %s", err, result)
	}
	trace := `{"type":"track","message_id":"both-workers","name":"buffered native","actor_id":"test","timestamp":"2020-01-01T12:00:00Z"}`
	for _, worker := range []*sharedJourneyProcess{a, b} {
		ack := sharedJourneyNative(t, worker.origin, ingest, trace, 200)
		receipt := ack["receipts"].([]any)[0].(map[string]any)
		_, duplicates := ack["duplicates"]
		if ack["durability"] != "buffered" || ack["received"] != float64(1) || duplicates || len(receipt) != 2 || receipt["message_id"] != "both-workers" || receipt["uuid"] == "" {
			t.Fatalf("invalid buffered acknowledgment: %v", ack)
		}
	}
	for i, worker := range []*sharedJourneyProcess{a, b} {
		payload, err := json.Marshal(map[string]any{"api_key": ingest, "event": "cookieless visit", "uuid": []string{"cookieless-a", "cookieless-b"}[i], "properties": map[string]any{"distinct_id": ingestfmt.CookielessSentinel}})
		if err != nil {
			t.Fatal(err)
		}
		ack := sharedJourneyHTTP(t, worker.origin, "POST", "/capture/", "", payload, nil, 200)
		if ack["durability"] != "buffered" {
			t.Fatal("PostHog endpoint did not use buffered admission")
		}
	}
	segment, err := json.Marshal(map[string]string{
		"writeKey": ingest, "messageId": "buffered-segment", "event": "segment visit", "userId": "visitor", "timestamp": "2020-01-02T12:00:00Z",
	})
	if err != nil {
		t.Fatal(err)
	}
	segmentAck := sharedJourneyHTTP(t, a.origin, "POST", "/v1/track", "", segment, nil, 200)
	if segmentAck["status"] != float64(1) || segmentAck["durability"] != "buffered" || segmentAck["received"] != float64(1) {
		t.Fatalf("Segment endpoint did not accept buffered input: %v", segmentAck)
	}
	segmentReceipt := segmentAck["receipts"].([]any)[0].(map[string]any)
	if len(segmentReceipt) != 2 || segmentReceipt["message_id"] != "buffered-segment" || segmentReceipt["uuid"] == "" {
		t.Fatalf("invalid Segment receipt: %v", segmentReceipt)
	}
	sharedJourneyQuery(t, b.origin, project, restricted, "select count(*) from events", 403)
	a.stopProcess(t, false)
	b.stopProcess(t, false)
	for _, root := range []string{scratchA, scratchB} {
		if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
			if err == nil && strings.Contains(entry.Name(), "sqlite") {
				t.Errorf("hosted worker wrote a SQLite file: %s", path)
			}
			return err
		}); err != nil {
			t.Fatal(err)
		}
		if err := os.RemoveAll(root); err != nil {
			t.Fatal(err)
		}
	}
	cold, _ := launch()
	observed := sharedJourneyQuery(t, cold.origin, project, reader, "select count(*) as visits, count(distinct distinct_id) as visitors from events where event='cookieless visit'", 200)
	rows := observed["result"].(map[string]any)["data"].([]any)
	if !reflect.DeepEqual(rows, []any{map[string]any{"visits": "2", "visitors": "1"}}) {
		t.Fatalf("cookieless visitors differed across workers: %v", observed)
	}
	observed = sharedJourneyQuery(t, cold.origin, project, reader, "select count(*) as visits from events where event='buffered native'", 200)
	if !reflect.DeepEqual(observed["result"].(map[string]any)["data"], []any{map[string]any{"visits": "1"}}) {
		t.Fatalf("buffered retry retained more than one event: %v", observed)
	}
	listed := sharedJourneyHTTP(t, cold.origin, "GET", "/api/v1/projects/"+project+"/events", reader, nil, nil, 200)
	if len(listed["events"].([]any)) != 4 {
		t.Fatalf("recent event discovery did not read the hosted catalog: %v", listed)
	}
	control, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer control.Close()
	var obsoleteSchema bool
	var live int
	err = control.QueryRow(`SELECT to_regclass('shared_project_owners') IS NOT NULL OR to_regclass('shared_workers') IS NOT NULL,
        (SELECT coalesce(sum(live_events),0) FROM usage_rollups WHERE project_id=$1)`, project).Scan(&obsoleteSchema, &live)
	if err != nil || obsoleteSchema || live != 4 {
		t.Fatalf("hosted control state has retired owner schema=%t live=%d: %v", obsoleteSchema, live, err)
	}
}

func startHostedProfileBinary(t *testing.T, binary, dsn, scratch string, fixture duckLakeS3Fixture, configure ...func(map[string]any)) *sharedJourneyProcess {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	config := map[string]any{
		"profile": "hosted", "listen": addr, "bootstrap_token": adminToken,
		"acceptance": map[string]string{"mode": "buffered"},
		"shared":     map[string]string{"control_database_url": dsn},
		"storage":    map[string]string{"ducklake": scratch},
		"ducklake": map[string]any{
			"memory_mb": 128, "threads": 1, "max_open_projects": 2,
			"cleanup_grace": "240h", "restore_window": "24h",
			"restore_max_reader_time": "1h", "restore_safety_margin": "1h",
			"s3": map[string]any{
				"bucket": fixture.storage.Bucket, "prefix": fixture.storage.Prefix, "region": fixture.storage.Region,
				"endpoint": fixture.storage.Endpoint, "access_key_id": fixture.storage.AccessKeyID,
				"secret_access_key": fixture.storage.SecretAccessKey, "path_style": true, "ca_cert_file": fixture.storage.CACertFile,
			},
		},
	}
	for _, apply := range configure {
		apply(config)
	}
	raw, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	configFile := filepath.Join(t.TempDir(), "porcupine.json")
	if err := os.WriteFile(configFile, raw, 0600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(binary, "--config", configFile)
	command.Dir = scratch
	command.Env = slices.DeleteFunc(os.Environ(), func(item string) bool {
		return strings.HasPrefix(item, "PORCUPINE_")
	})
	worker := launchSharedJourneyProcess(t, command)
	worker.origin = "http://" + addr
	client := &http.Client{Timeout: time.Second}
	deadline := time.NewTimer(30 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		if response, err := client.Get(worker.origin + "/readyz"); err == nil {
			response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return worker
			}
		}
		select {
		case <-worker.done:
			diagnostic, _ := os.ReadFile(worker.log)
			t.Fatal("hosted binary exited before readiness", worker.err, string(diagnostic))
		case <-deadline.C:
			diagnostic, _ := os.ReadFile(worker.log)
			t.Fatal("hosted binary did not become ready", string(diagnostic))
		case <-ticker.C:
		}
	}
}
