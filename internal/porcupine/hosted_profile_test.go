package porcupine

import (
	"database/sql"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	ingestfmt "github.com/olup/porcupine/internal/ingest"
	lakeapi "github.com/olup/porcupine/internal/lake"
)

func TestDuckLakeS3SharedPostgresHostedProfileProcesses(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	fixture := nativeS3Fixture(t)
	dsn := sharedPostgresDSN(t)
	lake := lakeapi.DuckLakeConfig{
		PostgresURL: dsn, S3: &fixture.storage, MemoryLimitMB: 128, Threads: 1,
		MaxOpenProjects: 2, CleanupGrace: 10 * 24 * time.Hour,
		RestoreWindow: 24 * time.Hour, RestoreMaxReaderTime: time.Hour, RestoreSafetyMargin: time.Hour,
	}
	base := Config{Profile: ProfileHosted, AcceptanceMode: AcceptanceModeBuffered, BootstrapToken: adminToken,
		Shared: &SharedConfig{DatabaseURL: dsn}, DuckLake: &lake}
	if _, err := Open(base); err == nil || !strings.Contains(err.Error(), "startup is unavailable") {
		t.Fatalf("public startup must stay gated, got %v", err)
	}
	launch := func() (*sharedJourneyProcess, string) {
		scratch := t.TempDir()
		local := lake
		local.Directory = scratch
		return startSharedJourneyProcess(t, sharedJourneyProcessConfig{DatabaseURL: dsn, HostedLake: &local}), scratch
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
	if status["status"] != "ok" || checks["sqlite"] != "not_configured" || configuration["owner_routing"] != false {
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
	if len(listed["events"].([]any)) != 3 {
		t.Fatalf("recent event discovery did not read the hosted catalog: %v", listed)
	}
	control, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer control.Close()
	var owners, workers, live int
	err = control.QueryRow("SELECT (SELECT count(*) FROM shared_project_owners),(SELECT count(*) FROM shared_workers),(SELECT coalesce(sum(live_events),0) FROM usage_rollups WHERE project_id=$1)", project).Scan(&owners, &workers, &live)
	if err != nil || owners != 0 || workers != 0 || live != 3 {
		t.Fatalf("shared control state owners=%d registrations=%d live=%d: %v", owners, workers, live, err)
	}
}
