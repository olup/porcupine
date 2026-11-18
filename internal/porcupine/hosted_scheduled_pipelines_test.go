package porcupine

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/acceptance"
	"github.com/olup/porcupine/internal/auth"
	eventapi "github.com/olup/porcupine/internal/event"
	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/output"
)

func TestDuckLakeS3SharedPostgresHostedScheduledPipeline(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	ctx := t.Context()
	fixture := nativeS3Fixture(t)
	dsn := sharedPostgresDSN(t)
	destination := &pipelineRecoveryS3{markers: map[string][]byte{}}
	remote := httptest.NewServer(destination)
	t.Cleanup(remote.Close)
	openWorker := func() *Service {
		cfg := Config{
			Profile: ProfileHosted, AcceptanceMode: AcceptanceModeBuffered, BootstrapToken: adminToken,
			Shared: &SharedConfig{DatabaseURL: dsn},
			DuckLake: &lakeapi.DuckLakeConfig{
				PostgresURL: dsn, S3: &fixture.storage, MemoryLimitMB: 128, Threads: 1,
				MaxOpenProjects: 2, CleanupGrace: 10 * 24 * time.Hour, MaintenanceInterval: time.Hour,
				RestoreWindow: 24 * time.Hour, RestoreMaxReaderTime: time.Hour, RestoreSafetyMargin: time.Hour,
			},
			Pipelines: &output.PipelineConfig{Key: bytes.Repeat([]byte{2}, 32), AllowedS3Endpoints: []string{remote.URL},
				ExportLimits: output.ExportConfig{MaxExecutionSeconds: 45}},
			OAuth: &OAuthConfig{Issuer: "http://127.0.0.1:42999", AllowLegacyKeys: true},
		}
		s, err := openHostedProfile(cfg, acceptance.DefaultBufferConfig())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := s.Close(); err != nil {
				t.Error(err)
			}
		})
		// Step workers explicitly so the commit failure and takeover are ordered.
		s.pipelines.Stop()
		return s
	}
	a := openWorker()
	b := openWorker()
	scope, _ := project(t, a)
	other, _ := project(t, a)
	host := httptest.NewServer(a.Handler())
	t.Cleanup(host.Close)
	status := sharedJourneyHTTP(t, host.URL, "GET", "/api/v1/status", adminToken, nil, nil, 200)
	if status["configuration"].(map[string]any)["s3_query_pipelines"] != true {
		t.Fatal("guarded hosted worker did not advertise scheduled S3")
	}
	accountBody, _ := json.Marshal(TenantServiceAccountSpec{Name: "scheduled creator", Projects: []string{scope}, Permissions: []string{"pipelines:write", "events:read"}})
	account := sharedJourneyHTTP(t, host.URL, "POST", "/api/v1/tenants/default/service-accounts", adminToken, accountBody, nil, 201)
	keyBody, _ := json.Marshal(ServiceKeySpec{Name: "scheduled key", Audience: "api", Projects: []string{scope}, Permissions: []string{"pipelines:write", "events:read"}})
	credential := sharedJourneyHTTP(t, host.URL, "POST", "/api/v1/tenants/default/service-accounts/"+account["client_id"].(string)+"/api-keys", adminToken, keyBody, nil, 201)
	creator := auth.Principal{ID: credential["id"].(string)}
	start := time.Now().UTC().Truncate(24 * time.Hour).Add(-24 * time.Hour)
	flush := func(s *Service, project, name string, at time.Time) {
		t.Helper()
		e := eventapi.Event{ProjectID: project, UUID: eventapi.NewID(), Event: name, DistinctID: "person", Properties: json.RawMessage(`{}`),
			Context: json.RawMessage(`{}`), Timestamp: time.Now().UTC().Format(time.RFC3339Nano), ReceivedAt: at.Format(time.RFC3339Nano)}
		err := s.hosted.flusher.FlushLive(ctx, acceptance.FlushBatch{ID: eventapi.NewID(), ProjectID: project,
			Events: []acceptance.BufferedEvent{{Input: acceptance.Input{Event: e}, AdmittedAt: at}}})
		if err != nil {
			t.Fatal(err)
		}
	}
	flush(a, scope, "inside", start)
	flush(a, scope, "exclusive end", start.Add(24*time.Hour))
	flush(b, other, "other project", start)
	request := output.PipelineRequest{Name: "Daily report", Kind: "s3", S3: &output.S3PipelineSpec{
		SQL:     "SELECT event FROM events WHERE received_at_ms >= CAST($window_start AS BIGINT) AND received_at_ms < CAST($window_end AS BIGINT) ORDER BY event",
		StartAt: start.Format(time.RFC3339), IntervalSeconds: 86400,
		Destination: output.S3PipelineDestination{Bucket: "reports", Region: "us-east-1", Endpoint: remote.URL}},
		Credentials: output.PipelineCredentials{AccessKeyID: "key-before", SecretAccessKey: "secret-before"}}
	p, err := a.pipelines.Define(ctx, scope, request, creator)
	if err != nil {
		t.Fatal(err)
	}
	// A live flush after schedule creation must never interpret a SQL rule as a webhook rule.
	flush(b, scope, "inside two", start.Add(time.Second))
	if _, err = b.pipelines.Get(ctx, other, p.ID); err == nil {
		t.Fatal("other project retrieved schedule")
	}
	if _, err = a.control.ExecContext(ctx, `
CREATE FUNCTION test_schedule_commit_failure() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'test failure after immutable marker'; END; $$;
CREATE TRIGGER test_schedule_commit_failure BEFORE UPDATE ON hosted_s3_pipeline_runs
FOR EACH ROW WHEN (new.state='succeeded') EXECUTE FUNCTION test_schedule_commit_failure();`); err != nil {
		t.Fatal(err)
	}
	if worked, err := a.pipelines.RunS3(ctx); err != nil || !worked {
		t.Fatalf("first worker advanced=%v err=%v", worked, err)
	}
	runs, err := b.pipelines.Runs(ctx, scope, p.ID, 10, 0)
	if err != nil || len(runs.Runs) != 1 || runs.Runs[0].State != "queued" {
		t.Fatalf("queued checkpoint: %+v %v", runs, err)
	}
	id := runs.Runs[0].ID
	var checkpoint string
	if err = a.control.QueryRowContext(ctx, `SELECT manifest FROM hosted_s3_pipeline_runs WHERE id=$1`, id).Scan(&checkpoint); err != nil || checkpoint == "" {
		t.Fatalf("durable manifest checkpoint unavailable: %v", err)
	}
	// A late flush into the same receipt window is outside the retained cut.
	flush(b, scope, "late flush", start.Add(2*time.Second))
	if _, err := a.control.ExecContext(ctx, `DROP TRIGGER test_schedule_commit_failure ON hosted_s3_pipeline_runs;
UPDATE hosted_s3_pipeline_runs SET state='running',lease_until=clock_timestamp()-interval '1 second';`); err != nil {
		t.Fatal(err)
	}
	if _, err := a.pipelines.SetState(ctx, scope, p.ID, "paused"); err != nil {
		t.Fatal(err)
	}
	if worked, err := b.pipelines.RunS3(ctx); err != nil || worked {
		t.Fatalf("paused worker advanced=%v err=%v", worked, err)
	}
	if _, err := b.pipelines.Replace(ctx, scope, p.ID, output.PipelineChange{Credentials: output.PipelineCredentials{AccessKeyID: "key-after", SecretAccessKey: "secret-after"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := a.pipelines.SetState(ctx, scope, p.ID, "active"); err != nil {
		t.Fatal(err)
	}
	if worked, err := b.pipelines.RunS3(ctx); err != nil || !worked {
		t.Fatalf("cold worker advanced=%v err=%v", worked, err)
	}
	run, err := a.pipelines.Run(ctx, scope, p.ID, id)
	if err != nil || run.State != "succeeded" || run.Rows != 2 || run.Bytes <= 0 || run.Attempts != 2 {
		t.Fatalf("wrong retained result: %+v %v", run, err)
	}
	var usedBytes, activities, reserved int64
	if err = a.control.QueryRowContext(ctx, `
SELECT (SELECT COALESCE(sum(pipeline_bytes),0) FROM usage_rollups WHERE project_id=$1),
(SELECT count(*) FROM hosted_project_activity WHERE project_id=$1),
(SELECT used_artifacts FROM pipeline_capacity WHERE scope_kind='project' AND scope_id=$1)`, scope).
		Scan(&usedBytes, &activities, &reserved); err != nil || usedBytes != run.Bytes || activities != 0 || reserved != 0 {
		t.Fatalf("usage=%d activities=%d reserved=%d err=%v", usedBytes, activities, reserved, err)
	}
	destination.mu.Lock()
	markerPuts := destination.markerPuts
	destination.mu.Unlock()
	if markerPuts != 1 {
		t.Fatal("takeover rewrote the immutable manifest")
	}
	destination.data.mu.Lock()
	var compressed []byte
	for _, object := range destination.data.objects {
		compressed = object
	}
	objectCount := len(destination.data.objects)
	destination.data.mu.Unlock()
	decoded, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil || objectCount != 1 {
		t.Fatalf("destination objects=%d, gzip error=%v", objectCount, err)
	}
	body, err := io.ReadAll(decoded)
	decoded.Close()
	if err != nil || string(body) != `{"event":"inside"}`+"\n"+`{"event":"inside two"}`+"\n" {
		t.Fatalf("unexpected published rows %q: %v", body, err)
	}

}
