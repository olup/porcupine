package porcupine

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/delivery"
	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/output"
)

func TestSharedHostedLeaseRecoversAfterPostgresOutage(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	bin := os.Getenv("PORCUPINE_TEST_PITR_POSTGRES_BIN")
	if bin == "" {
		t.Skip("run with the isolated shared PostgreSQL and S3 fixtures")
	}
	fixture := nativeS3Fixture(t)
	pg := newPITRPostgres(t, bin)
	ctx := t.Context()
	if _, err := pg.admin().ExecContext(ctx, "CREATE DATABASE hosted"); err != nil {
		t.Fatal(err)
	}
	config := hostedJourneyConfig(t, pg.dsn("hosted"), fixture)
	config.OAuth = &OAuthConfig{Issuer: "https://porcupine-lease-outage.invalid", AllowLegacyKeys: true}
	config.Pipelines = &output.PipelineConfig{Key: bytes.Repeat([]byte{1}, 32)}
	worker, host, _ := hostedCleanupWorker(t, config)
	worker.pipelines.Stop()
	project, ingest := sharedJourneyProject(t, host.URL, defaultTenantID, "lease outage")
	permissions := []string{"pipelines:write", "events:read"}
	accountSpec, _ := json.Marshal(TenantServiceAccountSpec{Name: "outage webhook", Projects: []string{project}, Permissions: permissions})
	account := sharedJourneyHTTP(t, host.URL, "POST", "/api/v1/tenants/default/service-accounts", adminToken, accountSpec, nil, 201)
	keySpec, _ := json.Marshal(ServiceKeySpec{Name: "outage webhook key", Audience: "api", Projects: []string{project}, Permissions: permissions})
	creator := sharedJourneyHTTP(t, host.URL, "POST", "/api/v1/tenants/default/service-accounts/"+account["client_id"].(string)+"/api-keys", adminToken, keySpec, nil, 201)["token"].(string)
	spec, _ := json.Marshal(PipelineCreateBody{Name: "outage webhook", Kind: "webhook", Webhook: &output.WebhookPipelineSpec{
		Endpoint: "https://example.test/webhook", DeliveryMode: "event",
	}})
	sharedJourneyHTTP(t, host.URL, "POST", "/api/v1/projects/"+project+"/pipelines", creator, spec, nil, 201)

	control := worker.control
	var priorID string
	if err := control.QueryRowContext(ctx, "SELECT incarnation_id FROM hosted_worker_incarnations").Scan(&priorID); err != nil {
		t.Fatal(err)
	}
	f := worker.hosted.flusher
	key := lakeapi.ProjectWriteKey{SourceID: f.native.Config.SharedSourceID, ProjectID: project}
	session, err := f.coordinator.Acquire(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	// Model a native call that ignores cancellation until its transport returns.
	// Its physical advisory lock holds the real buffered flush until PostgreSQL stops.
	entered := make(chan context.Context, 1)
	resume := make(chan struct{})
	finished := make(chan error, 1)
	defer func() {
		close(resume)
		<-finished
	}()
	go func() {
		err := f.runActiveHostedProject(ctx, session, project, func(ctx context.Context) error {
			entered <- ctx
			<-resume
			return ctx.Err()
		})
		finished <- errors.Join(err, session.Release())
	}()
	var previousWork context.Context
	select {
	case previousWork = <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("old operation did not enter the native call")
	}

	track := `{"type":"track","message_id":"outage-track","name":"tracked","actor_id":"person"}`
	identify := `{"type":"identify","message_id":"outage-identify","user_id":"person","anonymous_id":"anon","set":{"plan":"team"}}`
	admit := func() []string {
		t.Helper()
		var ids []string
		for _, body := range []string{track, identify} {
			response := sharedJourneyNative(t, host.URL, ingest, body, 200)
			ids = append(ids, response["receipts"].([]any)[0].(map[string]any)["uuid"].(string))
		}
		return ids
	}
	ids := admit()
	snapshot := worker.hosted.buffer.Snapshot
	waitHostedLeaseCondition(t, "buffered flush waiting for the advisory lock", 10*time.Second, func(ctx context.Context) bool {
		var waiting bool
		err := control.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND NOT granted
AND database=(SELECT oid FROM pg_database WHERE datname=current_database()))`).Scan(&waiting)
		return err == nil && waiting
	})
	if got := snapshot().Events; got != len(ids) {
		t.Fatalf("buffer contains %d of %d acknowledged events before outage", got, len(ids))
	}

	pg.run("pg_ctl", "-D", pg.primary, "-m", "immediate", "-w", "stop")
	pg.running = ""
	defer func() {
		if pg.running == "" {
			pg.start(pg.primary)
		}
	}()
	outage := time.NewTimer(90 * time.Second)
	defer outage.Stop()
	sharedJourneyHTTP(t, host.URL, "GET", "/readyz", "", nil, nil, 503)
	sharedJourneyNative(t, host.URL, ingest, track, 503)
	select {
	case <-previousWork.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("old native work did not receive cancellation")
	}
	select {
	case <-outage.C:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	sharedJourneyHTTP(t, host.URL, "GET", "/healthz", "", nil, nil, 200)
	sharedJourneyHTTP(t, host.URL, "GET", "/readyz", "", nil, nil, 503)
	if got := snapshot().Events; got != len(ids) {
		t.Fatalf("buffer retained %d of %d acknowledged events", got, len(ids))
	}

	restoring := time.Now()
	pg.start(pg.primary)
	var newID string
	waitHostedLeaseCondition(t, "new worker incarnation and drained buffer", 40*time.Second, func(ctx context.Context) bool {
		err := control.QueryRowContext(ctx, `SELECT incarnation_id FROM hosted_worker_incarnations WHERE expires_at>clock_timestamp()`).Scan(&newID)
		return err == nil && newID != priorID && snapshot().Events == 0
	})
	t.Logf("90-second outage retained %d acknowledged events, drained %s after PostgreSQL restart began", len(ids), time.Since(restoring).Round(time.Millisecond))
	sharedJourneyHTTP(t, host.URL, "GET", "/readyz", "", nil, nil, 200)

	// Replay both stable IDs after the original buffered batch has flushed.
	if retried := admit(); !reflect.DeepEqual(retried, ids) {
		t.Fatalf("replay changed stable event IDs: got %v, want %v", retried, ids)
	}
	waitHostedLeaseCondition(t, "duplicate replay to drain", 15*time.Second, func(context.Context) bool {
		return snapshot().Events == 0
	})
	result := sharedJourneyQuery(t, host.URL, project, creator, "SELECT count(*) AS n FROM events", 200)
	rows := result["result"].(map[string]any)["data"]
	if !reflect.DeepEqual(rows, []any{map[string]any{"n": "1"}}) {
		t.Fatalf("expected one visible track event, got %v", rows)
	}
	native, release, err := f.native.LeaseProject(ctx, project)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	var version, identityEventID int64
	var traits string
	var identified bool
	err = native.DB.QueryRowContext(ctx, `SELECT version, traits, is_identified,
(SELECT id FROM lake.current_events WHERE uuid=?) FROM lake.identity_profiles WHERE identified_id='person'`, ids[1]).
		Scan(&version, &traits, &identified, &identityEventID)
	if err != nil || !identified || version != identityEventID || traits != `{"plan":"team"}` {
		t.Fatalf("identity replay changed its projection: version=%d event=%d traits=%s identified=%t, %v", version, identityEventID, traits, identified, err)
	}
	var usage, jobs int
	if err := control.QueryRowContext(ctx, `SELECT COALESCE(sum(live_events),0),
(SELECT count(*) FROM pipeline_effect_jobs WHERE project_id=$1 AND kind='delivery') FROM usage_rollups WHERE project_id=$1`, project).Scan(&usage, &jobs); err != nil || usage != 2 || jobs != 1 {
		t.Fatalf("usage or webhook publication duplicated or missing: usage=%d delivery_jobs=%d, %v", usage, jobs, err)
	}
	sends := 0
	*worker.pipelineClient = http.Client{Transport: hostedWebhookRoundTrip(func(r *http.Request) (*http.Response, error) {
		var payload delivery.WebhookPayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil || payload.Event == nil || payload.Event.UUID != ids[0] ||
			payload.DeliveryID == "" || payload.DeliveryID != r.Header.Get("Idempotency-Key") {
			t.Errorf("invalid webhook delivery: %v, %v", payload, err)
		}
		sends++
		return &http.Response{StatusCode: http.StatusNoContent, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, nil
	})}
	for range 2 {
		if _, err := worker.pipelines.RunWebhook(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if sends != 1 {
		t.Fatalf("got %d webhook sends, want 1", sends)
	}

	// A second worker can take over even while the previous callback is pending.
	siblingDB, err := sql.Open("pgx", config.Shared.DatabaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer siblingDB.Close()
	coordinator, err := lakeapi.NewPostgresProjectCoordinator(siblingDB, lakeapi.DefaultProjectCoordinatorConfig())
	if err != nil {
		t.Fatal(err)
	}
	successor, err := coordinator.Acquire(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	defer successor.Release()
	scope, err := f.native.LookupHostedProjectExisting(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.native.AdvanceWriteFence(ctx, successor.Fence(scope.StreamID)); err != nil {
		t.Fatal(err)
	}
	if err := f.native.AdvanceWriteFence(ctx, session.Fence(scope.StreamID)); !errors.Is(err, lakeapi.ErrProjectWriteFenceStale) {
		t.Fatalf("stale incarnation advanced the native fence: %v", err)
	}
}

func waitHostedLeaseCondition(t *testing.T, description string, timeout time.Duration, ready func(context.Context) bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for !ready(ctx) {
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s", description)
		case <-ticker.C:
		}
	}
}
