package porcupine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/acceptance"
	eventapi "github.com/olup/porcupine/internal/event"
	lakeapi "github.com/olup/porcupine/internal/lake"
)

func TestSharedHostedRecoveryMigrationQueuesExistingStreams(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db, _ := sharedControlFixture(t)
	for _, ddl := range sharedControlSteps[:21] {
		if _, err := db.ExecContext(ctx, ddl); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.ExecContext(ctx, `
INSERT INTO metadata(key,value) VALUES('schema_version','21');
INSERT INTO tenants(id,name,created_at,hot_query_max_concurrent,hot_query_timeout_ms,hot_query_max_rows,hot_query_max_result_bytes)
VALUES('t','tenant','now',1,1000,1,256);
INSERT INTO projects(id,tenant_id,name,created_at) VALUES('quiet','t','quiet','now'),('stream','t','stream','now');
INSERT INTO pipeline_effect_streams(source_id,project_id,catalog_id,stream_id,tenant_id)
VALUES('source','stream','catalog','effects','t');`); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := migrateSharedControl(ctx, tx); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if projects, err := hostedRecoveryProjects(ctx, db, ""); err != nil || !slices.Equal(projects, []string{"stream"}) {
		t.Fatalf("migration recovery projects = %v, want [stream], %v", projects, err)
	}
}

func TestSharedHostedRecoveryBackoffMigratesV25(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db, _ := sharedControlFixture(t)
	exec := func(query string) {
		t.Helper()
		if _, err := db.ExecContext(ctx, query); err != nil {
			t.Fatal(err)
		}
	}
	for _, ddl := range sharedControlSteps[:25] {
		exec(ddl)
	}
	exec(`INSERT INTO metadata(key,value) VALUES('schema_version','25');
INSERT INTO tenants(id,name,created_at,hot_query_max_concurrent,hot_query_timeout_ms,hot_query_max_rows,hot_query_max_result_bytes)
VALUES('tenant','tenant','now',1,1000,1,256);
INSERT INTO projects(id,tenant_id,name,created_at) VALUES('project','tenant','project','now');
INSERT INTO hosted_recovery_pending(project_id,generation,quarantine_code) VALUES('project',9,'native_evidence_gap');`)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := migrateSharedControl(ctx, tx); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var version int
	if err := db.QueryRowContext(ctx, `SELECT value FROM metadata WHERE key='schema_version'`).Scan(&version); err != nil || version != len(sharedControlSteps) {
		t.Fatalf("expected upgraded schema version %d, got %d, error %v", len(sharedControlSteps), version, err)
	}
	assertPending := func(generation int64, deferred bool) {
		t.Helper()
		var actualGeneration int64
		var quarantine string
		var retryAt sql.NullTime
		err := db.QueryRowContext(ctx, `SELECT generation,quarantine_code,retry_at FROM hosted_recovery_pending WHERE project_id='project'`).Scan(&actualGeneration, &quarantine, &retryAt)
		if err != nil || actualGeneration != generation || quarantine != "native_evidence_gap" || retryAt.Valid != deferred {
			t.Fatalf("pending generation=%d, quarantine=%q, retry_at=%v, error=%v", actualGeneration, quarantine, retryAt, err)
		}
	}
	coordinator, err := lakeapi.NewPostgresProjectCoordinator(db, lakeapi.DefaultProjectCoordinatorConfig())
	if err != nil {
		t.Fatal(err)
	}
	session, err := coordinator.Acquire(ctx, lakeapi.ProjectWriteKey{SourceID: "source", ProjectID: "project"})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Release()
	postpone := func(generation int64) {
		t.Helper()
		if err := deferHostedRecovery(ctx, session, "project", generation); err != nil {
			t.Fatal(err)
		}
	}
	assertPending(9, false)
	postpone(0) // A producer queued a positive generation after a reservation-only read.
	assertPending(9, false)
	postpone(9)
	assertPending(9, true)
	exec(`INSERT INTO hosted_mutation_projects(source_id,project_id,tenant_id,catalog_id,stream_id)
VALUES('source','project','tenant','catalog','stream');
INSERT INTO hosted_mutation_intents(source_id,project_id,tenant_id,catalog_id,stream_id,mutation_id,key_hash,request_hash,actor_hash,operation,expected_revision,revision,state,body_key,body_sha256,body_bytes,body_prefix,created_at)
VALUES('source','project','tenant','catalog','stream','mutation','key','request','actor','edit',0,1,'staging','body','hash',1,'prefix','now');
UPDATE hosted_mutation_intents SET state='ready',acceptance_order=1 WHERE project_id='project';`)
	assertPending(10, false)
	postpone(9) // A ready mutation superseded the failure from the previous generation.
	assertPending(10, false)
	postpone(10)
	assertPending(10, true)
	exec(`INSERT INTO hosted_worker_incarnations(incarnation_id,expires_at,reclaim_after)
VALUES('lost-worker',clock_timestamp()-interval '2 seconds',clock_timestamp()-interval '1 second');
INSERT INTO hosted_project_activity(project_id,activity_id,incarnation_id) VALUES('project','activity','lost-worker');`)
	if projects, err := hostedRecoveryProjects(ctx, db, ""); err != nil || !slices.Equal(projects, []string{"project"}) {
		t.Fatalf("reclaimed activity did not make project eligible: %v, %v", projects, err)
	}
	assertPending(11, false)
}

func expireHostedRecovery(t *testing.T, db *sql.DB, project string) {
	t.Helper()
	result, err := db.ExecContext(t.Context(), `UPDATE hosted_recovery_pending SET retry_at=clock_timestamp()-interval '1 second'
WHERE project_id=$1 AND retry_at IS NOT NULL`, project)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := result.RowsAffected(); err != nil || n != 1 {
		t.Fatalf("expected one deferred recovery, affected %d rows, error %v", n, err)
	}
}

func (f *hostedLiveFlusher) RecoverProject(ctx context.Context, key lakeapi.ProjectWriteKey) error {
	session, err := f.coordinator.Acquire(ctx, key)
	if err != nil {
		return err
	}
	defer session.Release()
	var tenant string
	var generation int64
	err = f.publisher.control.QueryRowContext(ctx, `SELECT p.tenant_id,COALESCE(r.generation,0)
FROM projects p LEFT JOIN hosted_recovery_pending r ON r.project_id=p.id WHERE p.id=$1`, key.ProjectID).Scan(&tenant, &generation)
	if err != nil {
		return err
	}
	return ignoreHostedACKPending(f.recoverProjectHeld(ctx, session, key, tenant, generation))
}

func TestDuckLakeS3SharedPostgresHostedRecoveryBackoff(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	ctx := t.Context()
	fixture := nativeS3Fixture(t)
	traffic := economicsS3Proxy(t, &fixture)
	a, b := sharedNativeJourneyPair(t, fixture)
	db := a.service.control
	first, second := a.service.hosted.flusher, b.service.hosted.flusher
	project, _ := sharedJourneyProject(t, a.host.URL, defaultTenantID, "blocked recovery")
	healthy, _ := sharedJourneyProject(t, b.host.URL, defaultTenantID, "healthy recovery")
	key := lakeapi.ProjectWriteKey{SourceID: first.native.Config.SharedSourceID, ProjectID: project}
	healthyKey := lakeapi.ProjectWriteKey{SourceID: key.SourceID, ProjectID: healthy}
	if _, err := db.ExecContext(ctx, `
CREATE TABLE test_blocked_publications(project_id TEXT PRIMARY KEY);
CREATE FUNCTION test_block_effect_publication() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.sequence > OLD.sequence AND EXISTS(SELECT 1 FROM test_blocked_publications WHERE project_id=NEW.project_id) THEN
  RAISE EXCEPTION 'fixture blocked publication';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER test_block_effect_publication BEFORE UPDATE ON pipeline_effect_streams
FOR EACH ROW EXECUTE FUNCTION test_block_effect_publication();`); err != nil {
		t.Fatal(err)
	}
	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := db.ExecContext(ctx, query, args...); err != nil {
			t.Fatal(err)
		}
	}
	snapshots := func(project string) int64 {
		t.Helper()
		schema := lakeapi.SharedNativeSchema(lakeapi.SharedNativeCatalogID(key.SourceID, project))
		var count int64
		if err := first.native.SharedCatalog.QueryRowContext(ctx, `SELECT count(*) FROM `+schema+`.ducklake_snapshot`).Scan(&count); err != nil {
			t.Fatal(err)
		}
		return count
	}
	blocked := func(err error) {
		t.Helper()
		if err == nil || !strings.Contains(err.Error(), "fixture blocked publication") {
			t.Fatalf("expected publication failure, got %v", err)
		}
	}
	stageUnpublished := func(flusher *hostedLiveFlusher, project string) acceptance.FlushBatch {
		t.Helper()
		exec(`INSERT INTO test_blocked_publications(project_id) VALUES($1)`, project)
		at := time.Now().UTC()
		event := eventapi.Event{ProjectID: project, UUID: eventapi.NewID(), Event: "unpublished", DistinctID: "actor",
			Properties: json.RawMessage(`{}`), Context: json.RawMessage(`{}`), Timestamp: at.Format(time.RFC3339Nano), ReceivedAt: at.Format(time.RFC3339Nano)}
		batch := acceptance.FlushBatch{ID: eventapi.NewID(), ProjectID: project, Events: []acceptance.BufferedEvent{{Input: acceptance.Input{Event: event}, AdmittedAt: at}}}
		blocked(flusher.FlushLive(ctx, batch))
		return batch
	}
	candidates := func() []string {
		t.Helper()
		projects, err := hostedRecoveryProjects(ctx, db, "")
		if err != nil {
			t.Fatal(err)
		}
		return projects
	}
	batch := stageUnpublished(first, project)
	stageUnpublished(second, healthy)
	exec(`DELETE FROM test_blocked_publications WHERE project_id=$1`, healthy)
	func() {
		session, err := first.coordinator.Acquire(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		defer session.Release()
		if err := second.recoverProject(ctx, key); !errors.Is(err, lakeapi.ErrProjectWriteBusy) {
			t.Fatalf("expected contention to yield, got %v", err)
		}
	}()
	var pending bool
	if err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM hosted_recovery_pending WHERE project_id=$1)`, project).Scan(&pending); err != nil || pending {
		t.Fatalf("expected reservation-only recovery, pending row=%t, error=%v", pending, err)
	}
	if projects := candidates(); !slices.Contains(projects, project) || !slices.Contains(projects, healthy) {
		t.Fatalf("workers missed queued reservations: %v", projects)
	}
	start, startPuts := snapshots(project), traffic.puts()
	blocked(first.recoverProject(ctx, key))
	var deferred bool
	if err := db.QueryRowContext(ctx, `SELECT retry_at>clock_timestamp() FROM hosted_recovery_pending WHERE project_id=$1`, project).Scan(&deferred); err != nil || !deferred {
		t.Fatalf("reservation-only failure did not create a shared retry deadline: %t, %v", deferred, err)
	}
	// The second worker discovered this project before the first finished.
	if err := second.recoverProject(ctx, key); err != nil {
		t.Fatal("stale candidate ignored the deadline", err)
	}
	workers := [2]*hostedLiveFlusher{first, second}
	for i := range 4 {
		projects, err := hostedRecoveryProjects(ctx, workers[i%2].publisher.control, "")
		if err != nil || slices.Contains(projects, project) || !slices.Contains(projects, healthy) {
			t.Fatalf("expected only the healthy candidate, got %v, error %v", projects, err)
		}
	}
	if writes, puts := snapshots(project)-start, traffic.puts()-startPuts; writes != 1 || puts != 1 {
		t.Fatalf("six scheduled replays wrote %d DuckLake snapshots and issued %d S3 PUTs, want one each", writes, puts)
	}
	t.Log("six scheduled replays: 1 DuckLake snapshot, 1 S3 PUT")
	if err := second.recoverProject(ctx, healthyKey); err != nil {
		t.Fatal("healthy sibling could not reconcile", err)
	}
	if slices.Contains(candidates(), healthy) {
		t.Fatal("healthy recovery stayed queued")
	}
	healthyStart := snapshots(healthy)
	hostedCleanupWrite(t, b.service, healthy, "ordinary successful live flush")
	if got := snapshots(healthy) - healthyStart; got != 1 {
		t.Fatalf("ordinary live flush wrote %d DuckLake snapshots, want one", got)
	}
	start, startPuts = snapshots(project), traffic.puts()
	blocked(first.FlushLive(ctx, batch))
	t.Logf("one explicit buffered-flush retry: %d DuckLake snapshots, %d S3 PUTs", snapshots(project)-start, traffic.puts()-startPuts)

	// A quarantine signal is a new generation and makes the project eligible now.
	exec(`UPDATE hosted_recovery_pending SET quarantine_code='native_evidence_gap',generation=generation+1 WHERE project_id=$1`, project)
	if !slices.Contains(candidates(), project) {
		t.Fatal("new quarantine generation did not wake recovery")
	}
	blocked(second.recoverProject(ctx, key))
	var quarantine string
	if err := db.QueryRowContext(ctx, `SELECT retry_at>clock_timestamp(),quarantine_code FROM hosted_recovery_pending WHERE project_id=$1`, project).Scan(&deferred, &quarantine); err != nil || !deferred || quarantine != "native_evidence_gap" {
		t.Fatalf("quarantined generation did not retain its reason and defer: %t, %q, %v", deferred, quarantine, err)
	}
	if slices.Contains(candidates(), project) {
		t.Fatal("quarantined generation bypassed the retry deadline")
	}
	exec(`DELETE FROM test_blocked_publications WHERE project_id=$1`, project)
	start, startPuts = snapshots(project), traffic.puts()
	if err := first.recoverProject(ctx, key); err != nil {
		t.Fatal(err)
	}
	if snapshots(project) != start || traffic.puts() != startPuts {
		t.Fatal("scheduled recovery wrote before the retry deadline")
	}
	expireHostedRecovery(t, db, project)
	if !slices.Contains(candidates(), project) {
		t.Fatal("retry deadline passed without making the project eligible")
	}
	if err := second.recoverProject(ctx, key); err != nil {
		t.Fatal("repaired project could not reconcile", err)
	}
	if slices.Contains(candidates(), project) {
		t.Fatal("repaired recovery stayed queued")
	}
	scope, err := second.native.LookupHostedProjectExisting(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := second.native.PendingNativeEffects(ctx, scope, 0, 1)
	if err != nil || proof.AcknowledgedThrough != 1 || len(proof.Evidence) != 1 || proof.Evidence[0].OperationID != batch.ID {
		t.Fatalf("recovery did not publish and acknowledge the original batch: %+v, %v", proof, err)
	}
}

func TestDuckLakeS3SharedPostgresHostedRecoveryAfterWriterKill(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	fixture := nativeS3Fixture(t)
	target, err := url.Parse(fixture.storage.Endpoint)
	if err != nil {
		t.Fatal(err)
	}
	forward := httputil.NewSingleHostReverseProxy(target)
	forward.Transport = fixture.client.Options().HTTPClient.(*http.Client).Transport
	var block atomic.Bool
	var hasReservation func(context.Context) (bool, error)
	firstPUT := make(chan error, 1)
	releasePUT := make(chan struct{})
	unblock := sync.OnceFunc(func() { block.Store(false); close(releasePUT) })
	defer unblock()
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if block.Load() && r.Method == http.MethodPut {
			reserved, err := hasReservation(r.Context())
			if reserved || err != nil {
				select {
				case firstPUT <- err:
				default:
				}
				select {
				case <-r.Context().Done():
				case <-releasePUT:
				}
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
		}
		forward.ServeHTTP(w, r)
	}))
	t.Cleanup(proxy.Close)
	fixture.storage.Endpoint, fixture.storage.CACertFile = proxy.URL, ""
	dsn := sharedPostgresDSN(t)
	config := hostedJourneyConfig(t, dsn, fixture)
	survivor, host, _ := hostedCleanupWorker(t, config)
	project, ingest := sharedJourneyProject(t, host.URL, defaultTenantID, "orphaned reservation")
	idle, _ := sharedJourneyProject(t, host.URL, defaultTenantID, "idle")
	deadTicketOnly, _ := sharedJourneyProject(t, host.URL, defaultTenantID, "dead ticket without reservation")
	source := survivor.hosted.native.Config.SharedSourceID
	snapshot := func(project string) int64 {
		t.Helper()
		schema := lakeapi.SharedNativeSchema(lakeapi.SharedNativeCatalogID(source, project))
		var id int64
		if err := survivor.hosted.native.SharedCatalog.QueryRowContext(t.Context(), "SELECT max(snapshot_id) FROM "+schema+".ducklake_snapshot").Scan(&id); err != nil {
			t.Fatal(err)
		}
		return id
	}
	idleBefore, deadTicketBefore := snapshot(idle), snapshot(deadTicketOnly)
	writerLake := *config.DuckLake
	writerLake.Directory = t.TempDir()
	writer := startSharedJourneyProcess(t, sharedJourneyProcessConfig{DatabaseURL: dsn, HostedLake: &writerLake, HostedFlushInterval: 250 * time.Millisecond})
	db := survivor.control
	ctx := t.Context()
	hasReservation = func(ctx context.Context) (bool, error) {
		var reserved bool
		err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pipeline_effect_reservations WHERE project_id=$1)`, project).Scan(&reserved)
		return reserved, err
	}
	block.Store(true)
	ack := sharedJourneyNative(t, writer.origin, ingest, `{"type":"track","message_id":"orphan","name":"interrupted","actor_id":"writer"}`, 200)
	if ack["durability"] != "buffered" || ack["received"] != float64(1) {
		t.Fatal("writer did not buffer the event", ack)
	}
	select {
	case err := <-firstPUT:
		if err != nil {
			t.Fatal("read reservation at first PUT", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("writer did not reach the first PUT")
	}
	var incarnation string
	err = db.QueryRowContext(ctx, `SELECT a.incarnation_id
FROM pipeline_effect_reservations r JOIN hosted_project_activity a USING(project_id) WHERE r.project_id=$1`, project).Scan(&incarnation)
	if err != nil {
		t.Fatal("first PUT did not follow a committed reservation and ticket", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO hosted_project_activity(project_id,activity_id,incarnation_id) VALUES($1,'ticket-only',$2)`, deadTicketOnly, incarnation); err != nil {
		t.Fatal(err)
	}
	writer.stopProcess(t, true)
	var killed interface{ ExitCode() int }
	if !errors.As(writer.err, &killed) || killed.ExitCode() != -1 {
		t.Fatalf("writer did not die by signal: %v", writer.err)
	}
	unblock()
	survivor.startHostedRecovery(100 * time.Millisecond)
	waitFor := func(query, value string) {
		t.Helper()
		deadline := time.Now().Add(15 * time.Second)
		for {
			var ready bool
			if err := db.QueryRowContext(ctx, query, value).Scan(&ready); err != nil {
				t.Fatal(err)
			}
			if ready {
				return
			}
			if time.Now().After(deadline) {
				t.Fatal("surviving worker did not reconcile", value)
			}
			time.Sleep(25 * time.Millisecond)
		}
	}
	waitFor(`SELECT NOT EXISTS(SELECT 1 FROM hosted_recovery_pending WHERE project_id=$1)
AND NOT EXISTS(SELECT 1 FROM pipeline_effect_reservations WHERE project_id=$1)`, project)
	if got := snapshot(deadTicketOnly); got != deadTicketBefore {
		t.Fatalf("live-grace ticket wrote a snapshot: %d → %d", deadTicketBefore, got)
	}
	if _, err := db.ExecContext(ctx, `UPDATE hosted_worker_incarnations
SET expires_at=clock_timestamp()-interval '2 seconds',reclaim_after=clock_timestamp()-interval '1 second' WHERE incarnation_id=$1`, incarnation); err != nil {
		t.Fatal(err)
	}
	waitFor(`SELECT NOT EXISTS(SELECT 1 FROM hosted_project_activity WHERE activity_id='ticket-only' AND project_id=$1)
AND NOT EXISTS(SELECT 1 FROM hosted_recovery_pending WHERE project_id=$1)`, deadTicketOnly)
	if got := snapshot(deadTicketOnly); got != deadTicketBefore+1 {
		t.Fatalf("reclaimed ticket did not cause one recovery fence: %d → %d", deadTicketBefore, got)
	}
	if after := snapshot(idle); after != idleBefore {
		t.Fatalf("unrelated idle project wrote a snapshot: %d → %d", idleBefore, after)
	}
}
