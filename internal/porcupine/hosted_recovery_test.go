package porcupine

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"slices"
	"sync/atomic"
	"testing"
	"time"

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
	firstPUT := make(chan struct{}, 1)
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if block.Load() && r.Method == http.MethodPut {
			select {
			case firstPUT <- struct{}{}:
			default:
			}
			<-r.Context().Done()
			return
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
	block.Store(true)
	ack := sharedJourneyNative(t, writer.origin, ingest, `{"type":"track","message_id":"orphan","name":"interrupted","actor_id":"writer"}`, 200)
	if ack["durability"] != "buffered" || ack["received"] != float64(1) {
		t.Fatal("writer did not buffer the event", ack)
	}
	select {
	case <-firstPUT:
	case <-time.After(15 * time.Second):
		t.Fatal("writer did not reach the first PUT")
	}
	db := survivor.control
	ctx := t.Context()
	var incarnation, attempt string
	err = db.QueryRowContext(ctx, `SELECT a.incarnation_id,r.acquisition_id
FROM pipeline_effect_reservations r JOIN hosted_project_activity a USING(project_id) WHERE r.project_id=$1`, project).Scan(&incarnation, &attempt)
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
	block.Store(false)
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
	waitFor(`SELECT EXISTS(SELECT 1 FROM pipeline_effect_cleanup WHERE owner_id=$1)`, attempt)
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
