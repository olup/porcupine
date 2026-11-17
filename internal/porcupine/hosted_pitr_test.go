package porcupine

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	lakeapi "github.com/olup/porcupine/internal/lake"
)

const pitrRestorePoint = "before_event_mutations"

func TestHostedPostgresPITR(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	bin := os.Getenv("PORCUPINE_TEST_PITR_POSTGRES_BIN")
	if bin == "" {
		t.Skip("run make hosted-restore-drill with PORCUPINE_TEST_PITR_POSTGRES_BIN set")
	}
	if os.Getenv("PORCUPINE_TEST_DUCKLAKE_S3_ENDPOINT") == "" {
		t.Fatal("the PITR drill requires the isolated S3 fixture")
	}
	fixture := nativeS3Fixture(t)
	pg := newPITRPostgres(t, bin)
	admin := pg.admin()
	for _, db := range []string{"control", "catalogs", "hosted"} {
		if _, err := admin.ExecContext(t.Context(), "CREATE DATABASE "+db); err != nil {
			t.Fatal(err)
		}
	}
	// The base backup predates all service records and migrations.
	pg.backup()
	primary, closePrimary := pitrOfflineWorker(t, pg, fixture, "pitr-primary")
	projectA, ingestA := sharedJourneyProject(t, primary.URL, defaultTenantID, "original project")
	tenant := sharedJourneyHTTP(t, primary.URL, http.MethodPost, "/api/v1/tenants", adminToken, []byte(`{"name":"PITR neighbor"}`), nil, 201)
	otherTenant := tenant["tenant"].(map[string]any)["id"].(string)
	projectB, ingestB := sharedJourneyProject(t, primary.URL, otherTenant, "isolated project")
	projectC, _ := sharedJourneyProject(t, primary.URL, defaultTenantID, "hosted output project")
	scope := pitrCreateHostedOutput(t, primary, projectC)
	deliveryID := pitrReadHostedOutput(t, primary, scope)
	pitrAssertEvent(t, primary.URL, projectC, adminToken, "hosted")
	revokedID, revoked := sharedJourneyKey(t, primary.URL, projectA, []string{"events:read"})
	_, editor := sharedJourneyKey(t, primary.URL, projectA, []string{"events:read", "events:edit", "events:delete"})
	_, foreign := sharedJourneyKey(t, primary.URL, projectB, []string{"events:read"})
	receipt := sharedJourneyNative(t, primary.URL, ingestA,
		`{"type":"track","message_id":"restore-original","actor_id":"a","name":"original","properties":{"amount":1}}`, 200)
	uuid := receipt["receipts"].([]any)[0].(map[string]any)["uuid"].(string)
	sharedJourneyNative(t, primary.URL, ingestB,
		`{"type":"track","message_id":"restore-other","actor_id":"b","name":"neighbor"}`, 200)
	duckLakeJourneyDrain(t, primary.service)
	pitrAssertEvent(t, primary.URL, projectA, revoked, "original")
	pitrAssertEvent(t, primary.URL, projectB, foreign, "neighbor")
	store := primary.service.lake.(*lakeapi.DuckLakeStore)
	referencedKeys := fixture.keys(t, fixture.projectPrefix(t, store, projectA))
	if len(referencedKeys) == 0 {
		t.Fatal("the recovery target must reference S3 Parquet")
	}
	hostedConfig := Config{Profile: ProfileHosted, AcceptanceMode: AcceptanceModeBuffered, BootstrapToken: adminToken,
		Shared: &SharedConfig{DatabaseURL: pg.dsn("hosted")}, allowUnreleasedHostedFixture: true,
		DuckLake: &lakeapi.DuckLakeConfig{PostgresURL: pg.dsn("hosted"), S3: &fixture.storage, MemoryLimitMB: 128, Threads: 1,
			MaintenanceInterval: time.Hour, CleanupGrace: 72 * time.Hour,
			RestoreWindow: 24 * time.Hour, RestoreMaxReaderTime: time.Hour, RestoreSafetyMargin: time.Hour}}
	hosted, hostedHTTP, closeHosted := hostedCleanupWorker(t, hostedConfig)
	hostedProject, _ := sharedJourneyProject(t, hostedHTTP.URL, defaultTenantID, "hosted project under deletion")
	pitrCreateHostedOutput(t, pitrWorker{hostedHTTP, hosted}, hostedProject)
	hostedKeys := fixture.keys(t, fixture.projectPrefix(t, hosted.hosted.native, hostedProject))
	if len(hostedKeys) < 2 {
		t.Fatal("hosted recovery target must include Parquet and an immutable live manifest")
	}
	if _, err := admin.ExecContext(t.Context(), "SELECT pg_create_restore_point($1)", pitrRestorePoint); err != nil {
		t.Fatal(err)
	}

	eventPath := "/api/v1/projects/" + projectA + "/events/" + uuid
	edit := sharedJourneyHTTP(t, primary.URL, http.MethodPatch, eventPath, editor, []byte(
		`{"expected_revision":"0","idempotency_key":"restore-edit","snapshot":{"event":"edited","distinct_id":"a","timestamp":"2020-01-01T00:00:00Z","properties":{"amount":42}}}`), nil, 202)
	duckLakeJourneyDrain(t, primary.service)
	pitrAssertEvent(t, primary.URL, projectA, editor, "edited")
	deletion, err := json.Marshal(map[string]any{"expected_revision": edit["revision"], "idempotency_key": "restore-delete"})
	if err != nil {
		t.Fatal(err)
	}
	sharedJourneyHTTP(t, primary.URL, http.MethodDelete, eventPath, editor, deletion, nil, 202)
	duckLakeJourneyDrain(t, primary.service)
	sharedJourneyHTTP(t, primary.URL, http.MethodDelete, "/api/v1/access-keys/"+revokedID, adminToken, nil, nil, 204)
	pitrAssertEvent(t, primary.URL, projectA, editor, "")
	sharedJourneyQuery(t, primary.URL, projectA, revoked, "SELECT event FROM events", 401)

	// Expire snapshots past the shared-reader horizon while obsolete files
	// are still protected by the configured grace.
	snapshotsBefore := pitrSnapshotCount(t, store, projectA)
	cleanupTime := time.Now().Add(9 * 24 * time.Hour)
	for i := 0; i < 3; i++ {
		err := store.CleanupDue(t.Context(), cleanupTime)
		if !errors.Is(err, lakeapi.ErrHostedNativeGuardRequired) {
			s3FixtureError(t, fixture, "run native cleanup", err)
		}
	}
	status, err := store.CleanupStatus(t.Context())
	if err != nil || status.CompletedCatalogs != 2 || status.FailedCatalogs != 1 {
		t.Fatalf("cleanup did not finish both shared catalogs and refuse the fenced hosted catalog: %+v (%v)", status, err)
	}
	if snapshotsAfter := pitrSnapshotCount(t, store, projectA); snapshotsAfter >= snapshotsBefore {
		t.Fatalf("cleanup did not expire project snapshots: before=%d after=%d", snapshotsBefore, snapshotsAfter)
	}
	if err := store.DeleteProject(t.Context(), projectC); !errors.Is(err, lakeapi.ErrHostedNativeGuardRequired) {
		t.Fatal("legacy physical cleanup must refuse the fenced hosted project", err)
	}
	if err := hosted.beginControlProjectDeletion(t.Context(), hostedProject); err != nil {
		t.Fatal(err)
	}
	if err := hosted.hosted.flusher.sealProject(t.Context(), hostedProject); err != nil {
		t.Fatal(err)
	}
	if err := hosted.cleanHostedProject(t.Context(), hostedProject); err != nil {
		t.Fatal(err)
	}
	if err := hosted.cleanHostedProject(t.Context(), hostedProject); !errors.Is(err, errHostedRecoveryFloor) {
		t.Fatalf("hosted cleanup crossed the current restore target: %v", err)
	}
	closeHosted()
	beforeRestore := fixture.keys(t, fixture.storage.Prefix)
	for _, key := range hostedKeys {
		if !slices.Contains(beforeRestore, key) {
			t.Fatalf("hosted cleanup dropped retained object %s", key)
		}
	}
	for _, key := range referencedKeys {
		if !slices.Contains(beforeRestore, key) {
			t.Fatalf("obsolete-file grace failed to retain %s", key)
		}
	}
	closePrimary()
	pg.recover(pitrRestorePoint)
	restored, _ := pitrOfflineWorker(t, pg, fixture, "pitr-restored")
	restoredHosted, restoredHTTP, _ := hostedCleanupWorker(t, hostedConfig)
	pitrAssertEvent(t, restoredHTTP.URL, hostedProject, adminToken, "hosted")

	// Reapply the post-target revocation over the private fixture listener.
	sharedJourneyHTTP(t, restored.URL, http.MethodDelete, "/api/v1/access-keys/"+revokedID, adminToken, nil, nil, 204)
	sharedJourneyQuery(t, restored.URL, projectA, revoked, "SELECT event FROM events", 401)
	sharedJourneyQuery(t, restored.URL, projectA, foreign, "SELECT event FROM events", 403)
	sharedJourneyQuery(t, restored.URL, projectB, editor, "SELECT event FROM events", 403)
	pitrAssertEvent(t, restored.URL, projectA, editor, "original")
	pitrAssertEvent(t, restored.URL, projectB, foreign, "neighbor")
	pitrAssertEvent(t, restored.URL, projectC, adminToken, "hosted")
	if id := pitrReadHostedOutput(t, restored, scope); id != deliveryID {
		t.Fatalf("restored output changed delivery identity: %s != %s", id, deliveryID)
	}
	if afterRestore := fixture.keys(t, fixture.storage.Prefix); !slices.Equal(beforeRestore, afterRestore) {
		t.Fatalf("recovery changed S3 objects: before=%v after=%v", beforeRestore, afterRestore)
	}
	native := restoredHosted.hosted.native
	scopeAfterRestore, err := native.LookupHostedProjectExisting(t.Context(), lakeapi.ProjectWriteKey{SourceID: native.Config.SharedSourceID, ProjectID: hostedProject})
	if err != nil {
		t.Fatal(err)
	}
	proofs, err := native.PendingNativeEffects(t.Context(), scopeAfterRestore, 0, 2)
	if err != nil || len(proofs.Evidence) != 1 {
		t.Fatalf("restore lost the hosted native proof: %+v %v", proofs, err)
	}
	if _, err := native.Remote.ReadImmutableEffectObject(t.Context(), scopeAfterRestore, proofs.Evidence[0].Effects.Manifest); err != nil {
		t.Fatal("restore could not read the hosted immutable manifest", err)
	}
}

type pitrWorker struct {
	*httptest.Server
	service *Service
}

func pitrOfflineWorker(t *testing.T, pg *pitrPostgres, fixture duckLakeS3Fixture, worker string) (pitrWorker, func()) {
	t.Helper()
	host := httptest.NewUnstartedServer(http.NotFoundHandler())
	t.Cleanup(host.Close)
	dir := t.TempDir()
	service, err := Open(Config{
		DBPath: filepath.Join(dir, "scratch.db"), BootstrapToken: adminToken,
		OAuth: &OAuthConfig{Issuer: "https://porcupine-pitr.invalid", AllowLegacyKeys: true},
		Shared: &SharedConfig{DatabaseURL: pg.dsn("control"), WorkerID: worker,
			RouteURL: "http://" + host.Listener.Addr().String(), RoutingSecret: []byte(sharedJourneySecret)},
		DuckLake: &lakeapi.DuckLakeConfig{Directory: filepath.Join(dir, "lake"), PostgresURL: pg.dsn("catalogs"),
			S3: &fixture.storage, MemoryLimitMB: 128, Threads: 1, MaxOpenProjects: 3,
			SnapshotRetention: 24 * time.Hour, CleanupGrace: 30 * 24 * time.Hour, MaintenanceInterval: 24 * time.Hour,
			RestoreWindow: 14 * 24 * time.Hour, RestoreMaxReaderTime: 7 * 24 * time.Hour, RestoreSafetyMargin: 2 * 24 * time.Hour},
	})
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	closeWorker := func() {
		if !closed {
			closed = true
			host.Close()
			if err := service.Close(); err != nil {
				t.Error(err)
			}
		}
	}
	t.Cleanup(closeWorker)
	if service.pipelines.Enabled() || service.exports.Enabled() {
		t.Fatal("offline recovery worker must not enable outputs")
	}
	service.deliveryCancel()
	<-service.deliveryDone
	service.lakeMaintenanceCancel()
	<-service.lakeMaintenanceDone
	host.Config.Handler = service.Handler()
	host.Start()
	return pitrWorker{host, service}, closeWorker
}

func pitrAssertEvent(t *testing.T, origin, project, key, want string) {
	t.Helper()
	rows := sharedJourneyQuery(t, origin, project, key, "SELECT event FROM events", 200)["result"].(map[string]any)["data"].([]any)
	if want == "" && len(rows) == 0 {
		return
	}
	if len(rows) != 1 || rows[0].(map[string]any)["event"] != want {
		t.Fatalf("want event %q, got %v", want, rows)
	}
}

func pitrSnapshotCount(t *testing.T, store *lakeapi.DuckLakeStore, project string) int {
	t.Helper()
	lake, release, err := store.LeaseProject(t.Context(), project)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	var count int
	if err := lake.DB.QueryRowContext(t.Context(), "SELECT count(*) FROM ducklake_snapshots('lake')").Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}
