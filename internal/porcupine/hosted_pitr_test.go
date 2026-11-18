package porcupine

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"sync/atomic"
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
	if _, err := admin.ExecContext(t.Context(), "CREATE DATABASE hosted"); err != nil {
		t.Fatal(err)
	}
	// The base backup predates all service records and migrations.
	pg.backup()
	hostedConfig := hostedJourneyConfig(t, pg.dsn("hosted"), fixture)
	hostedConfig.OAuth = &OAuthConfig{Issuer: "https://porcupine-pitr-hosted.invalid", AllowLegacyKeys: true}
	hostedConfig.DuckLake.MaintenanceInterval = time.Hour
	hostedConfig.DuckLake.CleanupGrace = 72 * time.Hour
	hostedConfig.DuckLake.RestoreWindow = 24 * time.Hour
	// The drill supports restores only at or after its named restore point.
	var restoreFloor atomic.Pointer[time.Time]
	hostedConfig.HostedRecoveryFloor = func(context.Context) (time.Time, error) {
		if floor := restoreFloor.Load(); floor != nil {
			return *floor, nil
		}
		return time.Time{}, errors.New("restore point not recorded")
	}
	hosted, hostedHTTP, closeHosted := hostedCleanupWorker(t, hostedConfig)
	primary := pitrWorker{hostedHTTP, hosted}
	projectA, _ := sharedJourneyProject(t, primary.URL, defaultTenantID, "original project")
	tenant := sharedJourneyHTTP(t, primary.URL, http.MethodPost, "/api/v1/tenants", adminToken, []byte(`{"name":"PITR neighbor"}`), nil, 201)
	otherTenant := tenant["tenant"].(map[string]any)["id"].(string)
	projectB, _ := sharedJourneyProject(t, primary.URL, otherTenant, "isolated project")
	projectC, _ := sharedJourneyProject(t, primary.URL, defaultTenantID, "hosted output project")
	scope := pitrCreateHostedOutput(t, primary, projectC)
	deliveryID := pitrReadHostedOutput(t, primary, scope)
	pitrAssertEvent(t, primary.URL, projectC, adminToken, "hosted")
	revokedID, revoked := sharedJourneyKey(t, primary.URL, projectA, []string{"events:read"})
	_, editor := sharedJourneyKey(t, primary.URL, projectA, []string{"events:read", "events:edit", "events:delete"})
	_, foreign := sharedJourneyKey(t, primary.URL, projectB, []string{"events:read"})
	uuid := hostedCleanupWrite(t, hosted, projectA, "original")
	hostedCleanupWrite(t, hosted, projectB, "neighbor")
	pitrAssertEvent(t, primary.URL, projectA, revoked, "original")
	pitrAssertEvent(t, primary.URL, projectB, foreign, "neighbor")
	hostedProject, _ := sharedJourneyProject(t, primary.URL, defaultTenantID, "hosted project under deletion")
	pitrCreateHostedOutput(t, primary, hostedProject)
	hostedKeys := fixture.keys(t, fixture.projectPrefix(t, hosted.hosted.native, hostedProject))
	if len(hostedKeys) < 2 {
		t.Fatal("hosted recovery target must include Parquet and an immutable live manifest")
	}
	referencedKeys := fixture.keys(t, fixture.projectPrefix(t, hosted.hosted.native, projectA))
	if len(referencedKeys) == 0 {
		t.Fatal("the recovery target must reference S3 Parquet")
	}
	restoreAt := time.Now()
	restoreFloor.Store(&restoreAt)
	if _, err := admin.ExecContext(t.Context(), "SELECT pg_create_restore_point($1)", pitrRestorePoint); err != nil {
		t.Fatal(err)
	}

	eventPath := "/api/v1/projects/" + projectA + "/events/" + uuid
	edit := sharedJourneyHTTP(t, primary.URL, http.MethodPatch, eventPath, editor, []byte(
		`{"expected_revision":"0","idempotency_key":"restore-edit","snapshot":{"event":"edited","distinct_id":"a","timestamp":"2020-01-01T00:00:00Z","properties":{"amount":42}}}`), nil, 202)
	projectKey := lakeapi.ProjectWriteKey{SourceID: hosted.hosted.native.Config.SharedSourceID, ProjectID: projectA}
	if err := hosted.hosted.flusher.RecoverProject(t.Context(), projectKey); err != nil {
		t.Fatal(err)
	}
	pitrAssertEvent(t, primary.URL, projectA, editor, "edited")
	deletion, err := json.Marshal(map[string]any{"expected_revision": edit["revision"], "idempotency_key": "restore-delete"})
	if err != nil {
		t.Fatal(err)
	}
	sharedJourneyHTTP(t, primary.URL, http.MethodDelete, eventPath, editor, deletion, nil, 202)
	if err := hosted.hosted.flusher.RecoverProject(t.Context(), projectKey); err != nil {
		t.Fatal(err)
	}
	// Hosted history cleanup with its clock past every grace still keeps the
	// files that the restore point references.
	for day := range 12 {
		if err := hosted.hosted.native.CleanupHostedDue(t.Context(), time.Now().Add(time.Duration(30+day)*24*time.Hour), hosted.hosted.flusher.coordinator, hosted.cleanupHostedHistory); err != nil {
			t.Fatal(err)
		}
	}
	sharedJourneyHTTP(t, primary.URL, http.MethodDelete, "/api/v1/access-keys/"+revokedID, adminToken, nil, nil, 204)
	pitrAssertEvent(t, primary.URL, projectA, editor, "")
	sharedJourneyQuery(t, primary.URL, projectA, revoked, "SELECT event FROM events", 401)

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
	pg.recover(pitrRestorePoint)
	hostedConfig.DuckLake.Directory = t.TempDir()
	restoredHosted, restoredHTTP, _ := hostedCleanupWorker(t, hostedConfig)
	restored := pitrWorker{restoredHTTP, restoredHosted}
	pitrAssertEvent(t, restored.URL, hostedProject, adminToken, "hosted")

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
