package porcupine

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	eventapi "github.com/olup/porcupine/internal/event"
	lakeapi "github.com/olup/porcupine/internal/lake"
)

func TestDuckLakeS3SharedPostgresHostedProjectCleanup(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	fixture := nativeS3Fixture(t)
	ctx := t.Context()
	dsn := sharedPostgresDSN(t)
	lake := lakeapi.DuckLakeConfig{PostgresURL: dsn, S3: &fixture.storage, Threads: 1, MemoryLimitMB: 128, MaxOpenProjects: 2,
		MaintenanceInterval: time.Hour, CleanupGrace: 3 * time.Second,
		RestoreWindow: time.Second, RestoreMaxReaderTime: time.Second, RestoreSafetyMargin: time.Second}
	base := Config{Profile: ProfileHosted, AcceptanceMode: AcceptanceModeBuffered, BootstrapToken: adminToken,
		Shared: &SharedConfig{DatabaseURL: dsn}, DuckLake: &lake}
	a, host, closeA := hostedCleanupWorker(t, base)
	project, _ := sharedJourneyProject(t, host.URL, defaultTenantID, "delete this project")
	tenant := sharedJourneyHTTP(t, host.URL, http.MethodPost, "/api/v1/tenants", adminToken, []byte(`{"name":"Independent tenant"}`), nil, 201)
	tenantB := tenant["tenant"].(map[string]any)["id"].(string)
	other, _ := sharedJourneyProject(t, host.URL, tenantB, "retain this project")
	eventA := hostedCleanupWrite(t, a, project, "selected data")
	hostedCleanupWrite(t, a, other, "neighbor data")

	_, editor := sharedJourneyKey(t, host.URL, project, []string{"events:read", "events:edit"})
	sharedJourneyHTTP(t, host.URL, http.MethodPatch, "/api/v1/projects/"+project+"/events/"+eventA, editor, []byte(
		`{"expected_revision":"0","idempotency_key":"cleanup-owns-body","snapshot":{"event":"edited","distinct_id":"a","timestamp":"2020-01-01T00:00:00Z","properties":{"amount":42}}}`), nil, 202)

	prefixA := fixture.projectPrefix(t, a.hosted.native, project)
	prefixB := fixture.projectPrefix(t, a.hosted.native, other)
	unknown := prefixA + "unexpected/unowned.json"
	fixture.put(t, unknown, "retain")
	beforeA, beforeB := fixture.keys(t, prefixA), fixture.keys(t, prefixB)
	if len(beforeA) < 3 || len(beforeB) == 0 {
		t.Fatalf("expected exact managed objects in both catalogs: %v / %v", beforeA, beforeB)
	}

	if err := a.beginTenantDeletion(ctx, defaultTenantID); err != nil {
		t.Fatal(err)
	}
	if err := a.hosted.flusher.sealProject(ctx, project); err != nil {
		t.Fatal(err)
	}
	if err := a.cleanHostedProject(ctx, project); err != nil {
		t.Fatal(err)
	}
	if err := a.cleanHostedProject(ctx, project); !errors.Is(err, errHostedRecoveryFloor) {
		t.Fatalf("cleanup without a verified floor: %v", err)
	}
	held := fixture.keys(t, prefixA)
	for _, key := range beforeA {
		if !slices.Contains(held, key) {
			t.Fatalf("retention gate dropped managed object %q", key)
		}
	}
	var inventoryAt time.Time
	if err := a.control.QueryRowContext(ctx, `SELECT inventory_at FROM hosted_project_cleanup WHERE project_id=$1`, project).Scan(&inventoryAt); err != nil {
		t.Fatal(err)
	}
	closeA()

	// The cold worker has no prior catalog handles or local inventory. The
	// fixture attests that no restore target before the completed inventory is supported.
	base.HostedRecoveryFloor = func(context.Context) (time.Time, error) { return inventoryAt, nil }
	b, coldHost, _ := hostedCleanupWorker(t, base)
	deadline := time.Now().Add(15 * time.Second)
	for {
		err := b.cleanHostedProject(ctx, project)
		if err != nil && !errors.Is(err, errHostedRecoveryFloor) {
			t.Fatal(err)
		}
		var complete int
		if err := b.control.QueryRowContext(ctx, `SELECT completed FROM project_deletions WHERE project_id=$1`, project).Scan(&complete); err != nil {
			t.Fatal(err)
		}
		if complete == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("retained cleanup did not complete")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if remaining := fixture.keys(t, prefixA); !slices.Equal(remaining, []string{unknown}) {
		t.Fatalf("cleanup changed unowned keys or retained managed keys: %v", remaining)
	}
	if remaining := fixture.keys(t, prefixB); !slices.Equal(beforeB, remaining) {
		t.Fatalf("cleanup changed the other tenant's objects: %v", remaining)
	}
	pitrAssertEvent(t, coldHost.URL, other, adminToken, "neighbor data")
	if err := b.finishTenantDeletion(ctx, defaultTenantID); err != nil {
		t.Fatal(err)
	}
	if err := b.cleanHostedProject(ctx, project); err != nil {
		t.Fatal("completed cleanup did not converge on retry", err)
	}
	var state string
	if err := b.control.QueryRowContext(ctx, `SELECT state FROM tenant_deletions WHERE tenant_id=$1`, defaultTenantID).Scan(&state); err != nil || state != "logically_deleted" {
		t.Fatalf("tenant deletion state = %q, %v", state, err)
	}
}

func hostedCleanupWorker(t *testing.T, c Config) (*Service, *httptest.Server, func()) {
	t.Helper()
	s, err := Open(c)
	if err != nil {
		t.Fatal(err)
	}
	s.deletionCancel()
	<-s.deletionDone
	s.hosted.stopRecovery()
	<-s.hosted.recoveryDone
	host := httptest.NewServer(s.Handler())
	closed := false
	closeWorker := func() {
		if closed {
			return
		}
		closed = true
		host.Close()
		if err := s.Close(); err != nil {
			t.Error("close hosted worker", err)
		}
	}
	t.Cleanup(closeWorker)
	return s, host, closeWorker
}

func hostedCleanupWrite(t *testing.T, s *Service, project, name string) string {
	t.Helper()
	at := time.Now().UTC()
	event := eventapi.Event{ProjectID: project, UUID: eventapi.NewID(), Event: name, DistinctID: "actor",
		Properties: json.RawMessage(`{}`), Context: json.RawMessage(`{}`), Timestamp: at.Format(time.RFC3339Nano), ReceivedAt: at.Format(time.RFC3339Nano)}
	batch := bufferedFlushBatch{id: eventapi.NewID(), projectID: project, events: []bufferedLiveEvent{{input: validatedLiveEvent{event: event}, admittedAt: at}}}
	if err := s.hosted.flusher.FlushLive(t.Context(), batch); err != nil {
		t.Fatal("commit hosted live event", err)
	}
	return event.UUID
}
