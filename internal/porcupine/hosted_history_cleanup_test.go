package porcupine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/olup/porcupine/internal/acceptance"
	eventapi "github.com/olup/porcupine/internal/event"
	lakeapi "github.com/olup/porcupine/internal/lake"
)

// A deleted event must leave every live S3 object of its project once the
// scheduled hosted maintenance has expired its snapshots and the file grace
// has passed. The neighbor project keeps its data.
func TestDuckLakeS3SharedPostgresHostedHistoryPurge(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	fixture := nativeS3Fixture(t)
	dsn := sharedPostgresDSN(t)
	lake := lakeapi.DuckLakeConfig{PostgresURL: dsn, S3: &fixture.storage, Threads: 1, MemoryLimitMB: 128, MaxOpenProjects: 2,
		MaintenanceInterval: time.Second, SnapshotRetention: time.Second, CleanupInterval: time.Second, CleanupGrace: 3 * time.Second,
		RestoreWindow: time.Second, RestoreMaxReaderTime: time.Second, RestoreSafetyMargin: time.Second}
	// The fixture attests that no restore target before the present is supported.
	config := Config{Profile: ProfileHosted, AcceptanceMode: AcceptanceModeBuffered, BootstrapToken: adminToken,
		Shared: &SharedConfig{DatabaseURL: dsn}, DuckLake: &lake,
		HostedRecoveryFloor: func(context.Context) (time.Time, error) { return time.Now().Add(-time.Second), nil }}
	s, host, _ := hostedCleanupWorker(t, config)
	project, _ := sharedJourneyProject(t, host.URL, defaultTenantID, "purge deleted event")
	neighbor, _ := sharedJourneyProject(t, host.URL, defaultTenantID, "purge neighbor")
	marker, kept := "purge-marker-"+eventapi.NewID(), "neighbor-marker-"+eventapi.NewID()
	deleted := hostedHistoryWrite(t, s, project, "deleted", marker)
	hostedHistoryWrite(t, s, project, "kept", "kept-"+eventapi.NewID())
	hostedHistoryWrite(t, s, neighbor, "neighbor", kept)
	prefix := fixture.projectPrefix(t, s.hosted.native, project)
	neighborPrefix := fixture.projectPrefix(t, s.hosted.native, neighbor)
	if len(fixture.objectsContaining(t, prefix, marker)) == 0 || len(fixture.objectsContaining(t, neighborPrefix, kept)) == 0 {
		t.Fatal("the fixture cannot observe event bytes in S3 objects")
	}

	_, editor := sharedJourneyKey(t, host.URL, project, []string{"events:read", "events:delete"})
	sharedJourneyHTTP(t, host.URL, http.MethodDelete, "/api/v1/projects/"+project+"/events/"+deleted, editor,
		[]byte(`{"expected_revision":"0","idempotency_key":"purge-delete"}`), nil, 202)
	if err := s.hosted.flusher.RecoverProject(t.Context(), lakeapi.ProjectWriteKey{SourceID: s.hosted.native.Config.SharedSourceID, ProjectID: project}); err != nil {
		t.Fatal(err)
	}
	pitrAssertEvent(t, host.URL, project, editor, "kept")

	deadline := time.Now().Add(2 * time.Minute)
	for {
		remaining := fixture.objectsContaining(t, prefix, marker)
		if len(remaining) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("deleted event bytes remain in live S3 objects: %v", remaining)
		}
		time.Sleep(time.Second)
	}
	pitrAssertEvent(t, host.URL, project, editor, "kept")
	pitrAssertEvent(t, host.URL, neighbor, adminToken, "neighbor")
	if len(fixture.objectsContaining(t, neighborPrefix, kept)) == 0 {
		t.Fatal("history cleanup removed the neighbor's live data")
	}
	status := sharedJourneyHTTP(t, host.URL, http.MethodGet, "/api/v1/status", adminToken, nil, nil, 200)
	cleanup, _ := status["ducklake_cleanup"].(map[string]any)
	if cleanup["enabled"] != true || cleanup["last_success"] == nil || cleanup["files_deleted"] == nil || cleanup["files_deleted"].(float64) < 1 {
		t.Fatalf("cleanup status does not report the purge: %v", cleanup)
	}
}

// Files that a metadata restore at the recovery floor still references must
// survive history cleanup, even when the cleanup clock is far past the grace.
func TestDuckLakeS3SharedPostgresHostedHistoryCleanupKeepsRestoreFloor(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	fixture := nativeS3Fixture(t)
	dsn := sharedPostgresDSN(t)
	lake := lakeapi.DuckLakeConfig{PostgresURL: dsn, S3: &fixture.storage, Threads: 1, MemoryLimitMB: 128, MaxOpenProjects: 2,
		MaintenanceInterval: 24 * time.Hour, SnapshotRetention: time.Second, CleanupInterval: time.Second, CleanupGrace: 3 * time.Second,
		RestoreWindow: time.Second, RestoreMaxReaderTime: time.Second, RestoreSafetyMargin: time.Second}
	var floor atomic.Pointer[time.Time]
	config := Config{Profile: ProfileHosted, AcceptanceMode: AcceptanceModeBuffered, BootstrapToken: adminToken,
		Shared: &SharedConfig{DatabaseURL: dsn}, DuckLake: &lake,
		HostedRecoveryFloor: func(context.Context) (time.Time, error) {
			if at := floor.Load(); at != nil {
				return *at, nil
			}
			return time.Time{}, errors.New("restore point not recorded")
		}}
	s, host, _ := hostedCleanupWorker(t, config)
	project, _ := sharedJourneyProject(t, host.URL, defaultTenantID, "restore floor")
	neighbor, _ := sharedJourneyProject(t, host.URL, defaultTenantID, "restore floor neighbor")
	marker, kept := "floor-marker-"+eventapi.NewID(), "floor-neighbor-"+eventapi.NewID()
	deleted := hostedHistoryWrite(t, s, project, "deleted", marker)
	hostedHistoryWrite(t, s, project, "kept", "kept-"+eventapi.NewID())
	hostedHistoryWrite(t, s, neighbor, "neighbor", kept)
	prefix := fixture.projectPrefix(t, s.hosted.native, project)
	neighborPrefix := fixture.projectPrefix(t, s.hosted.native, neighbor)

	// A restore target after these writes needs every object listed now.
	target := time.Now()
	floor.Store(&target)
	atTarget := fixture.keys(t, prefix)
	_, editor := sharedJourneyKey(t, host.URL, project, []string{"events:read", "events:delete"})
	sharedJourneyHTTP(t, host.URL, http.MethodDelete, "/api/v1/projects/"+project+"/events/"+deleted, editor,
		[]byte(`{"expected_revision":"0","idempotency_key":"floor-delete"}`), nil, 202)
	if err := s.hosted.flusher.RecoverProject(t.Context(), lakeapi.ProjectWriteKey{SourceID: s.hosted.native.Config.SharedSourceID, ProjectID: project}); err != nil {
		t.Fatal(err)
	}
	clock := time.Now().Add(30 * 24 * time.Hour)
	passes := func() {
		t.Helper()
		for range 6 {
			clock = clock.Add(time.Hour)
			if err := s.hosted.native.CleanupHostedDue(t.Context(), clock, s.hosted.flusher.coordinator, s.cleanupHostedHistory); err != nil {
				t.Fatal(err)
			}
		}
	}
	progress := func() (expired, deleted int64) {
		t.Helper()
		if err := s.control.QueryRowContext(t.Context(), `SELECT snapshots_expired,files_deleted FROM pc_native_cleanup WHERE project_id=$1`, project).Scan(&expired, &deleted); err != nil {
			t.Fatal(err)
		}
		return expired, deleted
	}
	passes()
	if expired, removed := progress(); expired == 0 || removed != 0 {
		t.Fatalf("cleanup below the floor expired=%d deleted=%d", expired, removed)
	}
	remaining := fixture.keys(t, prefix)
	for _, key := range atTarget {
		if !slices.Contains(remaining, key) {
			t.Fatalf("cleanup deleted %s, which a restore at the recovery floor references", key)
		}
	}
	if len(fixture.objectsContaining(t, prefix, marker)) == 0 {
		t.Fatal("cleanup deleted the restore target's copy of the event")
	}

	// The schedule was written before PostgreSQL committed the expiry. A floor
	// between that schedule and the committed observation still protects it.
	var cutoff, observed time.Time
	if err := s.control.QueryRowContext(t.Context(), `SELECT file_cutoff,files_observed_at FROM pc_native_cleanup WHERE project_id=$1`, project).Scan(&cutoff, &observed); err != nil {
		t.Fatal(err)
	}
	if !cutoff.Before(observed) {
		t.Fatalf("the schedule cutoff %v did not precede its observation %v", cutoff, observed)
	}
	floor.Store(&cutoff)
	passes()
	if _, removed := progress(); removed != 0 {
		t.Fatalf("cleanup deleted %d files below the committed observation", removed)
	}

	advanced := time.Now()
	floor.Store(&advanced)
	passes()
	if _, removed := progress(); removed == 0 {
		t.Fatal("cleanup deleted no file after the floor advanced")
	}
	if found := fixture.objectsContaining(t, prefix, marker); len(found) != 0 {
		t.Fatalf("deleted event bytes remain after the floor advanced: %v", found)
	}
	pitrAssertEvent(t, host.URL, project, editor, "kept")
	pitrAssertEvent(t, host.URL, neighbor, adminToken, "neighbor")
	if len(fixture.objectsContaining(t, neighborPrefix, kept)) == 0 {
		t.Fatal("history cleanup removed the neighbor's live data")
	}

	// Without a recovery floor every pass refuses and reports the guard.
	floor.Store(nil)
	passes()
	var code string
	if err := s.control.QueryRowContext(t.Context(), `SELECT last_error FROM pc_native_cleanup WHERE project_id=$1`, project).Scan(&code); err != nil || code != "restore_guard" {
		t.Fatalf("guard refusal code=%q err=%v", code, err)
	}
}

func hostedHistoryWrite(t *testing.T, s *Service, project, name, marker string) string {
	t.Helper()
	at := time.Now().UTC()
	properties, err := json.Marshal(map[string]string{"marker": marker})
	if err != nil {
		t.Fatal(err)
	}
	event := eventapi.Event{ProjectID: project, UUID: eventapi.NewID(), Event: name, DistinctID: "actor",
		Properties: properties, Context: json.RawMessage(`{}`), Timestamp: at.Format(time.RFC3339Nano), ReceivedAt: at.Format(time.RFC3339Nano)}
	batch := acceptance.FlushBatch{ID: eventapi.NewID(), ProjectID: project, Events: []acceptance.BufferedEvent{{Input: acceptance.Input{Event: event}, AdmittedAt: at}}}
	if err := s.hosted.flusher.FlushLive(t.Context(), batch); err != nil {
		t.Fatal("commit hosted live event", err)
	}
	return event.UUID
}

// objectsContaining reads every live object under prefix.
func (f duckLakeS3Fixture) objectsContaining(t *testing.T, prefix, marker string) []string {
	t.Helper()
	var found []string
	for _, key := range f.keys(t, prefix) {
		ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
		object, err := f.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(f.storage.Bucket), Key: aws.String(key)})
		if err != nil {
			cancel()
			// Cleanup can remove a file between listing and reading it.
			var gone *types.NoSuchKey
			if errors.As(err, &gone) {
				continue
			}
			s3FixtureError(t, f, "read fixture object", err)
		}
		body, err := io.ReadAll(object.Body)
		object.Body.Close()
		cancel()
		s3FixtureError(t, f, "read fixture object body", err)
		if bytes.Contains(body, []byte(marker)) {
			found = append(found, key)
		}
	}
	slices.Sort(found)
	return found
}
