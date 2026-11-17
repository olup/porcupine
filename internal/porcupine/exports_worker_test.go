package porcupine

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestExportWorkerPublishesOnlyCompleteValidatedResult(t *testing.T) {
	t.Parallel()
	fake := &exportTestS3{}
	s, job := nativeExportWorkerFixture(t, fake)
	s.runExport(context.Background(), job)
	current, err := s.loadExport(context.Background(), job.ProjectID, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.State != ExportSucceeded || current.Rows != 1 || current.SQL != "" || len(current.Parameters) != 0 || current.FileCount != 1 || current.ExpiresAtMS <= s.clock().UnixMilli() || len(current.Columns) != 3 {
		t.Fatalf("invalid successful job: %+v", current)
	}
	artifacts, err := s.exportArtifacts(context.Background(), job.ID, job.AttemptID)
	if err != nil || len(artifacts) != 1 || !artifacts[0].Complete {
		t.Fatal("complete artifact missing", err)
	}
}

func TestExportWorkerCancellationAndRevocationWinPublication(t *testing.T) {
	t.Parallel()
	for _, revoke := range []bool{false, true} {
		fake := &exportTestS3{}
		s, job := nativeExportWorkerFixture(t, fake)
		fake.onComplete = func() {
			var err error
			if revoke {
				_, err = s.control.Exec(`UPDATE access_keys SET revoked_at=? WHERE id=?`, now(), job.ActorID)
			} else {
				_, err = s.db.Exec(`UPDATE export_jobs SET cancel_requested=1 WHERE id=?`, job.ID)
			}
			if err != nil {
				t.Error(err)
			}
		}
		s.runExport(context.Background(), job)
		current, err := s.loadExport(context.Background(), job.ProjectID, job.ID)
		if err != nil {
			t.Fatal(err)
		}
		if current.State != ExportCanceled || current.CleanupPending {
			t.Fatalf("cancellation/revocation lost publication race: %+v", current)
		}
		fake.mu.Lock()
		if len(fake.objects) != 0 {
			t.Error("canceled export retained published object")
		}
		fake.mu.Unlock()
	}
}

func TestExportCleanupDeletesObjectAfterUnknownCompletionAndRetriesFailures(t *testing.T) {
	t.Parallel()
	fake := &exportTestS3{failComplete: true, failDelete: true}
	s, job := nativeExportWorkerFixture(t, fake)
	s.runExport(context.Background(), job)
	current, err := s.loadExport(context.Background(), job.ProjectID, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.State != ExportQueued || !current.CleanupPending {
		t.Fatalf("unknown completion was not retained for cleanup: %+v", current)
	}
	artifacts, _ := s.exportArtifacts(context.Background(), job.ID, job.AttemptID)
	fake.mu.Lock()
	if len(fake.objects) != 1 || len(artifacts) != 1 || artifacts[0].Complete {
		t.Error("test did not leave an uncertain committed object")
	}
	fake.failDelete = false
	fake.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err = s.cleanupExport(ctx, current); err != nil {
		t.Fatal(err)
	}
	current, _ = s.loadExport(context.Background(), job.ProjectID, job.ID)
	artifacts, _ = s.exportArtifacts(context.Background(), job.ID, job.AttemptID)
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if current.CleanupPending || len(artifacts) != 0 || len(fake.objects) != 0 || fake.aborts < 1 || fake.deletes < 2 {
		t.Fatal("durable cleanup did not converge")
	}
}

func TestExportWorkerAdmissionBarrierPressureAndRevokedCreator(t *testing.T) {
	t.Parallel()
	s, job := nativeExportWorkerFixture(t, &exportTestS3{})
	request(t, s, "POST", "/capture/", "", captureBody(func() string { _, token := project(t, s); return token }(), "other-project-pressure"), 200)
	ready, err := s.exportAdmission(context.Background(), job)
	if err != nil || !ready {
		t.Fatal("unrelated low pending counts blocked completed project barrier", err)
	}
	s.exports.config.QueueThreshold = 0
	ready, err = s.exportAdmission(context.Background(), job)
	if err != nil || ready {
		t.Fatal("global delivery pressure ignored", err)
	}
	s.exports.config.QueueThreshold = 1000
	if _, err = s.db.Exec(`INSERT INTO metadata(key,value) VALUES('last_delivery_error','private message')`); err != nil {
		t.Fatal(err)
	}
	ready, err = s.exportAdmission(context.Background(), job)
	if err != nil || ready {
		t.Fatal("delivery failure ignored", err)
	}
	if err = s.exportCanContinue(context.Background(), job); !errors.Is(err, errExportPressure) {
		t.Fatal("active worker ignored delivery error", err)
	}
	if _, err = s.db.Exec(`DELETE FROM metadata WHERE key='last_delivery_error'`); err != nil {
		t.Fatal(err)
	}
	if _, err = s.control.Exec(`UPDATE access_keys SET revoked_at=? WHERE id=?`, now(), job.ActorID); err != nil {
		t.Fatal(err)
	}
	if err = s.exportCanContinue(context.Background(), job); !errors.Is(err, errExportRevoked) {
		t.Fatal("worker ignored revoked creator", err)
	}
}

func TestExportWorkerRestartCleansAttemptBeforeNewClaim(t *testing.T) {
	t.Parallel()
	fake := &exportTestS3{}
	s, job := nativeExportWorkerFixture(t, fake)
	sink := newExportSink(context.Background(), s, job)
	sink.setColumns([]AnalyticsResultColumn{{Name: "uuid"}})
	if err := sink.writeRow([]json.RawMessage{json.RawMessage(`"partial"`)}); err != nil {
		t.Fatal(err)
	}
	if err := sink.completeFile(); err != nil {
		t.Fatal(err)
	}
	// The existing persisted running attempt models a process dying after a chunk completed.
	s.startExports()
	deadline := time.Now().Add(4 * time.Second)
	var current storedExport
	for time.Now().Before(deadline) {
		current, _ = s.loadExport(context.Background(), job.ProjectID, job.ID)
		if current.State == ExportSucceeded {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	s.stopExports()
	if current.State != ExportSucceeded || current.AttemptID == job.AttemptID || current.Attempts != 2 {
		t.Fatalf("restart did not retry from a fresh attempt: %+v", current)
	}
	old, _ := s.exportArtifacts(context.Background(), job.ID, job.AttemptID)
	if len(old) != 0 {
		t.Fatal("restart retained prior attempt metadata")
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.objects) != 1 || fake.deletes < 1 {
		t.Fatal("restart retained prior attempt data")
	}
}

func TestExportWorkerStopWithoutStartedRuntime(t *testing.T) {
	t.Parallel()
	s := service(t, 20)
	enableTestExports(t, s, 100)
	s.stopExports()
}

func TestExportWorkerRevalidatesPersistedSQL(t *testing.T) {
	t.Parallel()
	s, job := nativeExportWorkerFixture(t, &exportTestS3{})
	job.SQL = "SELECT password FROM system.users"
	if _, err := s.db.Exec(`UPDATE export_jobs SET sql=? WHERE id=?`, job.SQL, job.ID); err != nil {
		t.Fatal(err)
	}
	s.runExport(context.Background(), job)
	current, err := s.loadExport(context.Background(), job.ProjectID, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.State != ExportFailed || current.Error != "stream_invalid" || current.SQL != "" {
		t.Fatal("persisted SQL bypassed current query policy")
	}
}

func TestExportWorkerNativeRowFailureCleansCompletedChunks(t *testing.T) {
	t.Parallel()
	fake := &exportTestS3{}
	s, job := nativeExportWorkerFixture(t, fake)
	_, writer := key(t, s, []string{"events:write"}, []string{job.ProjectID})
	for _, row := range []struct{ id, padding string }{{"a", "short"}, {"b", "short"}, {"c", "short"}, {"d", strings.Repeat("x", 1024)}} {
		raw, _ := json.Marshal(map[string]any{"api_key": writer, "uuid": row.id, "event": "purchase", "distinct_id": "u", "properties": map[string]any{"padding": row.padding}})
		request(t, s, "POST", "/capture/", "", string(raw), 200)
	}
	duckLakeJourneyDrain(t, s)
	s.exports.config.FileBytes = 1
	s.exports.config.MaxRowBytes = 128
	job.SQL = "SELECT uuid,properties FROM events ORDER BY uuid"
	job.Parameters = nil
	s.runExport(context.Background(), job)
	current, err := s.loadExport(context.Background(), job.ProjectID, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.State != ExportFailed || current.Error != "row_limit" || current.CleanupPending {
		t.Fatalf("failed native stream published: %+v", current)
	}
	artifacts, _ := s.exportArtifacts(context.Background(), job.ID, job.AttemptID)
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.completes < 2 || len(fake.objects) != 0 || len(artifacts) != 0 {
		t.Fatal("native stream failure retained completed chunks")
	}
}

func TestExportWorkerShutdownRequeuesWithoutPublishing(t *testing.T) {
	t.Parallel()
	fake := &exportTestS3{}
	s, job := nativeExportWorkerFixture(t, fake)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	var once sync.Once
	fake.onComplete = func() { once.Do(func() { close(started) }); <-ctx.Done() }
	done := make(chan struct{})
	go func() { defer close(done); s.runExport(ctx, job) }()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		cancel()
		t.Fatal("native export did not reach upload")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown did not cancel native export")
	}
	current, err := s.loadExport(context.Background(), job.ProjectID, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.State != ExportQueued || current.Error != "interrupted" || current.SQL == "" || current.CleanupPending {
		t.Fatalf("shutdown lost resume state: %+v", current)
	}
}
