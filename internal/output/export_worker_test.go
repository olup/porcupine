package output

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	lakeapi "github.com/olup/porcupine/internal/lake"
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
	var published int64
	if err = s.db.QueryRow(`SELECT COALESCE(SUM(export_bytes),0) FROM usage_rollups WHERE project_id=?`, job.ProjectID).Scan(&published); err != nil || published != current.Bytes {
		t.Fatal("published compressed bytes not metered exactly once", published, current.Bytes, err)
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
		var published int64
		if err = s.db.QueryRow(`SELECT COALESCE(SUM(export_bytes),0) FROM usage_rollups WHERE project_id=?`, job.ProjectID).Scan(&published); err != nil || published != 0 {
			t.Fatal("canceled or revoked output was metered", published, err)
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
	if _, err := s.db.Exec(`INSERT INTO projects(id,tenant_id,name,created_at) VALUES('other','default','other','now'); INSERT INTO events(project_id,uuid,event,distinct_id,properties,timestamp,received_at) VALUES('other','pressure','purchase','u','{}','now','now')`); err != nil {
		t.Fatal(err)
	}
	ready, err := s.exportAdmission(context.Background(), job)
	if err != nil || !ready {
		t.Fatal("unrelated low pending counts blocked completed project barrier", err)
	}
	s.config.QueueThreshold = 0
	ready, err = s.exportAdmission(context.Background(), job)
	if err != nil || ready {
		t.Fatal("global delivery pressure ignored", err)
	}
	s.config.QueueThreshold = 1000
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
	if err = s.exportCanContinue(context.Background(), job); !errors.Is(err, ErrExportRevoked) {
		t.Fatal("worker ignored revoked creator", err)
	}
}

func TestExportWorkerDefersClaimUntilDeliveryAndCapacityAreReady(t *testing.T) {
	t.Parallel()
	s := newExportWorkerFixture(t, &exportTestS3{})
	ctx := t.Context()
	if _, err := s.db.Exec(`INSERT INTO events(project_id,uuid,event,distinct_id,properties,timestamp,received_at) VALUES('project','pending','purchase','u','{}','now','now')`); err != nil {
		t.Fatal(err)
	}
	created, err := s.Submit(ctx, "project", ExportRequest{IdempotencyKey: "admission", SQL: "SELECT uuid FROM events"}, fixtureActor())
	if err != nil {
		t.Fatal(err)
	}

	available := false
	var acquisitions, releases int
	s.acquire = func(context.Context) (func(), bool) {
		acquisitions++
		if !available {
			return nil, false
		}
		return func() { releases++ }, true
	}
	checkDeferred := func(wantAcquisitions int) {
		t.Helper()
		worked, err := s.RunOne(ctx)
		if err != nil || worked {
			t.Fatalf("worker progressed before admission: worked=%v, err=%v", worked, err)
		}
		if acquisitions != wantAcquisitions || releases != 0 {
			t.Fatalf("attempted %d permit acquisitions and %d releases, want %d acquisitions and no releases", acquisitions, releases, wantAcquisitions)
		}
		job, err := s.loadExport(ctx, "project", created.ID)
		if err != nil || job.State != ExportQueued || job.AttemptID != "" || job.Attempts != 0 || job.StartedAt != "" {
			t.Fatalf("deferral changed the queued job: %+v, err=%v", job, err)
		}
	}

	checkDeferred(0)
	applyExportEvent(t, s, "project", "pending", "short")
	if _, err := s.db.Exec(`DELETE FROM events WHERE project_id='project' AND uuid='pending'`); err != nil {
		t.Fatal(err)
	}
	checkDeferred(1)

	available = true
	if worked, err := s.RunOne(ctx); err != nil || !worked {
		t.Fatalf("ready export did not run: worked=%v, err=%v", worked, err)
	}
	job, err := s.loadExport(ctx, "project", created.ID)
	if err != nil || job.State != ExportSucceeded || job.Rows != 1 || job.Attempts != 1 {
		t.Fatalf("ready export did not succeed on its first attempt: %+v, err=%v", job, err)
	}
	if acquisitions != 2 || releases != 1 {
		t.Fatalf("completed export used %d acquisitions and %d releases, want 2 acquisitions and 1 release", acquisitions, releases)
	}
}

func TestExportWorkerRestartCleansAttemptBeforeNewClaim(t *testing.T) {
	t.Parallel()
	fake := &exportTestS3{}
	s, job := nativeExportWorkerFixture(t, fake)
	sink := newExportSink(context.Background(), s, job)
	sink.setColumns([]lakeapi.AnalyticsResultColumn{{Name: "uuid"}})
	if err := sink.writeRow([]json.RawMessage{json.RawMessage(`"partial"`)}); err != nil {
		t.Fatal(err)
	}
	if err := sink.completeFile(); err != nil {
		t.Fatal(err)
	}
	// The existing persisted running attempt models a process dying after a chunk completed.
	s.Start()
	deadline := time.Now().Add(4 * time.Second)
	var current storedExport
	for time.Now().Before(deadline) {
		current, _ = s.loadExport(context.Background(), job.ProjectID, job.ID)
		if current.State == ExportSucceeded {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	s.Stop()
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
	s, _ := exportWorkerFixture(t, &exportTestS3{})
	s.Stop()
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
	for _, row := range []struct{ id, padding string }{{"a", "short"}, {"b", "short"}, {"c", "short"}, {"d", strings.Repeat("x", 1024)}} {
		applyExportEvent(t, s, job.ProjectID, row.id, row.padding)
	}

	s.config.FileBytes = 1
	s.config.MaxRowBytes = 128
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

func TestExportJobsIsolatedSuccessTransactionRollbackAndReplay(t *testing.T) {
	t.Parallel()
	s, job := nativeExportWorkerFixture(t, &exportTestS3{})
	ctx := t.Context()
	if _, err := s.db.Exec(`CREATE TRIGGER reject_usage BEFORE INSERT ON usage_rollups BEGIN SELECT RAISE(ABORT,'usage unavailable'); END`); err != nil {
		t.Fatal(err)
	}
	s.runExport(ctx, job)
	current, err := s.loadExport(ctx, job.ProjectID, job.ID)
	if err != nil || current.State != ExportQueued || current.Bytes == 0 {
		t.Fatal("failed usage transaction published success or lost attempt", current, err)
	}
	var bytes int64
	if err := s.db.QueryRow(`SELECT coalesce(sum(export_bytes),0) FROM usage_rollups`).Scan(&bytes); err != nil || bytes != 0 {
		t.Fatal("rolled back success was metered", bytes, err)
	}
	if _, err := s.db.Exec(`DROP TRIGGER reject_usage`); err != nil {
		t.Fatal(err)
	}
	worked, err := s.RunOne(ctx)
	if err != nil || !worked {
		t.Fatal("retry did not execute", worked, err)
	}
	current, err = s.loadExport(ctx, job.ProjectID, job.ID)
	if err != nil || current.State != ExportSucceeded {
		t.Fatal("retry failed", current, err)
	}
	if err := s.db.QueryRow(`SELECT coalesce(sum(export_bytes),0) FROM usage_rollups`).Scan(&bytes); err != nil || bytes != current.Bytes {
		t.Fatal("winning output metered incorrectly", bytes, current.Bytes, err)
	}
	// A repeated publication must lose the same state predicate before AddTx.
	sink := &exportSink{outputSink: &outputSink{compressed: current.Bytes}}
	if err := s.publishExport(ctx, current, lakeapi.ExportStreamResult{}, sink); !errors.Is(err, errExportCanceled) {
		t.Fatal("terminal publication replay won", err)
	}
	var replayed int64
	if err := s.db.QueryRow(`SELECT coalesce(sum(export_bytes),0) FROM usage_rollups`).Scan(&replayed); err != nil || replayed != bytes {
		t.Fatal("publication replay was metered", replayed, bytes, err)
	}
}

func TestExportJobsCancelCommitsBeforeSignalingActiveAttempt(t *testing.T) {
	t.Parallel()
	s, job := exportWorkerFixture(t, &exportTestS3{})
	called := false
	s.activeJob = job.ID
	s.activeCancel = func() {
		called = true
		var requested, cleanup bool
		if err := s.db.QueryRow(`SELECT cancel_requested,cleanup_pending FROM export_jobs WHERE id=?`, job.ID).Scan(&requested, &cleanup); err != nil || !requested || !cleanup {
			t.Error("active attempt signaled before durable cancellation", err)
		}
	}
	if _, err := s.Cancel(t.Context(), job.ProjectID, job.ID, fixtureActor()); err != nil || !called {
		t.Fatal("creator cancellation did not signal", called, err)
	}
}

func TestExportJobsFiniteExecutionOutageRetainsOriginalExpiry(t *testing.T) {
	t.Parallel()
	s, job := exportWorkerFixture(t, &exportTestS3{})
	at := s.clock()
	s.issuer = "https://issuer.example"
	job.ExternalTenantID = "default"
	job.ExternalIssuer = s.issuer
	job.ExternalExpiresAtMS = at.Add(5 * time.Minute).UnixMilli()
	if _, err := s.db.Exec(`INSERT INTO export_external_authority(job_id,tenant_id,issuer,expires_at_ms) VALUES(?,?,?,?)`, job.ID, job.ExternalTenantID, job.ExternalIssuer, job.ExternalExpiresAtMS); err != nil {
		t.Fatal(err)
	}
	if _, err := s.exportJobActor(t.Context(), job); err != nil {
		t.Fatal("valid private authority rejected", err)
	}
	if err := s.control.Close(); err != nil {
		t.Fatal(err)
	}
	_, err := s.exportJobActor(t.Context(), job)
	if !authenticationUnavailable(err) {
		t.Fatal("control outage became revoked authority", err)
	}
	s.failExport(t.Context(), job, err)
	current, err := s.loadExport(t.Context(), job.ProjectID, job.ID)
	if err != nil || current.State != ExportQueued || current.Error != "authorization_unavailable" || current.SQL == "" {
		t.Fatal("outage destroyed running execution intent", current, err)
	}
	s.clock = func() time.Time { return at.Add(6 * time.Minute) }
	if _, err := s.exportJobActor(t.Context(), job); !errors.Is(err, errExportAuthorizationExpired) {
		t.Fatal("outage extended finite execution authority", err)
	}
}
