package porcupine

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const workerExportBody = `{"meta":[{"name":"uuid","type":"String"},{"name":"event","type":"String"}],"data":[["a","purchase"],["b","purchase"]],"rows":2,"totals":["all","2"],"extremes":{"min":["a","purchase"],"max":["b","purchase"]},"statistics":{"elapsed":0.1}}`

func bindExportCH(t *testing.T, s *Service, body string, kills *atomic.Int32) {
	t.Helper()
	c := testClickHouse(t, func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		if strings.HasSuffix(r.Header.Get("X-ClickHouse-User"), "_export") {
			io.WriteString(w, body)
			return
		}
		if strings.HasPrefix(string(data), "KILL QUERY") && kills != nil {
			kills.Add(1)
		}
	})
	configureAnalyticsTest(s, c)
}

func TestExportWorkerPublishesOnlyCompleteValidatedResult(t *testing.T) {
	fake := &exportTestS3{}
	s, job := exportWorkerFixture(t, fake)
	bindExportCH(t, s, workerExportBody, nil)
	s.runExport(context.Background(), job)
	current, err := s.loadExport(context.Background(), job.ProjectID, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.State != ExportSucceeded || current.Rows != 2 || current.SQL != "" || len(current.Parameters) != 0 || current.FileCount != 1 || current.ExpiresAtMS <= s.clock().UnixMilli() || len(current.Columns) != 2 {
		t.Fatalf("invalid successful job: %+v", current)
	}
	if !json.Valid(current.Totals) || !json.Valid(current.Extremes) || string(current.Totals) != `["all","2"]` {
		t.Fatal("query totals/extremes were not preserved")
	}
	artifacts, err := s.exportArtifacts(context.Background(), job.ID, job.AttemptID)
	if err != nil || len(artifacts) != 1 || !artifacts[0].Complete {
		t.Fatal("complete artifact missing", err)
	}
}

func TestExportWorkerLateHTTP200FailureCleansPreviouslyCompletedChunks(t *testing.T) {
	fake := &exportTestS3{}
	s, job := exportWorkerFixture(t, fake)
	s.exports.config.FileBytes = 1
	var kills atomic.Int32
	bindExportCH(t, s, `{"meta":[{"name":"uuid","type":"String"},{"name":"event","type":"String"}],"data":[["a","purchase"],["b","purchase"],["c","purchase"]]`, &kills)
	s.runExport(context.Background(), job)
	current, err := s.loadExport(context.Background(), job.ProjectID, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.State != ExportFailed || current.Error != "stream_invalid" || current.CleanupPending || current.SQL != "" {
		t.Fatalf("partial result became visible or was not cleaned: %+v", current)
	}
	artifacts, _ := s.exportArtifacts(context.Background(), job.ID, job.AttemptID)
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.completes < 2 || len(fake.objects) != 0 || len(artifacts) != 0 || kills.Load() == 0 {
		t.Fatal("late failure retained objects or did not kill server query")
	}
}

func TestExportWorkerCancellationAndRevocationWinPublication(t *testing.T) {
	for _, revoke := range []bool{false, true} {
		fake := &exportTestS3{}
		s, job := exportWorkerFixture(t, fake)
		bindExportCH(t, s, workerExportBody, nil)
		fake.onComplete = func() {
			var err error
			if revoke {
				_, err = s.db.Exec(`UPDATE access_keys SET revoked_at=? WHERE id=?`, now(), job.ActorID)
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
	fake := &exportTestS3{failComplete: true, failDelete: true}
	s, job := exportWorkerFixture(t, fake)
	bindExportCH(t, s, workerExportBody, nil)
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
	s, job := exportWorkerFixture(t, &exportTestS3{})
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
	if _, err = s.db.Exec(`UPDATE access_keys SET revoked_at=? WHERE id=?`, now(), job.ActorID); err != nil {
		t.Fatal(err)
	}
	if err = s.exportCanContinue(context.Background(), job); !errors.Is(err, errExportRevoked) {
		t.Fatal("worker ignored revoked creator", err)
	}
}

func TestExportWorkerRestartCleansAttemptBeforeNewClaim(t *testing.T) {
	fake := &exportTestS3{}
	s, job := exportWorkerFixture(t, fake)
	bindExportCH(t, s, workerExportBody, nil)
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
	s := service(t, 20)
	enableTestExports(s, 100)
	s.stopExports()
}

func TestExportCleanupKillFailurePreventsNewAttempt(t *testing.T) {
	fake := &exportTestS3{}
	s, job := exportWorkerFixture(t, fake)
	sink := newExportSink(context.Background(), s, job)
	sink.setColumns([]AnalyticsResultColumn{{Name: "value"}})
	if err := sink.writeRow([]json.RawMessage{json.RawMessage(`"prior attempt"`)}); err != nil {
		t.Fatal(err)
	}
	if err := sink.finish(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE export_jobs SET state='queued',cleanup_pending=1 WHERE id=?`, job.ID); err != nil {
		t.Fatal(err)
	}
	var killFails atomic.Bool
	killFails.Store(true)
	c := testClickHouse(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if strings.HasPrefix(string(body), "KILL QUERY") && killFails.Load() {
			w.WriteHeader(503)
		}
	})
	configureAnalyticsTest(s, c)
	current, _ := s.loadExport(context.Background(), job.ProjectID, job.ID)
	if err := s.cleanupExport(context.Background(), current); err == nil {
		t.Fatal("failed query cancellation was ignored")
	}
	if _, ok := s.nextQueuedExport(context.Background()); ok {
		t.Fatal("new attempt admitted while old query might remain active")
	}
	artifacts, _ := s.exportArtifacts(context.Background(), job.ID, job.AttemptID)
	if len(artifacts) != 1 {
		t.Fatal("query cancellation failure lost cleanup metadata")
	}
	killFails.Store(false)
	if err := s.cleanupExport(context.Background(), current); err != nil {
		t.Fatal(err)
	}
	queued, ok := s.nextQueuedExport(context.Background())
	if !ok {
		t.Fatal("cleanup did not unblock retry")
	}
	next, err := s.claimExport(context.Background(), queued)
	if err != nil || next.AttemptID == job.AttemptID || next.QueryID == job.QueryID {
		t.Fatal("retry reused old attempt or query identity", err)
	}
}

func TestExportWorkerShutdownRequeuesWithoutPublishing(t *testing.T) {
	s, job := exportWorkerFixture(t, &exportTestS3{})
	started := make(chan struct{})
	c := testClickHouse(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.Header.Get("X-ClickHouse-User"), "_export") {
			io.Copy(io.Discard, r.Body)
			close(started)
			<-r.Context().Done()
		}
	})
	configureAnalyticsTest(s, c)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); s.runExport(ctx, job) }()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		cancel()
		t.Fatal("export did not start")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown did not cancel active query")
	}
	current, err := s.loadExport(context.Background(), job.ProjectID, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.State != ExportQueued || current.Error != "interrupted" || current.SQL == "" || current.CleanupPending {
		t.Fatalf("shutdown lost resumable job or published partial result: %+v", current)
	}
}

func TestExportWorkerRevalidatesPersistedSQL(t *testing.T) {
	s, job := exportWorkerFixture(t, &exportTestS3{})
	bindExportCH(t, s, workerExportBody, nil)
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
