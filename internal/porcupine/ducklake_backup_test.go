//go:build ducklake

package porcupine

import (
	"context"
	"database/sql"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func nativeBackupService(t *testing.T) (*Service, Config) {
	t.Helper()
	root := t.TempDir()
	config := Config{DBPath: filepath.Join(root, "live", "queue.db"), BootstrapToken: adminToken, DuckLake: &DuckLakeConfig{Directory: filepath.Join(root, "live", "lake"), MemoryLimitMB: 256, Threads: 1, MaintenanceInterval: 24 * time.Hour}, NativeBackup: &NativeBackupConfig{Directory: filepath.Join(root, "backups"), Retain: 1}}
	s, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	s.stopNativeBackups()
	t.Cleanup(func() { s.Close() })
	return s, config
}

func TestDuckLakeBackupRecoveryLifecycleAndAgentStatus(t *testing.T) {
	s, config := nativeBackupService(t)
	a, ingest := project(t, s)
	b, _ := project(t, s)
	_, reader := key(t, s, []string{"events:read", "management:read"}, []string{a})
	revokedID, revoked := key(t, s, []string{"events:read"}, []string{a})
	request(t, s, "DELETE", "/api/v1/access-keys/"+revokedID, adminToken, "", 204)
	lifecycle := lakeMaintenanceLifecycle(t, s, a, ingest, "backup", true)
	s.deliveryCancel()
	<-s.deliveryDone
	pending := `{"type":"track","message_id":"backup-pending","actor_id":"pending-user","name":"pending"}`
	pendingID := receiptFrom(t, nativeRequest(t, s, ingest, pending, 200), 0)["event_id"]
	ctx := context.Background()
	if err := s.createNativeBackup(ctx); err != nil {
		t.Fatal(err)
	}
	status := s.nativeBackupStatus()
	bundle := filepath.Join(config.NativeBackup.Directory, status.LatestBundle)
	manifest, err := verifyNativeBackup(ctx, bundle)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.CapturedAt != status.LastCheckpointAt || manifest.CapturedAt == "" || manifest.SQLitePath != config.DBPath || manifest.SourceID == "" || len(manifest.Files) < 3 {
		t.Fatalf("invalid native recovery manifest: %+v", manifest)
	}
	admin := request(t, s, "GET", "/api/v1/status", adminToken, "", 200)
	if admin["native_backup"].(map[string]any)["state"] != "ok" {
		t.Fatal("admin missing backup status")
	}
	scoped := request(t, s, "GET", "/api/v1/status", reader, "", 200)
	if scoped["native_backup"] != nil {
		t.Fatal("project reader saw backup policy/progress")
	}
	host := httptest.NewServer(s.Handler())
	mcpctx, session := mcpTestSession(t, host.URL, adminToken, "2025-11-25")
	result := mcpTestCall(t, mcpctx, session, mcpCallInput{OperationID: "get-instance-status"}, 200)
	if result["native_backup"] == nil {
		t.Fatal("MCP missing backup status")
	}
	scopedctx, scopedSession := mcpTestSession(t, host.URL, reader, "2025-11-25")
	scopedResult := mcpTestCall(t, scopedctx, scopedSession, mcpCallInput{OperationID: "get-instance-status"}, 200)
	if scopedResult["native_backup"] != nil {
		t.Fatal("project-scoped MCP exposed backup status")
	}

	host.Close()
	// Recovery must depend on the bundle, not the live analytical directory.
	if err := s.lake.DeleteProject(ctx, a); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Dir(config.DBPath)); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(config.DBPath), 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := verifyNativeBackup(ctx, bundle); err != nil {
		t.Fatal("native source deletion damaged completed bundle", err)
	}
	if err := copyCheckpointFile(ctx, filepath.Join(bundle, "acceptance.sqlite"), config.DBPath); err != nil {
		t.Fatal(err)
	}
	if err := os.CopyFS(config.DuckLake.Directory, os.DirFS(filepath.Join(bundle, "ducklake"))); err != nil {
		t.Fatal(err)
	}
	config.NativeBackup = nil
	restored, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	duckLakeJourneyDrain(t, restored)
	lakeMaintenanceVerify(t, restored, a, lifecycle)
	rows := duckLakeJourneyQuery(t, restored, a, reader, "SELECT event_id FROM events WHERE event='pending'", nil)
	if len(rows) != 1 || rows[0].(map[string]any)["event_id"] != pendingID {
		t.Fatal("restored pending event lost its ID", rows)
	}
	retry := nativeRequest(t, restored, ingest, lakeMaintenancePages("backup"), 200)
	if retry["duplicates"] != float64(2) {
		t.Fatal("restored event retry resurrected or replaced event")
	}
	request(t, restored, "POST", "/api/v1/projects/"+b+"/query", reader, `{"sql":"SELECT count(*) FROM events"}`, 403)
	request(t, restored, "POST", "/api/v1/projects/"+a+"/query", revoked, `{"sql":"SELECT count(*) FROM events"}`, 401)
}

func TestDuckLakeBackupFailureRetentionAndVerification(t *testing.T) {
	s, config := nativeBackupService(t)
	a, _ := project(t, s)
	ctx := context.Background()
	if err := s.createNativeBackup(ctx); err != nil {
		t.Fatal(err)
	}
	first := s.nativeBackupStatus().LatestBundle
	if err := s.createNativeBackup(ctx); err != nil {
		t.Fatal(err)
	}
	second := s.nativeBackupStatus().LatestBundle
	if first == second {
		t.Fatal("backup ID reused")
	}
	if _, err := os.Stat(filepath.Join(config.NativeBackup.Directory, first)); !os.IsNotExist(err) {
		t.Fatal("old completed backup not pruned", err)
	}
	bundle := filepath.Join(config.NativeBackup.Directory, second)
	_, release, err := s.lake.(*duckLakeStore).leaseProject(ctx, a)
	if err != nil {
		t.Fatal(err)
	}
	deadline, cancel := context.WithTimeout(ctx, 25*time.Millisecond)
	err = s.createNativeBackup(deadline)
	cancel()
	release()
	if err == nil {
		t.Fatal("backup ignored active reader/deadline")
	}
	if _, err := verifyNativeBackup(ctx, bundle); err != nil {
		t.Fatal("failed backup damaged last good bundle", err)
	}
	if status := s.nativeBackupStatus(); status.State != "failed" || status.LatestBundle != second {
		t.Fatalf("failed backup status: %+v", status)
	}
	admin := request(t, s, "GET", "/api/v1/status", adminToken, "", 200)
	if admin["status"] != "degraded" {
		t.Fatal("backup failure not visible to administrator")
	}
	if err := s.createNativeBackup(ctx); err != nil {
		t.Fatal("backup did not recover", err)
	}
	bundle = filepath.Join(config.NativeBackup.Directory, s.nativeBackupStatus().LatestBundle)
	// Hashes detect corruption before a recovery target is changed.
	path := filepath.Join(bundle, "acceptance.sqlite")
	if err := os.WriteFile(path, []byte("corrupted"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := verifyNativeBackup(ctx, bundle); err == nil {
		t.Fatal("corrupt backup accepted")
	}
}

func TestDuckLakeBackupAutomaticScheduleAndShutdown(t *testing.T) {
	root := t.TempDir()
	config := Config{DBPath: filepath.Join(root, "live", "queue.db"), BootstrapToken: adminToken, DuckLake: &DuckLakeConfig{Directory: filepath.Join(root, "live", "lake"), MemoryLimitMB: 128, Threads: 1}, NativeBackup: &NativeBackupConfig{Directory: filepath.Join(root, "backups")}}
	s, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	deadline := time.Now().Add(10 * time.Second)
	for {
		status := s.nativeBackupStatus()
		if status.State == "ok" {
			if _, err := verifyNativeBackup(context.Background(), filepath.Join(config.NativeBackup.Directory, status.LatestBundle)); err != nil {
				t.Fatal(err)
			}
			break
		}
		if status.State == "failed" || time.Now().After(deadline) {
			t.Fatalf("scheduled backup did not succeed: %+v", status)
		}
		time.Sleep(20 * time.Millisecond)
	}
	s.stopNativeBackups()
	stages, err := filepath.Glob(filepath.Join(config.DuckLake.Directory, ".checkpoint-*"))
	if err != nil || len(stages) != 0 {
		t.Fatal("completed automatic backup retained its stage", stages, err)
	}
	entries, err := os.ReadDir(config.NativeBackup.Directory)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() && strings.HasPrefix(entry.Name(), ".") {
			t.Fatal("automatic backup left incomplete directory", entry.Name())
		}
	}
}

type pausedCheckpointStore struct {
	lakeStore
	captured chan struct{}
	resume   chan struct{}
}

func (s *pausedCheckpointStore) stageCheckpoint(ctx context.Context, acceptance func(context.Context, string) error) (string, error) {
	return s.lakeStore.stageCheckpoint(ctx, func(ctx context.Context, path string) error {
		if err := acceptance(ctx, path); err != nil {
			return err
		}
		close(s.captured)
		select {
		case <-s.resume:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
}

func TestDuckLakeBackupKeepsAcceptanceAndQueriesAvailable(t *testing.T) {
	s, config := nativeBackupService(t)
	s.deliveryCancel()
	<-s.deliveryDone
	a, ingest := project(t, s)
	nativeRequest(t, s, ingest, `{"type":"track","message_id":"before-checkpoint","actor_id":"user","name":"before"}`, 200)
	duckLakeJourneyDrain(t, s)
	observer := &pausedCheckpointStore{lakeStore: s.lake, captured: make(chan struct{}), resume: make(chan struct{})}
	s.lake = observer
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	finished := make(chan struct{})
	defer func() { cancel(); <-finished }()
	go func() { defer close(finished); done <- s.createNativeBackup(ctx) }()
	select {
	case <-observer.captured:
	case <-ctx.Done():
		t.Fatal("backup did not capture SQLite")
	}
	nativeRequest(t, s, ingest, `{"type":"track","message_id":"after-checkpoint","actor_id":"user","name":"after"}`, 200)
	rows := duckLakeJourneyQuery(t, s, a, adminToken, "SELECT count(*) AS total FROM events", nil)
	if rows[0].(map[string]any)["total"] != "1" {
		t.Fatal("queries did not continue during SQLite checkpoint")
	}
	blocked, stop := context.WithTimeout(ctx, 20*time.Millisecond)
	if _, err := s.deliverBatch(blocked); err == nil {
		t.Fatal("native delivery bypassed the exclusive checkpoint guard")
	}
	stop()
	close(observer.resume)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	bundle := filepath.Join(config.NativeBackup.Directory, s.nativeBackupStatus().LatestBundle)
	manifest, err := verifyNativeBackup(ctx, bundle)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.ID == "" {
		t.Fatal("missing completed manifest")
	}
	database, err := sql.Open("sqlite", filepath.Join(bundle, "acceptance.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var queued int
	if err := database.QueryRow("SELECT count(*) FROM events").Scan(&queued); err != nil || queued != 0 {
		t.Fatal("checkpoint included an event accepted after its SQLite snapshot", queued, err)
	}
	duckLakeJourneyDrain(t, s)
	rows = duckLakeJourneyQuery(t, s, a, adminToken, "SELECT count(*) AS total FROM events", nil)
	if rows[0].(map[string]any)["total"] != "2" {
		t.Fatal("live event accepted during checkpoint was lost")
	}
}

func TestDuckLakeBackupReusesImmutableFilesAcrossRetention(t *testing.T) {
	s, config := nativeBackupService(t)
	a, ingest := project(t, s)
	nativeRequest(t, s, ingest, `{"type":"track","message_id":"reuse-parquet","actor_id":"user","name":"kept"}`, 200)
	duckLakeJourneyDrain(t, s)
	flushCleanupProject(t, s.lake.(*duckLakeStore), a)
	ctx := context.Background()
	if err := s.createNativeBackup(ctx); err != nil {
		t.Fatal(err)
	}
	first := s.nativeBackupStatus().LatestBundle
	manifest := readNativeManifest(t, filepath.Join(config.NativeBackup.Directory, first))
	var relative string
	for _, file := range manifest.Files {
		if strings.Contains(file.Path, "/files/") && strings.HasSuffix(file.Path, ".parquet") {
			relative = file.Path
			break
		}
	}
	if relative == "" {
		t.Fatal("fixture did not produce a Parquet file")
	}
	original, err := os.Stat(filepath.Join(config.NativeBackup.Directory, first, filepath.FromSlash(relative)))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.createNativeBackup(ctx); err != nil {
		t.Fatal(err)
	}
	second := s.nativeBackupStatus().LatestBundle
	reused, err := os.Stat(filepath.Join(config.NativeBackup.Directory, second, filepath.FromSlash(relative)))
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(original, reused) {
		t.Fatal("unchanged Parquet was duplicated instead of shared")
	}
	if _, err := os.Stat(filepath.Join(config.NativeBackup.Directory, first)); !os.IsNotExist(err) {
		t.Fatal("expired bundle not removed")
	}
	if _, err := verifyNativeBackup(ctx, filepath.Join(config.NativeBackup.Directory, second)); err != nil {
		t.Fatal("retention removed reused data", err)
	}
}
