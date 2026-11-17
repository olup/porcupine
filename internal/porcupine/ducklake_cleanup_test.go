package porcupine

import (
	"context"
	"database/sql"
	"io/fs"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func cleanupParquetFiles(t *testing.T, directory string) []string {
	t.Helper()
	var paths []string
	if err := filepath.WalkDir(directory, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(path, ".parquet") {
			paths = append(paths, path)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return paths
}

// Inspect decoded Parquet rows, not compressed file bytes, to prove that the
// deleted payload is no longer in any managed data file.
func assertCleanupPayloadAbsent(t *testing.T, directory, marker string) {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	files := cleanupParquetFiles(t, directory)
	if len(files) == 0 {
		t.Fatal("expected surviving Parquet data")
	}
	for _, path := range files {
		var found int
		if err := db.QueryRow("SELECT count(*) FROM read_parquet(?) t WHERE contains(to_json(t)::VARCHAR,?)", path, marker).Scan(&found); err != nil {
			t.Fatal(err)
		}
		if found != 0 {
			t.Fatalf("deleted payload still in %s", filepath.Base(path))
		}
	}
}

func cleanupEventFiles(t *testing.T, s *duckLakeStore, id string) []string {
	t.Helper()
	p, release, err := s.LeaseProject(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	rows, err := p.DB.Query("SELECT data_file FROM ducklake_list_files('lake','current_events')")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var paths []string
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			t.Fatal(err)
		}
		paths = append(paths, path)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatal("expected event data file")
	}
	return paths
}

func flushCleanupProject(t *testing.T, s *duckLakeStore, id string) {
	t.Helper()
	p, release, err := s.LeaseProject(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if err := p.MaintenanceCommand(context.Background(), "CALL ducklake_flush_inlined_data('lake')", &duckLakeMaintenanceStats{}); err != nil {
		t.Fatal(err)
	}
}

func TestDuckLakeCleanupStatusAgentPermissionsAndLifecycle(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	s, a, ingest := maintenanceOpenService(t)
	b, otherIngest := project(t, s)
	_, reader := key(t, s, []string{"management:read"}, []string{a})
	state := lakeMaintenanceLifecycle(t, s, a, ingest, "cleanup", true)
	nativeRequest(t, s, otherIngest, `{"type":"track","message_id":"other","actor_id":"other","name":"Other"}`, 200)
	duckLakeJourneyDrain(t, s)
	lake := s.lake.(*duckLakeStore)
	now := time.Now().Add(10 * 24 * time.Hour)
	for range 2 {
		if err := lake.CleanupDue(context.Background(), now); err != nil {
			t.Fatal(err)
		}
	}
	for range 2 {
		if err := lake.CleanupDue(context.Background(), now.Add(3*24*time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
	lakeMaintenanceVerify(t, s, a, state)
	if lakeStorageCount(t, lake, b, "events") != 1 {
		t.Fatal("other project's event changed")
	}
	admin := request(t, s, "GET", "/api/v1/status", adminToken, "", 200)
	if admin["ducklake_cleanup"].(map[string]any)["completed_catalogs"] != float64(2) {
		t.Fatalf("missing admin cleanup status: %v", admin)
	}
	scoped := request(t, s, "GET", "/api/v1/status", reader, "", 200)
	if _, ok := scoped["ducklake_cleanup"]; ok {
		t.Fatal("project reader saw instance cleanup details")
	}
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	ctx, session := mcpTestSession(t, host.URL, adminToken, "2025-11-25")
	result := mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "get-instance-status"}, 200)
	if result["ducklake_cleanup"] == nil {
		t.Fatal("MCP missing cleanup status")
	}
	ctx, session = mcpTestSession(t, host.URL, reader, "2025-11-25")
	result = mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "get-instance-status"}, 200)
	if result["ducklake_cleanup"] != nil {
		t.Fatal("MCP leaked cleanup status to scoped reader")
	}
	if _, err := lake.Registry.Exec("UPDATE cleanup_progress SET state='failed',failed=1 WHERE project_id=?", a); err != nil {
		t.Fatal(err)
	}
	admin = request(t, s, "GET", "/api/v1/status", adminToken, "", 200)
	if admin["status"] != "degraded" || admin["ducklake_cleanup"].(map[string]any)["failed_catalogs"] != float64(1) {
		t.Fatal("cleanup failure missing from admin health")
	}
	scoped = request(t, s, "GET", "/api/v1/status", reader, "", 200)
	if scoped["status"] != "ok" || scoped["ducklake_cleanup"] != nil {
		t.Fatal("project status disclosed instance cleanup failure")
	}

}
