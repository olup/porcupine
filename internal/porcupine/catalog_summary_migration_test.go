package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/acceptance"
	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/query"
)

func TestCatalogSummaryMigrationSQLite(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "control.db")
	db, err := openLocalSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	previous := localControlSchema
	previous.steps = previous.steps[:9]
	if err := migrateSQLite(t.Context(), db, previous); err != nil {
		t.Fatal(err)
	}
	seedCatalogStoreProjects(t, db)
	limits, _ := query.Normalize(nil)
	if err := initializeLocalControl(t.Context(), db, adminToken, limits, false); err != nil {
		t.Fatal(err)
	}
	assertCatalogMigration(t, db, `PRAGMA user_version`, 10)
}

func TestSharedCatalogSummaryMigrationAndHostedFence(t *testing.T) {
	t.Parallel()
	db, _ := sharedControlFixture(t)
	for _, step := range sharedControlSteps[:26] {
		if _, err := db.ExecContext(t.Context(), step); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.ExecContext(t.Context(), `INSERT INTO metadata(key,value) VALUES('schema_version','26')
ON CONFLICT(key) DO UPDATE SET value=excluded.value`); err != nil {
		t.Fatal(err)
	}
	seedCatalogStoreProjects(t, db)
	limits, _ := query.Normalize(nil)
	if err := initializeSharedControl(t.Context(), db, adminToken, limits, false); err != nil {
		t.Fatal(err)
	}
	assertCatalogMigration(t, db, `SELECT value FROM metadata WHERE key='schema_version'`, 27)

	s := &Service{control: db, hosted: &hostedRuntime{}, lake: &lakeapi.DuckLakeStore{}, eventRules: &eventRules{clock: time.Now}}
	s.startCatalogRefresh()
	refresh := s.eventCatalogRefresh
	refresh.Stop()
	<-refresh.Done()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	deleting, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer deleting.Rollback()
	var pid int
	if err := deleting.QueryRowContext(ctx, `SELECT pg_backend_pid() FROM tenants WHERE id='catalog-tenant' FOR UPDATE`).Scan(&pid); err != nil {
		t.Fatal(err)
	}
	if _, err := deleting.ExecContext(ctx, `INSERT INTO tenant_deletions(tenant_id,state,started_at,updated_at)
VALUES('catalog-tenant','deleting','','')`); err != nil {
		t.Fatal(err)
	}
	requested := make(chan error, 1)
	go func() {
		_, err := refresh.Request(ctx, "p")
		requested <- err
	}()
	for {
		var waiting bool
		if err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity
WHERE $1=ANY(pg_blocking_pids(pid)) AND query LIKE '%FOR SHARE OF t%')`, pid).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case err := <-requested:
			t.Fatal("catalog request passed the uncommitted tenant lock", err)
		case <-time.After(5 * time.Millisecond):
		}
	}
	if status, err := refresh.Request(ctx, "other"); err != nil || status == nil || status.Status != "queued" {
		t.Fatal("deletion blocked another tenant's catalog refresh", status, err)
	}
	if err := deleting.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := <-requested; !errors.Is(err, acceptance.ErrProjectFenced) {
		t.Fatal("catalog request missed the committed deletion fence", err)
	}
	var exists bool
	if err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM catalog_refresh_state WHERE project_id='p')`).Scan(&exists); err != nil || exists {
		t.Fatal("fenced request queued a catalog refresh", exists, err)
	}
}

func assertCatalogMigration(t *testing.T, db *sql.DB, versionQuery string, version int) {
	t.Helper()
	var got int
	if err := db.QueryRowContext(t.Context(), versionQuery).Scan(&got); err != nil || got != version {
		t.Fatalf("control schema version = %d, %v, want %d", got, err, version)
	}
	var refreshes, summaries, projects int
	if err := db.QueryRowContext(t.Context(), `SELECT
(SELECT count(*) FROM catalog_refresh_state), (SELECT count(*) FROM catalog_summary),
(SELECT count(*) FROM projects WHERE id IN ('p','other'))`).Scan(&refreshes, &summaries, &projects); err != nil ||
		refreshes != 0 || summaries != 0 || projects != 2 {
		t.Fatalf("upgrade changed catalog or projects: refreshes=%d summaries=%d projects=%d, %v", refreshes, summaries, projects, err)
	}
}

func seedCatalogStoreProjects(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.ExecContext(t.Context(), `INSERT INTO tenants(id,name,created_at,hot_query_max_concurrent,hot_query_timeout_ms,hot_query_max_rows,hot_query_max_result_bytes)
VALUES('catalog-tenant','Catalog tenant','',1,10000,100,1024),('other-tenant','Other tenant','',1,10000,100,1024);
INSERT INTO projects(id,tenant_id,name,created_at) VALUES('p','catalog-tenant','Project',''),('other','other-tenant','Other project','')`); err != nil {
		t.Fatal(err)
	}
}
