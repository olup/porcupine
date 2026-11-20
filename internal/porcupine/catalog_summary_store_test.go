package porcupine

import (
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/query"
)

func TestCatalogSummaryStoreSQLite(t *testing.T) {
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
	second, err := openLocalSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	testCatalogSummaryStore(t, catalogSummaryStore{db: db}, catalogSummaryStore{db: second})
}

func TestSharedCatalogSummaryStore(t *testing.T) {
	t.Parallel()
	db, dsn := sharedControlFixture(t)
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
	second, err := openSharedControl(t.Context(), SharedConfig{DatabaseURL: dsn})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	testCatalogSummaryStore(t, catalogSummaryStore{db: db, hosted: true}, catalogSummaryStore{db: second, hosted: true})
}

func seedCatalogStoreProjects(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.ExecContext(t.Context(), `INSERT INTO tenants(id,name,created_at,hot_query_max_concurrent,hot_query_timeout_ms,hot_query_max_rows,hot_query_max_result_bytes)
VALUES('catalog-tenant','Catalog tenant','',1,10000,100,1024),('other-tenant','Other tenant','',1,10000,100,1024);
INSERT INTO projects(id,tenant_id,name,created_at) VALUES('p','catalog-tenant','Project',''),('other','other-tenant','Other project','')`); err != nil {
		t.Fatal(err)
	}
}

func testCatalogSummaryStore(t *testing.T, first, second catalogSummaryStore) {
	t.Helper()
	ctx := t.Context()
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	lease := 5 * time.Minute
	read := func(s catalogSummaryStore, project string, limit, offset int) (lakeapi.EventCatalog, catalogSummaryState) {
		t.Helper()
		page, state, err := s.page(ctx, lakeapi.EventCatalogInput{ID: project, Limit: limit, Offset: offset, PropertyLimit: 10}, at)
		if err != nil {
			t.Fatal(err)
		}
		return page, state
	}
	claim := func(s catalogSummaryStore) catalogRefreshClaim {
		t.Helper()
		c, err := s.claim(ctx, at, lease)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	idle := func(s catalogSummaryStore) {
		t.Helper()
		if c, err := s.claim(ctx, at, lease); !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expected no due refresh: %+v %v", c, err)
		}
	}
	request := func(s catalogSummaryStore, project string, force bool, wantStatus string) {
		t.Helper()
		if state, err := s.request(ctx, project, at, force); err != nil || state.Status != wantStatus {
			t.Fatalf("request state: %+v, %v; want %s", state, err, wantStatus)
		}
	}
	complete := func(s catalogSummaryStore, c catalogRefreshClaim, result lakeapi.CatalogSnapshotRead) {
		t.Helper()
		if err := s.complete(ctx, c, result, at); err != nil {
			t.Fatal(err)
		}
	}

	idle(first)
	if _, state := read(first, "p", 1, 0); state.Status != "" {
		t.Fatalf("migration seeded a project before discovery: %+v", state)
	}
	if state, err := first.status(ctx, "p", at); err != nil || state != (catalogSummaryState{}) {
		t.Fatalf("status for an unseeded project: %+v %v", state, err)
	}
	idle(first)
	request(first, "p", false, "queued")
	if page, state := read(second, "p", 1, 0); state.Status != "queued" || state.Snapshot.CatalogID != "" || !state.RefreshedAt.IsZero() || len(page.Events) != 0 {
		t.Fatalf("unbuilt summary: %+v %+v", page, state)
	}

	// Two workers cannot run the same job. A manual request coalesces with the
	// running job, and a second connection can recover it later.
	firstClaim := raceCatalogClaims(t, first, second, at, lease)
	request(second, "p", true, "running")
	idle(second)
	at = at.Add(lease)
	stolen := claim(second)
	if stolen.ProjectID != "p" || stolen.token == firstClaim.token || stolen.Snapshot.CatalogID != "" {
		t.Fatalf("abandoned refresh did not recover: %+v", stolen)
	}
	v1 := lakeapi.ExportSnapshot{CatalogID: "live", Version: 1}
	seed := lakeapi.CatalogSnapshotRead{Snapshot: v1, Replace: true, Rows: []lakeapi.CatalogAggregate{
		catalogAggregate("alpha", "2", 10, 50), catalogAggregate("beta", "1", 100, 100),
	}}
	if err := first.complete(ctx, firstClaim, seed, at); !errors.Is(err, errCatalogRefreshClaimLost) {
		t.Fatalf("abandoned refresh completed: %v", err)
	}
	complete(second, stolen, seed)
	if p, s := read(first, "p", 1, 0); s.Status != "current" || s.Snapshot != v1 || !s.RefreshedAt.Equal(at) || len(p.Events) != 1 || p.Events[0] != seed.Rows[1].Event || p.NextOffset == nil || *p.NextOffset != 1 || p.Sampling.PropertyLimit != 10 {
		t.Fatalf("first summary page: %+v, %+v", p, s)
	}
	if state, err := second.status(ctx, "p", at); err != nil || state.Status != "current" || state.Snapshot != v1 || !state.RefreshedAt.Equal(at) {
		t.Fatalf("summary status: %+v %v", state, err)
	}
	if p, _ := read(second, "p", 1, 1); len(p.Events) != 1 || p.Events[0] != seed.Rows[0].Event || p.NextOffset != nil {
		t.Fatalf("second summary page: %+v", p)
	}
	if p, s := read(second, "other", 1, 0); len(p.Events) != 0 || s.Status != "" {
		t.Fatalf("summary leaked to unseeded project: %+v %+v", p, s)
	}

	at = at.Add(time.Minute)
	request(second, "p", false, "current")
	idle(first)
	request(first, "p", true, "queued")
	request(second, "p", true, "queued")
	incremental := claim(first)
	idle(second)
	v2 := lakeapi.ExportSnapshot{CatalogID: "live", Version: 2}
	delta := lakeapi.CatalogSnapshotRead{Snapshot: v2, Rows: []lakeapi.CatalogAggregate{
		catalogAggregate("alpha", "3", 20, 80), catalogAggregate("beta", "2", 55, 60), catalogAggregate("gamma", "1", 200, 200),
	}}
	bad := lakeapi.CatalogSnapshotRead{Snapshot: v2, Rows: []lakeapi.CatalogAggregate{delta.Rows[0], catalogAggregate("bad", "0", 1000, 1000)}}
	if err := first.complete(ctx, incremental, bad, at); err == nil {
		t.Fatal("invalid batch published")
	}
	if p, s := read(second, "p", 5, 0); s.Snapshot != v1 || len(p.Events) != 2 || p.Events[1].Count != "2" {
		t.Fatalf("invalid batch changed visible state: %+v %+v", p, s)
	}
	complete(first, incremental, delta)
	if err := second.complete(ctx, incremental, delta, at); !errors.Is(err, errCatalogRefreshClaimLost) {
		t.Fatalf("duplicate refresh completed: %v", err)
	}
	if p, s := read(second, "p", 5, 0); s.Snapshot != v2 || s.Status != "current" || len(p.Events) != 3 ||
		p.Events[0] != delta.Rows[2].Event || p.Events[1] != catalogAggregate("beta", "3", 55, 100).Event || p.Events[2] != catalogAggregate("alpha", "5", 10, 80).Event {
		t.Fatalf("incremental rows or ID boundaries: %+v %+v", p, s)
	}

	at = at.Add(catalogRefreshInterval - time.Millisecond)
	idle(first)
	at = at.Add(time.Millisecond)
	unchanged := claim(second)
	complete(second, unchanged, lakeapi.CatalogSnapshotRead{Snapshot: v2})
	if _, s := read(first, "p", 1, 0); s.Snapshot != v2 || !s.RefreshedAt.Equal(at) || s.Status != "current" {
		t.Fatalf("verification of unchanged snapshot: %+v", s)
	}
	if err := first.invalidate(ctx, "p", v1, at); err != nil {
		t.Fatal(err)
	}
	idle(second)
	request(first, "p", true, "queued")
	invalidated := claim(second)
	if err := first.invalidate(ctx, "p", v2, at); err != nil {
		t.Fatal(err)
	}
	if err := second.complete(ctx, invalidated, lakeapi.CatalogSnapshotRead{Snapshot: v2}, at); !errors.Is(err, errCatalogRefreshClaimLost) {
		t.Fatalf("invalidated refresh completed: %v", err)
	}
	if p, s := read(second, "p", 5, 0); s.Snapshot.CatalogID != "" || s.Status != "queued" || len(p.Events) != 0 {
		t.Fatalf("invalidated rows remained visible: %+v %+v", p, s)
	}
	failed := claim(first)
	if err := first.fail(ctx, failed, at, false); err != nil {
		t.Fatal(err)
	}
	if _, s := read(second, "p", 1, 0); s.Status != "retrying" {
		t.Fatalf("failed refresh state: %+v", s)
	}
	at = at.Add(time.Minute - time.Millisecond)
	idle(second)
	at = at.Add(time.Millisecond)
	retry := claim(second)
	replacement := lakeapi.CatalogSnapshotRead{Snapshot: lakeapi.ExportSnapshot{CatalogID: "restored", Version: 0}, Replace: true, Rows: []lakeapi.CatalogAggregate{catalogAggregate("restored", "1", 1, 1)}}
	complete(second, retry, replacement)
	if p, s := read(first, "p", 5, 0); s.Snapshot != replacement.Snapshot || len(p.Events) != 1 || p.Events[0] != replacement.Rows[0].Event {
		t.Fatalf("rebuilt catalog: %+v %+v", p, s)
	}
	request(first, "p", true, "queued")
	oversized := claim(first)
	tooLarge := lakeapi.CatalogSnapshotRead{Snapshot: lakeapi.ExportSnapshot{CatalogID: "restored", Version: 1}, Rows: []lakeapi.CatalogAggregate{
		catalogAggregate(strings.Repeat("x", 501), "1", 2, 2),
	}}
	if err := first.complete(ctx, oversized, tooLarge, at); !errors.Is(err, lakeapi.ErrCatalogSummaryTooLarge) {
		t.Fatalf("oversized catalog result: %v", err)
	}
	if err := first.fail(ctx, oversized, at, true); err != nil {
		t.Fatal(err)
	}
	if p, s := read(second, "p", 1, 0); s.Snapshot.CatalogID != "" || !s.RefreshedAt.IsZero() || s.Status != "retrying" || len(p.Events) != 0 {
		t.Fatalf("oversized catalog kept a partial or stale summary: %+v %+v", p, s)
	}
	at = at.Add(catalogRefreshInterval - time.Millisecond)
	idle(second)
	at = at.Add(time.Millisecond)
	complete(second, claim(second), replacement)

	request(first, "p", true, "queued")
	deleting := claim(first)
	request(first, "other", false, "queued")
	if _, err := second.db.ExecContext(ctx, `INSERT INTO tenant_deletions(tenant_id,state,started_at,updated_at) VALUES('catalog-tenant','deleting','','')`); err != nil {
		t.Fatal(err)
	}
	if _, err := first.request(ctx, "p", at, true); !errors.Is(err, errCatalogProjectFenced) {
		t.Fatalf("request crossed deletion fence: %v", err)
	}
	if err := first.complete(ctx, deleting, replacement, at); !errors.Is(err, errCatalogProjectFenced) {
		t.Fatalf("publish crossed deletion fence: %v", err)
	}
	if p, state := read(second, "p", 5, 0); len(p.Events) != 0 || state.Status != "" {
		t.Fatalf("summary visible across deletion fence: %+v %+v", p, state)
	}
	if state, err := second.status(ctx, "p", at); err != nil || state != (catalogSummaryState{}) {
		t.Fatalf("status crossed a deletion fence: %+v %v", state, err)
	}
	at = at.Add(lease)
	other := claim(second)
	if other.ProjectID != "other" {
		t.Fatalf("wrong project refresh claimed: %+v", other)
	}
	complete(second, other, replacement)
	idle(first)
	if _, err := second.db.ExecContext(ctx, `DELETE FROM projects WHERE id='p'`); err != nil {
		t.Fatal(err)
	}
	var left bool
	if err := first.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM catalog_refresh_state WHERE project_id='p')
OR EXISTS(SELECT 1 FROM catalog_summary WHERE project_id='p')`).Scan(&left); err != nil || left {
		t.Fatalf("deleted project retained catalog data: %v %v", left, err)
	}
	if p, s := read(first, "other", 5, 0); s.Snapshot != replacement.Snapshot || len(p.Events) != 1 {
		t.Fatalf("other project's catalog lost during deletion: %+v %+v", p, s)
	}
}

func catalogAggregate(event, count string, first, last int64) lakeapi.CatalogAggregate {
	return lakeapi.CatalogAggregate{FirstID: first, LastID: last, Event: lakeapi.EventCatalogEvent{
		Event: event, Count: count, FirstReceivedAt: fmt.Sprint(first), LastReceivedAt: fmt.Sprint(last), LatestEventUUID: fmt.Sprintf("uuid-%d", last), LatestEventTimestamp: fmt.Sprintf("time-%d", last),
	}}
}

func raceCatalogClaims(t *testing.T, first, second catalogSummaryStore, at time.Time, lease time.Duration) catalogRefreshClaim {
	t.Helper()
	type outcome struct {
		claim catalogRefreshClaim
		err   error
	}
	start := make(chan struct{})
	results := make(chan outcome, 2)
	for _, store := range []catalogSummaryStore{first, second} {
		go func() {
			<-start
			claim, err := store.claim(t.Context(), at, lease)
			results <- outcome{claim, err}
		}()
	}
	close(start)
	var winner catalogRefreshClaim
	for range 2 {
		result := <-results
		if errors.Is(result.err, sql.ErrNoRows) {
			continue
		}
		if result.err != nil || result.claim.ProjectID != "p" || winner.token != "" {
			t.Fatalf("concurrent claim: %+v %v; already claimed %+v", result.claim, result.err, winner)
		}
		winner = result.claim
	}
	if winner.token == "" {
		t.Fatal("neither worker claimed available refresh")
	}
	return winner
}
