package usage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	_ "modernc.org/sqlite"
	"path/filepath"
	"testing"
	"time"
)

func usageDB(t *testing.T, schema string) *sql.DB {
	t.Helper()
	db, e := sql.Open("sqlite", filepath.Join(t.TempDir(), "usage.db"))
	if e != nil {
		t.Fatal(e)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	_, e = db.Exec(`PRAGMA foreign_keys=ON;CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);CREATE TABLE tenants(id TEXT PRIMARY KEY);CREATE TABLE projects(id TEXT PRIMARY KEY,tenant_id TEXT NOT NULL,created_at TEXT NOT NULL);INSERT INTO tenants VALUES('t');INSERT INTO projects VALUES('p','t','2026-11-01T00:00:00Z');INSERT INTO metadata VALUES('usage_epoch','epoch'),('usage_available_since','2026-11-01T00:00:00.000000000Z');` + schema)
	if e != nil {
		t.Fatal(e)
	}
	return db
}
func clock() time.Time { return time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC) }
func config() Config   { c, _ := Normalize(nil); return c }
func TestRecorderAtomicDedupAndExecution(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := usageDB(t, JournalSchema)
	r := NewRecorder(db, "source", clock)
	tx, _ := db.Begin()
	if e := r.AddTx(ctx, tx, "p", clock(), Counters{Live: 3}, 0); e != nil {
		t.Fatal(e)
	}
	tx.Rollback()
	if got, _ := r.Pending(ctx); len(got) != 0 {
		t.Fatal("rolled back acceptance was metered")
	}
	tx, _ = db.Begin()
	if e := r.AddTx(ctx, tx, "p", clock(), Counters{Live: 3}, 0); e != nil {
		t.Fatal(e)
	}
	if e := tx.Commit(); e != nil {
		t.Fatal(e)
	}
	execution, e := r.StartExecution(ctx, "t", "p", "sql")
	if e != nil {
		t.Fatal(e)
	}
	got, _ := r.Pending(ctx)
	if got[0].Counters.Queries != 1 || got[0].Unresolved != 1 {
		t.Fatal(got)
	}
	if e = r.Observe(ctx); e != nil {
		t.Fatal(e)
	}
	got, _ = r.Pending(ctx)
	if got[0].Counters.QueryMicros != 0 {
		t.Fatal("admission waiting before native dispatch was measured", got)
	}
	execution.Begin()
	// Removing a project does not lose the final observation's frozen tenant.
	db.Exec(`DELETE FROM projects WHERE id='p'`)
	execution.Finish()
	execution.Finish()
	if e = r.Observe(ctx); e != nil {
		t.Fatal(e)
	}
	if e = r.Observe(ctx); e != nil {
		t.Fatal(e)
	}
	got, e = r.Pending(ctx)
	if e != nil {
		t.Fatal(e)
	}
	if got[0].Counters.Live != 3 || got[0].Counters.Queries != 1 || got[0].Unresolved != 0 {
		t.Fatal(got)
	}
}
func TestCumulativePublicationReplaysAndConflicts(t *testing.T) {
	t.Parallel()
	db := usageDB(t, ControlSchema)
	s := NewStore(db, false, config(), clock)
	ctx := context.Background()
	r := SourceTotals{SourceID: "a", TenantID: "t", ProjectID: "p", Day: Day(clock()), Revision: 2, Counters: Counters{Live: 8}}
	for range 2 {
		if e := s.PublishSourceTotals(ctx, []SourceTotals{r}); e != nil {
			t.Fatal(e)
		}
	}
	old := r
	old.Revision = 1
	old.Counters.Live = 2
	if e := s.PublishSourceTotals(ctx, []SourceTotals{old}); e != nil {
		t.Fatal(e)
	}
	bad := r
	bad.Counters.Live = 9
	if !errors.Is(s.PublishSourceTotals(ctx, []SourceTotals{bad}), ErrConflict) {
		t.Fatal("equal revision must have identical totals")
	}
	bad.Revision = 3
	bad.Counters.Live = 7
	if !errors.Is(s.PublishSourceTotals(ctx, []SourceTotals{bad}), ErrConflict) {
		t.Fatal("counters cannot decrease")
	}
	other := r
	other.SourceID = "b"
	if e := s.PublishSourceTotals(ctx, []SourceTotals{other}); e != nil {
		t.Fatal(e)
	}
	page, e := s.ReadSnapshot(ctx, Query{Start: "2026-11-17T00:00:00Z", End: "2026-11-18T00:00:00Z"})
	if e != nil {
		t.Fatal(e)
	}
	if len(page.Rows) != 1 || page.Rows[0].Counters.Live != "16" {
		t.Fatal(page)
	}
}
func TestSnapshotStablePagesCapacityAndExpiry(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := usageDB(t, ControlSchema)
	now := clock()
	s := NewStore(db, false, config(), func() time.Time { return now })
	q := Query{Start: "2026-11-16T00:00:00Z", End: "2026-11-18T00:00:00Z", Limit: 1}
	first, e := s.ReadSnapshot(ctx, q)
	if e != nil {
		t.Fatal(e)
	}
	if first.NextCursor == "" {
		t.Fatal("missing continuation")
	}
	if e = s.PublishSourceTotals(ctx, []SourceTotals{{SourceID: "a", TenantID: "t", ProjectID: "p", Day: Day(now), Revision: 1, Counters: Counters{Live: 999}}}); e != nil {
		t.Fatal(e)
	}
	next, e := s.ReadSnapshot(ctx, Query{Cursor: first.NextCursor})
	if e != nil {
		t.Fatal(e)
	}
	if next.Rows[0].Counters.Live != "0" {
		t.Fatal("snapshot changed", next)
	}
	if _, e = s.ReadSnapshot(ctx, Query{Cursor: first.NextCursor, TenantID: "different"}); !errors.Is(e, ErrCursor) {
		t.Fatal(e)
	}
	token, _, _, e := s.reserveBuild(ctx)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.ReadSnapshot(ctx, q); !errors.Is(e, ErrCapacity) {
		t.Fatal("global admission was not enforced", e)
	}
	s.releaseBuild(ctx, token)
	now = now.Add(SnapshotTTL)
	if _, e = s.ReadSnapshot(ctx, Query{Cursor: first.NextCursor}); !errors.Is(e, ErrExpired) {
		t.Fatal(e)
	}
}
func TestStorageCoverageAndEpoch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := usageDB(t, ControlSchema)
	db.Exec(`INSERT INTO projects VALUES('q','t','2026-11-01T00:00:00Z')`)
	s := NewStore(db, false, config(), clock)
	if e := s.RecordStorageSample(ctx, StorageSample{TenantID: "t", ProjectID: "p", Day: Day(clock()), ObservedAt: Stamp(clock().Add(-time.Hour)), Bytes: 100}); e != nil {
		t.Fatal(e)
	}
	p, e := s.ReadSnapshot(ctx, Query{Start: Day(clock()), End: Day(clock().AddDate(0, 0, 1)), GroupBy: "tenant"})
	if e != nil {
		t.Fatal(e)
	}
	if len(p.Rows) != 1 || p.Rows[0].Storage.Bytes != nil || p.Rows[0].Storage.Status != "partial" || p.Rows[0].Storage.MeasuredProjects != 1 {
		t.Fatal(p)
	}
	q := Query{Start: "2026-11-16T00:00:00Z", End: "2026-11-18T00:00:00Z", Limit: 1}
	p, e = s.ReadSnapshot(ctx, q)
	if e != nil {
		t.Fatal(e)
	}
	db.Exec(`UPDATE metadata SET value='restored' WHERE key='usage_epoch'`)
	if _, e = s.ReadSnapshot(ctx, Query{Cursor: p.NextCursor}); !errors.Is(e, ErrExpired) {
		t.Fatal(e)
	}
}

func TestSnapshotBoundsReleaseReservationAndRetention(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := usageDB(t, ControlSchema)
	c := config()
	c.MaxSnapshotBytes = 1 << 20
	s := NewStore(db, false, c, clock)
	for i := 0; i < 9; i++ {
		if _, e := db.Exec(`INSERT INTO projects VALUES(?, 't','2025-11-17T00:00:00Z')`, fmt.Sprint("extra", i)); e != nil {
			t.Fatal(e)
		}
	}
	if _, e := s.ReadSnapshot(ctx, Query{Start: "2025-11-17T00:00:00Z", End: "2026-11-18T00:00:00Z"}); !errors.Is(e, ErrTooLarge) {
		t.Fatal("byte bound not enforced", e)
	}
	if _, e := s.ReadSnapshot(ctx, Query{Start: Day(clock()), End: Day(clock().AddDate(0, 0, 1))}); e != nil {
		t.Fatal("failed build leaked its reservation", e)
	}
	r := SourceTotals{SourceID: "a", TenantID: "t", ProjectID: "p", Day: "2026-01-01T00:00:00Z", Revision: 1, Counters: Counters{Live: 4}}
	if e := s.PublishSourceTotals(ctx, []SourceTotals{r}); e != nil {
		t.Fatal(e)
	}
	s.Config.RetentionDays = 30
	if e := s.Prune(ctx); e != nil {
		t.Fatal(e)
	}
	var count int
	if e := db.QueryRow(`SELECT COUNT(*) FROM usage_rollups`).Scan(&count); e != nil || count != 0 {
		t.Fatal(count, e)
	}
}

func TestQueryFinishDoesNotWaitForObservationIO(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := usageDB(t, JournalSchema)
	r := NewRecorder(db, "source", clock)
	e, err := r.StartExecution(ctx, "t", "p", "sql")
	if err != nil {
		t.Fatal(err)
	}
	e.Begin()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	done := make(chan error, 1)
	go func() { done <- r.Observe(ctx) }()
	// Observe cannot obtain the only connection. Finish must still capture actual
	// backend return independently of SQLite latency.
	finished := make(chan struct{})
	go func() { e.Finish(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("native finish blocked on observation I/O")
	}
	tx.Rollback()
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	if err = r.Observe(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestBeforeInstrumentationIsUnknownRatherThanZero(t *testing.T) {
	t.Parallel()
	db := usageDB(t, ControlSchema)
	if _, e := db.Exec(`UPDATE projects SET created_at='2026-10-01T00:00:00Z' WHERE id='p'`); e != nil {
		t.Fatal(e)
	}
	s := NewStore(db, false, config(), clock)
	for _, group := range []string{"project", "tenant"} {
		page, e := s.ReadSnapshot(t.Context(), Query{Start: "2026-10-31T00:00:00Z", End: "2026-11-01T00:00:00Z", GroupBy: group})
		if e != nil {
			t.Fatal(e)
		}
		if len(page.Rows) != 1 || page.Rows[0].Counters != nil || page.Rows[0].QueryDuration != nil {
			t.Fatal(page)
		}
	}
}

func TestExecutionObservationsAreCumulativeAndCrashRemainsIncomplete(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db := usageDB(t, JournalSchema)
	r := NewRecorder(db, "source", clock)
	e, err := r.StartExecution(ctx, "t", "p", "sql")
	if err != nil {
		t.Fatal(err)
	}
	// Model an invocation already running for ten seconds without a ten-second
	// test sleep. Subsequent observations must add only the newly observed tail.
	e.mu.Lock()
	e.started = time.Now().Add(-10 * time.Second)
	started := e.started
	e.mu.Unlock()
	if err = r.Observe(ctx); err != nil {
		t.Fatal(err)
	}
	first, err := r.Pending(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Observe(ctx); err != nil {
		t.Fatal(err)
	}
	second, err := r.Pending(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if second[0].Counters.QueryMicros < first[0].Counters.QueryMicros || second[0].Counters.QueryMicros > time.Since(started).Microseconds() {
		t.Fatal("cumulative observations were added twice", first, second)
	}
	e.Finish()
	elapsed := e.elapsed
	if err = r.Observe(ctx); err != nil {
		t.Fatal(err)
	}
	if err = r.Observe(ctx); err != nil {
		t.Fatal(err)
	}
	finished, err := r.Pending(ctx)
	if err != nil || finished[0].Counters.QueryMicros != elapsed || finished[0].Unresolved != 0 {
		t.Fatal(finished, elapsed, err)
	}
	if _, err = r.StartExecution(ctx, "t", "p", "profile"); err != nil {
		t.Fatal(err)
	}
	recovered := NewRecorder(db, "source", clock)
	if err = recovered.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if err = recovered.Observe(ctx); err != nil {
		t.Fatal(err)
	}
	lost, err := recovered.Pending(ctx)
	if err != nil || lost[0].Counters.Queries != 2 || lost[0].Counters.QueryMicros != elapsed || lost[0].Unresolved != 1 {
		t.Fatal("crash fabricated completion or lost the admitted count", lost, err)
	}
}
