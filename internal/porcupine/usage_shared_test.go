package porcupine

import (
	"database/sql"
	"sync"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/usage"
)

func TestSharedUsageCumulativeSourcesAndSnapshotReplay(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	a, b := sharedJourneyPair(t, nil)
	tenant := tenantTestCreate(t, a.service, "Shared numerical usage")
	project, _ := tenantTestProject(t, a.service, tenant)
	at := time.Now()
	var wg sync.WaitGroup
	failures := make(chan error, 2)
	for i, node := range []*sharedJourneyNode{a, b} {
		wg.Go(func() {
			source := []string{"first", "second"}[i]
			for revision := int64(1); revision <= 5; revision++ {
				report := usage.SourceTotals{SourceID: source, TenantID: tenant, ProjectID: project, Day: usage.Day(at), Revision: revision, Counters: usage.Counters{Live: revision}}
				if e := node.service.usage.store.PublishSourceTotals(t.Context(), []usage.SourceTotals{report, report}); e != nil {
					failures <- e
					return
				}
			}
		})
	}
	wg.Wait()
	close(failures)
	for e := range failures {
		t.Fatal(e)
	}
	q := usage.Query{Start: usage.Day(at.AddDate(0, 0, -1)), End: usage.Day(at.AddDate(0, 0, 1)), TenantID: tenant, Limit: 1}
	if _, e := a.service.control.Exec(`UPDATE projects SET created_at=$1 WHERE id=$2`, q.Start, project); e != nil {
		t.Fatal(e)
	}
	// A source observed yesterday keeps that bucket visible after project creation
	// today; the immutable cursor must return the second page on another worker.
	report := usage.SourceTotals{SourceID: "first", TenantID: tenant, ProjectID: project, Day: q.Start, Revision: 1, Counters: usage.Counters{Import: 1}}
	if e := a.service.usage.store.PublishSourceTotals(t.Context(), []usage.SourceTotals{report}); e != nil {
		t.Fatal(e)
	}
	first, e := a.service.usage.store.ReadSnapshot(t.Context(), q)
	if e != nil {
		t.Fatal(e)
	}
	if first.NextCursor == "" { // Both periods need to belong to this project.
		t.Fatal("expected two snapshot pages", first)
	}
	next, e := b.service.usage.store.ReadSnapshot(t.Context(), usage.Query{Cursor: first.NextCursor})
	if e != nil {
		t.Fatal(e)
	}
	if len(next.Rows) != 1 || next.Rows[0].Counters.Live != "10" {
		t.Fatal(next)
	}
}

func TestSharedUsageQueryOnNonOwnerNeedsNoJournalProjectAnchor(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	a, b := sharedJourneyPair(t, nil)
	id, _ := project(t, a.service)
	// Owner-free query admission on B must retain its metering in source B even
	// though event ingestion and its local project anchor belong to A.
	var anchors int
	if e := b.service.db.QueryRow(`SELECT COUNT(*) FROM projects WHERE id=?`, id).Scan(&anchors); e != nil || anchors != 0 {
		t.Fatal(anchors, e)
	}
	_, _, scope, e := b.service.beginUsageQuery(t.Context(), id, "sql")
	if e != nil {
		t.Fatal("non-owner query rejected by metering", e)
	}
	scope.Begin()
	if e = scope.Finish(); e != nil {
		t.Fatal(e)
	}
	if e = b.service.usage.synchronize(t.Context()); e != nil {
		t.Fatal(e)
	}
	var count int64
	if e = b.service.control.QueryRow(`SELECT COALESCE(SUM(query_count),0) FROM usage_rollups WHERE project_id=$1 AND source_id=$2`, id, b.service.usage.recorder.SourceID).Scan(&count); e != nil || count != 1 {
		t.Fatal(count, e)
	}
	if e = b.service.db.QueryRow(`SELECT COUNT(*) FROM projects WHERE id=?`, id).Scan(&anchors); e != nil || anchors != 0 {
		t.Fatal("query manufactured an ingestion owner anchor", anchors, e)
	}
}

func TestSharedUsageStorageSampleCannotOverwriteCommittedDeletionZero(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	db, err := sql.Open("pgx", sharedPostgresDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(4)
	if _, err = db.Exec(`CREATE TABLE projects(id TEXT PRIMARY KEY,tenant_id TEXT NOT NULL); INSERT INTO projects VALUES('p','t');` + usage.ControlSchema); err != nil {
		t.Fatal(err)
	}
	config, _ := usage.Normalize(nil)
	store := usage.NewStore(db, true, config, time.Now)
	at := time.Now().UTC()
	sample := usage.StorageSample{TenantID: "t", ProjectID: "p", Day: usage.Day(at), ObservedAt: usage.Stamp(at), Bytes: 100}
	if err = store.RecordStorageSample(t.Context(), sample); err != nil {
		t.Fatal(err)
	}
	deleting, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer deleting.Rollback()
	var id string
	if err = deleting.QueryRow(`SELECT id FROM projects WHERE id='p' FOR UPDATE`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	zero := sample
	zero.Bytes = 0
	zero.ObservedAt = usage.Stamp(at.Add(time.Second))
	zero.Day = usage.Day(at.Add(time.Second))
	if err = store.RecordStorageSampleTx(t.Context(), deleting, zero); err != nil {
		t.Fatal(err)
	}
	if _, err = deleting.Exec(`DELETE FROM projects WHERE id='p'`); err != nil {
		t.Fatal(err)
	}
	sample.ObservedAt = usage.Stamp(at.Add(2 * time.Second))
	sample.Day = usage.Day(at.Add(2 * time.Second))
	done := make(chan error, 1)
	go func() { done <- store.RecordStorageSample(t.Context(), sample) }()
	// Prove the competing statement started and is waiting on this transaction;
	// a sleep would not establish that it acquired an old statement snapshot.
	deadline := time.Now().Add(2 * time.Second)
	for {
		var waiting bool
		if err = db.QueryRow(`SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock')`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case err = <-done:
			t.Fatal("sample escaped the uncommitted deletion", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("sample did not reach deletion lock")
		}
		time.Sleep(time.Millisecond)
	}
	if err = deleting.Commit(); err != nil {
		t.Fatal(err)
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	var bytes int64
	if err = db.QueryRow(`SELECT bytes FROM usage_storage_samples WHERE project_id='p' ORDER BY day_start DESC LIMIT 1`).Scan(&bytes); err != nil || bytes != 0 {
		t.Fatal("late positive sample overwrote committed zero", bytes, err)
	}
}
