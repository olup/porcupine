package porcupine

import (
	"github.com/olup/porcupine/internal/usage"
	"sync"
	"testing"
	"time"
)

func TestUsageSharedCumulativeSourcesAndSnapshotReplay(t *testing.T) {
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

func TestUsageQueryOnNonOwnerNeedsNoJournalProjectAnchor(t *testing.T) {
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
	_, _, finish, e := b.service.beginUsageQuery(t.Context(), id, "sql")
	if e != nil {
		t.Fatal("non-owner query rejected by metering", e)
	}
	if e = finish(); e != nil {
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
