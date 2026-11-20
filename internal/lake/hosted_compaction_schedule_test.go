package lake

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/event"
)

func TestDuckLakeS3SharedPostgresHostedCompactionSchedule(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	fixture := nativeS3Fixture(t)
	config := DuckLakeConfig{PostgresURL: sharedPostgresDSN(t), SharedSourceID: event.NewID(), S3: &fixture.storage, MemoryLimitMB: 128, Threads: 1, MaxOpenProjects: 2, DisableCleanup: true}
	open := func() *DuckLakeStore {
		config.Directory = t.TempDir()
		s, err := NewDuckLake(config)
		s3FixtureError(t, fixture, "open store", err)
		t.Cleanup(func() { s3FixtureError(t, fixture, "close store", s.Close()) })
		return s
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	a, b := open(), open()
	db, first := nativeFenceControl(t)
	second, err := NewPostgresProjectCoordinator(db, DefaultProjectCoordinatorConfig())
	if err != nil {
		t.Fatal(err)
	}
	idle := ProjectWriteKey{config.SharedSourceID, "a-idle"}
	busy := ProjectWriteKey{config.SharedSourceID, "b-work"}
	for _, key := range []ProjectWriteKey{idle, busy} {
		scope, err := a.InitializeHostedProject(ctx, key)
		s3FixtureError(t, fixture, "initialize hosted catalog", err)
		if key == busy {
			owner := acquireFenceSession(t, first, key)
			fence := owner.Fence(scope.StreamID)
			s3FixtureError(t, fixture, "advance seed fence", a.AdvanceWriteFence(ctx, fence))
			for _, marker := range []string{"one", "two", "three"} {
				s3FixtureError(t, fixture, "seed small Parquet file", fenceWitnessTx(ctx, a.Projects[key.ProjectID], fence, marker, nil, nil, nil))
			}
			s3FixtureError(t, fixture, "release seed owner", owner.Release())
		}
	}
	now := time.Now()
	due := func(at time.Time, store *DuckLakeStore, coordinator *PostgresProjectCoordinator, run func(context.Context, *ProjectWriteSession) (DuckLakeMaintenanceStats, error)) {
		t.Helper()
		s3FixtureError(t, fixture, "run scheduled compaction", store.CompactHostedDue(ctx, at, coordinator, run))
	}
	unexpected := func(context.Context, *ProjectWriteSession) (DuckLakeMaintenanceStats, error) {
		t.Fatal("unexpected native dispatch")
		return DuckLakeMaintenanceStats{}, nil
	}
	due(now, b, second, unexpected)
	if len(b.Projects) != 0 {
		t.Fatal("metadata-only idle check warmed a project")
	}
	state := func(project, want string) {
		t.Helper()
		var got string
		err := a.SharedCatalog.QueryRowContext(ctx, `SELECT state FROM pc_native_compaction WHERE source_id=$1 AND project_id=$2`, config.SharedSourceID, project).Scan(&got)
		if err != nil || got != want {
			t.Fatal(project, "state", got, "want", want, err)
		}
	}
	state(idle.ProjectID, "ok")
	owner := acquireFenceSession(t, first, busy)
	due(now, b, second, unexpected)
	state(busy.ProjectID, "waiting")
	due(now, b, second, unexpected)
	if len(b.Projects) != 0 {
		t.Fatal("contended coordinator warmed a project")
	}
	s3FixtureError(t, fixture, "release busy owner", owner.Release())

	// Two workers share the same claim. Cancel the winner while its dispatched
	// call is joining. Until return the project still belongs to that session.
	at := now.Add(hostedCompactionRetry)
	dispatched, returned := make(chan struct{}), make(chan error, 1)
	resume, joined := make(chan struct{}), make(chan struct{})
	canceled, cancelWinner := context.WithCancel(ctx)
	defer cancelWinner()
	go func() {
		defer close(joined)
		returned <- b.CompactHostedDue(canceled, at, second, func(ctx context.Context, s *ProjectWriteSession) (stats DuckLakeMaintenanceStats, err error) {
			err = s.RunNative(ctx, func(ctx context.Context) error {
				close(dispatched)
				<-ctx.Done()
				<-resume
				return ctx.Err()
			})
			return stats, err
		})
	}()
	defer func() {
		close(resume)
		<-joined
	}()
	select {
	case <-dispatched:
	case err := <-returned:
		t.Fatal("scheduler returned before dispatch", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	due(at, a, first, unexpected)
	cancelWinner()
	if session, err := first.TryAcquire(ctx, busy); !errors.Is(err, ErrProjectWriteBusy) {
		if session != nil {
			_ = session.Release()
		}
		t.Fatal("cancellation released ownership before native return", err)
	}
	if session, err := first.TryAcquire(ctx, idle); err != nil {
		t.Fatal("maintenance blocked unrelated project", err)
	} else {
		s3FixtureError(t, fixture, "release unrelated owner", session.Release())
	}
	resume <- struct{}{}
	select {
	case err := <-returned:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("cancelled maintenance", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	state(busy.ProjectID, "interrupted")

	// If DuckLake rejects a candidate layout, later polls must not open it
	// again. A new write restores eligibility.
	at = at.Add(hostedCompactionRetry)
	probes := 0
	due(at, b, second, func(context.Context, *ProjectWriteSession) (DuckLakeMaintenanceStats, error) {
		probes++
		return DuckLakeMaintenanceStats{Completed: true}, nil
	})
	if probes != 1 {
		t.Fatal("did not recheck interrupted project")
	}
	at = at.Add(hostedCompactionInterval)
	due(at, b, second, unexpected) // a-idle
	due(at, b, second, unexpected) // unchanged b-work
	p := a.Projects[busy.ProjectID]
	owner = acquireFenceSession(t, first, busy)
	scope, err := a.LookupHostedProjectExisting(ctx, busy)
	s3FixtureError(t, fixture, "read existing scope", err)
	fence := owner.Fence(scope.StreamID)
	s3FixtureError(t, fixture, "advance new write fence", a.AdvanceWriteFence(ctx, fence))
	s3FixtureError(t, fixture, "change file layout", fenceWitnessTx(ctx, p, fence, "four", nil, nil, nil))
	s3FixtureError(t, fixture, "release new write owner", owner.Release())

	before := maintenanceExperimentFiles(t, ctx, p)
	at = at.Add(hostedCompactionInterval)
	due(at, b, second, unexpected) // a-idle
	due(at, b, second, b.MaintainHostedProject)
	state(busy.ProjectID, "ok")
	if after := maintenanceExperimentFiles(t, ctx, p); after >= before {
		t.Fatal("scheduled maintenance did no work", before, after)
	}
	var count int
	if err := p.DB.QueryRowContext(ctx, `SELECT count(*) FROM lake.heads`).Scan(&count); err != nil || count != 4 {
		t.Fatal("compaction changed logical rows", count, err)
	}
	var legacy int
	if err := a.SharedCatalog.QueryRowContext(ctx, `SELECT count(*) FROM pc_native_cleanup WHERE source_id=$1`, config.SharedSourceID).Scan(&legacy); err != nil || legacy != 0 {
		t.Fatal("compaction touched retention schedule", legacy, err)
	}
}

func TestSharedPostgresHostedCompactionClaims(t *testing.T) {
	t.Parallel()
	db, _ := registryTestDB(t)
	if err := migrateNativeRegistry(t.Context(), db); err != nil {
		t.Fatal(err)
	}
	r := &postgresNativeRegistry{db: db, source: event.NewID()}
	ctx := t.Context()
	if _, err := db.ExecContext(ctx, `INSERT INTO pc_native_sources VALUES($1,'fixture')`, r.source); err != nil {
		t.Fatal(err)
	}
	for _, project := range []string{"ready", "unready", "deleted"} {
		if _, err := r.ensure(ctx, project); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.markReady(ctx, "ready"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE pc_native_projects SET ready=1,deleted=1 WHERE source_id=$1 AND project_id='deleted'`, r.source); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	first, err := r.claimDue(ctx, hostedCompactionSchedule, now, now.Add(hostedCompactionClaimTTL))
	if err != nil || first.ProjectID != "ready" {
		t.Fatal(first, err)
	}
	if _, err := r.claimDue(ctx, hostedCompactionSchedule, now, now.Add(hostedCompactionClaimTTL)); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("unexpired claim or ineligible project reselected", err)
	}
	next := now.Add(hostedCompactionClaimTTL)
	second, err := r.claimDue(ctx, hostedCompactionSchedule, next, next.Add(hostedCompactionClaimTTL))
	if err != nil || second.ProjectID != first.ProjectID || second.ID == first.ID {
		t.Fatal(second, err)
	}
	if err := r.finishHostedCompaction(ctx, second, "ok", next.Add(time.Hour), 20); err != nil {
		t.Fatal(err)
	}
	if err := r.finishHostedCompaction(ctx, first, "ok", next, 10); err != nil {
		t.Fatal(err)
	}
	var layout int64
	if err := db.QueryRowContext(ctx, `SELECT checked_layout_snapshot FROM pc_native_compaction WHERE source_id=$1`, r.source).Scan(&layout); err != nil || layout != 20 {
		t.Fatal("stale claim altered successor state", layout, err)
	}
}
