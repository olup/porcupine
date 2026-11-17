package lake

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"
)

func nativeFenceControl(t *testing.T) (*sql.DB, *PostgresProjectCoordinator) {
	t.Helper()
	db, err := sql.Open("pgx", sharedPostgresDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(8)
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.ExecContext(t.Context(), ProjectCoordinatorSchemaV1); err != nil {
		t.Fatal(err)
	}
	c := DefaultProjectCoordinatorConfig()
	c.MaxConcurrent = 2
	c.ProbeInterval = 10 * time.Millisecond
	c.ProbeTimeout = time.Second
	coordinator, err := NewPostgresProjectCoordinator(db, c)
	if err != nil {
		t.Fatal(err)
	}
	return db, coordinator
}
func acquireFenceSession(t *testing.T, c *PostgresProjectCoordinator, key ProjectWriteKey) *ProjectWriteSession {
	t.Helper()
	s, err := c.Acquire(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Release() })
	return s
}
func terminateFenceSession(t *testing.T, control *sql.DB, s *ProjectWriteSession) {
	t.Helper()
	var stopped bool
	if err := control.QueryRowContext(t.Context(), "SELECT pg_terminate_backend($1)", s.pid).Scan(&stopped); err != nil || !stopped {
		t.Fatal("could not terminate actual owner backend", stopped, err)
	}
	select {
	case <-s.Context().Done():
		if !errors.Is(context.Cause(s.Context()), ErrProjectWriteSessionLost) {
			t.Fatal(context.Cause(s.Context()))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("owner did not observe loss of its physical control backend")
	}
}
func fenceWitnessTx(ctx context.Context, p *ProjectLake, fence ProjectWriteFence, marker string, beforeBegin, afterGuard <-chan struct{}, entered chan<- struct{}) error {
	if beforeBegin != nil {
		entered <- struct{}{}
		<-beforeBegin
	}
	tx, err := p.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := p.acquireHostedNativeWrite(ctx, tx, fence); err != nil {
		return err
	}
	// A tombstone is a real durable native row and needs no event payload. It is
	// committed in the SAME transaction as the fence's generation change.
	if _, err := tx.ExecContext(ctx, `INSERT INTO lake.heads VALUES(?,1,0,true,'2026-11-17T00:00:00Z')`, marker); err != nil {
		return err
	}
	if afterGuard != nil {
		entered <- struct{}{}
		<-afterGuard
	}
	return tx.Commit()
}
func fenceWitnessCount(t *testing.T, p *ProjectLake, marker string) int {
	t.Helper()
	var count int
	if err := p.DB.QueryRowContext(t.Context(), "SELECT count(*) FROM lake.heads WHERE uuid=?", marker).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}
func TestDuckLakeS3SharedPostgresEpochFence(t *testing.T) {
	t.Parallel()
	ctx, a, b, config := sharedNativeStores(t)
	control, coordinator := nativeFenceControl(t)
	for _, point := range []string{"before advance", "before begin", "after guard"} {
		t.Run(point, func(t *testing.T) {
			key := ProjectWriteKey{SourceID: config.SharedSourceID, ProjectID: "fence-" + point}
			scope, err := a.InitializeHostedProject(ctx, key)
			if err != nil {
				t.Fatal(err)
			}
			pa, releaseA, err := a.LeaseProject(ctx, key.ProjectID)
			if err != nil {
				t.Fatal(err)
			}
			defer releaseA()
			pb, releaseB, err := b.LeaseProject(ctx, key.ProjectID)
			if err != nil {
				t.Fatal(err)
			}
			defer releaseB()
			owner := acquireFenceSession(t, coordinator, key)
			oldFence := owner.Fence(scope.StreamID)
			if point != "before advance" {
				if err := a.AdvanceWriteFence(ctx, oldFence); err != nil {
					t.Fatal(err)
				}
			}
			enter, release, finished := make(chan struct{}, 1), make(chan struct{}), make(chan error, 1)
			defer close(release)
			// This actual native writer deliberately ignores owner.Context cancellation.
			// The harness still joins it rather than treating lease loss as completion.
			nativeCtx, cancel := context.WithTimeout(context.WithoutCancel(owner.Context()), 30*time.Second)
			defer cancel()
			go func() {
				switch point {
				case "before advance":
					enter <- struct{}{}
					<-release
					finished <- a.AdvanceWriteFence(nativeCtx, oldFence)
				case "before begin":
					finished <- fenceWitnessTx(nativeCtx, pa, oldFence, "old", release, nil, enter)
				case "after guard":
					finished <- fenceWitnessTx(nativeCtx, pa, oldFence, "old", nil, release, enter)
				}
			}()
			select {
			case <-enter:
			case <-time.After(3 * time.Second):
				t.Fatal("old writer did not reach pause point")
			}
			terminateFenceSession(t, control, owner)
			successor := acquireFenceSession(t, coordinator, key)
			newFence := successor.Fence(scope.StreamID)
			if newFence.Epoch <= oldFence.Epoch {
				t.Fatal("successor epoch did not increase", oldFence.Epoch, newFence.Epoch)
			}
			if err := b.AdvanceWriteFence(ctx, newFence); err != nil {
				t.Fatal("successor did not fence old writer", err)
			}
			if fenceWitnessCount(t, pb, "old") != 0 {
				t.Fatal("uncommitted stale witness became visible")
			}
			select {
			case err := <-finished:
				t.Fatal("lease loss falsely completed blocked native writer", err)
			default:
			}
			release <- struct{}{}
			select {
			case err := <-finished:
				if point == "after guard" {
					if !isSharedNativeConflict(err) {
						t.Fatal("stale native commit was not a typed conflict", err)
					}
				} else if !errors.Is(err, ErrProjectWriteFenceStale) {
					t.Fatal("stale epoch was refreshed or accepted", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("actual old native return was not observed")
			}
			if fenceWitnessCount(t, pb, "old") != 0 {
				t.Fatal("stale native writer committed after successor fence")
			}
			if err := fenceWitnessTx(ctx, pb, newFence, "new", nil, nil, nil); err != nil {
				t.Fatal("new owner could not commit witness", err)
			}
			if fenceWitnessCount(t, pa, "new") != 1 {
				t.Fatal("new owner witness was not durable across engines")
			}
			if err := a.AdvanceWriteFence(ctx, oldFence); !errors.Is(err, ErrProjectWriteFenceStale) {
				t.Fatal("delayed advance lowered current epoch", err)
			}
			if err := owner.Release(); !errors.Is(err, ErrProjectWriteSessionLost) {
				t.Fatal("terminated session release claimed healthy ownership", err)
			}
			if err := successor.Release(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestDuckLakeS3SharedPostgresEpochFenceEarlierCommitAndScope(t *testing.T) {
	t.Parallel()
	ctx, a, b, config := sharedNativeStores(t)
	control, coordinator := nativeFenceControl(t)
	key := ProjectWriteKey{SourceID: config.SharedSourceID, ProjectID: "earlier"}
	scope, err := a.InitializeHostedProject(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	owner := acquireFenceSession(t, coordinator, key)
	fence := owner.Fence(scope.StreamID)
	if err := a.AdvanceWriteFence(ctx, fence); err != nil {
		t.Fatal(err)
	}
	pa, doneA, err := a.LeaseProject(ctx, key.ProjectID)
	if err != nil {
		t.Fatal(err)
	}
	defer doneA()
	if err := fenceWitnessTx(ctx, pa, fence, "earlier", nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	terminateFenceSession(t, control, owner)
	successor := acquireFenceSession(t, coordinator, key)
	next := successor.Fence(scope.StreamID)
	if err := b.AdvanceWriteFence(ctx, next); err != nil {
		t.Fatal(err)
	}
	pb, doneB, err := b.LeaseProject(ctx, key.ProjectID)
	if err != nil {
		t.Fatal(err)
	}
	defer doneB()
	if fenceWitnessCount(t, pb, "earlier") != 1 {
		t.Fatal("valid earlier commit was lost during takeover")
	}
	if err := b.AdvanceWriteFence(ctx, next); err != nil {
		t.Fatal("same acquisition could not verify ambiguous fence response", err)
	}
	wrongNonce := next
	wrongNonce.AcquisitionID = "another-acquisition"
	wrongStream := next
	wrongStream.StreamID = "another-stream"
	for _, wrong := range []ProjectWriteFence{wrongNonce, wrongStream, fence} {
		if err := b.AdvanceWriteFence(ctx, wrong); !errors.Is(err, ErrProjectWriteFenceStale) {
			t.Fatal("foreign nonce/stream or old epoch accepted", wrong, err)
		}
	}
	if err := successor.Release(); err != nil {
		t.Fatal(err)
	}
}
func TestDuckLakeS3SharedPostgresHostedCutoverRefusesLegacyIdentity(t *testing.T) {
	t.Parallel()
	ctx, a, _, config := sharedNativeStores(t)
	if err := a.Apply(ctx, []EventProjection{lakeStorageProjection(t, "legacy", "existing", 1)}); err != nil {
		t.Fatal(err)
	}
	if _, err := a.InitializeHostedProject(ctx, ProjectWriteKey{SourceID: config.SharedSourceID, ProjectID: "legacy"}); err == nil {
		t.Fatal("populated legacy authority was silently inferred")
	}
	p, release, err := a.LeaseProject(ctx, "legacy")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	var version int
	if err := p.DB.QueryRowContext(ctx, "SELECT max(version) FROM lake.schema_version").Scan(&version); err != nil || version != lakeSchemaVersion {
		t.Fatal("refused cutover changed schema", version, err)
	}
	if fenceWitnessCount(t, p, "existing") != 1 {
		t.Fatal("refused cutover lost existing state")
	}
}
func TestProjectCoordinatorBounds(t *testing.T) {
	t.Parallel()
	if _, err := NewPostgresProjectCoordinator(nil, DefaultProjectCoordinatorConfig()); err == nil {
		t.Fatal("nil control accepted")
	}
}
