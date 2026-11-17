package lake

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/event"
)

func TestDuckLakeS3SharedPostgresRecoveryExistingOnly(t *testing.T) {
	t.Parallel()
	ctx, a, b, config := sharedNativeStores(t)
	unknown := ProjectWriteKey{config.SharedSourceID, "unknown"}
	if _, err := b.LookupHostedProjectExisting(ctx, unknown); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("unknown project did not fail closed", err)
	}
	if _, err := os.Stat(b.ProjectDirectory(unknown.ProjectID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("recovery allocated worker directory", err)
	}
	var count int
	if err := a.SharedCatalog.QueryRowContext(ctx, `SELECT count(*) FROM pc_native_projects WHERE source_id=$1 AND project_id=$2`, unknown.SourceID, unknown.ProjectID).Scan(&count); err != nil || count != 0 {
		t.Fatal("recovery registered unknown project", count, err)
	}
	if len(b.Projects) != 0 {
		t.Fatal("recovery allocated unknown engine")
	}
	key := ProjectWriteKey{config.SharedSourceID, "existing"}
	scope, err := a.InitializeHostedProject(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	p, release, err := a.LeaseProject(ctx, key.ProjectID)
	if err != nil {
		t.Fatal(err)
	}
	var before int64
	if err := p.DB.QueryRowContext(ctx, `SELECT max(snapshot_id) FROM ducklake_snapshots('lake')`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	release()
	got, err := b.LookupHostedProjectExisting(ctx, key)
	if err != nil || got != scope {
		t.Fatal(got, err)
	}
	var after int64
	if err := b.Projects[key.ProjectID].DB.QueryRowContext(ctx, `SELECT max(snapshot_id) FROM ducklake_snapshots('lake')`).Scan(&after); err != nil || after != before {
		t.Fatal("cold recovery wrote native metadata", before, after, err)
	}
	if _, err := os.Stat(filepath.Join(b.Root, "registry.sqlite")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("recovery created local authority", err)
	}
	// A ready registry row with absent native metadata must never bootstrap it.
	missing := ProjectWriteKey{config.SharedSourceID, "missing-catalog"}
	if _, err := a.SharedCatalog.ExecContext(ctx, `INSERT INTO pc_native_projects(source_id,project_id,catalog_id,ready) VALUES($1,$2,$3,1)`, missing.SourceID, missing.ProjectID, SharedNativeCatalogID(missing.SourceID, missing.ProjectID)); err != nil {
		t.Fatal(err)
	}
	if _, err := b.LookupHostedProjectExisting(ctx, missing); err == nil {
		t.Fatal("missing catalog recreated")
	}
	var exists bool
	if err := a.SharedCatalog.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname=$1)`, SharedNativeSchema(SharedNativeCatalogID(missing.SourceID, missing.ProjectID))).Scan(&exists); err != nil || exists {
		t.Fatal("recovery created missing native schema", exists, err)
	}
	// A deletion-pending/tombstone record remains in inventory and cannot reopen.
	if _, err := a.SharedCatalog.ExecContext(ctx, `UPDATE pc_native_projects SET deleted=1 WHERE source_id=$1 AND project_id=$2`, key.SourceID, key.ProjectID); err != nil {
		t.Fatal(err)
	}
	records, err := b.ScanHostedProjects(ctx, "", 256)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, r := range records {
		if r.Key == key {
			found = true
			if !r.Deleted {
				t.Fatal("inventory lost deletion truth")
			}
		}
	}
	if !found {
		t.Fatal("inventory dropped deleted record")
	}
	if _, err := b.LookupHostedProjectExisting(ctx, key); !errors.Is(err, errSharedNativeDeleted) {
		t.Fatal("cached recovery ignored tombstone", err)
	}
	if _, err := b.ScanHostedProjects(ctx, "", 257); err == nil {
		t.Fatal("unbounded inventory accepted")
	}
}

func TestDuckLakeS3SharedPostgresRecoverySealAndACK(t *testing.T) {
	t.Parallel()
	ctx, a, b, config := sharedNativeStores(t)
	control, coordinator := nativeFenceControl(t)
	if _, err := control.ExecContext(ctx, `CREATE SEQUENCE shared_event_id_sequence AS BIGINT INCREMENT BY 1024 MINVALUE 1 CACHE 1 NO CYCLE`); err != nil {
		t.Fatal(err)
	}
	key := ProjectWriteKey{config.SharedSourceID, "recover-sealed"}
	scope, err := a.InitializeHostedProject(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	owner := acquireFenceSession(t, coordinator, key)
	fence := owner.Fence(scope.StreamID)
	if err := a.AdvanceWriteFence(ctx, fence); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 2; i++ {
		batch := LiveCommitBatch{ID: fmt.Sprintf("batch-%d", i), TenantID: "tenant-a", Scope: scope, Inputs: []LiveCommitInput{newLiveInput(t, key.ProjectID, "viewed", event.NewID(), "person", `{}`)}}
		commitLiveFixture(t, a, owner, batch, &liveProofStager{objects: a.Remote, attempt: fence.AcquisitionID})
	}
	if err := a.AcknowledgeNativeEffects(ctx, fence, scope, 3); !errors.Is(err, ErrNativeEvidenceGap) {
		t.Fatal("future ACK accepted", err)
	}
	if err := a.AcknowledgeNativeEffects(ctx, fence, scope, 1); err != nil {
		t.Fatal(err)
	}
	sealed, err := a.SealHostedProject(ctx, fence)
	if err != nil || sealed.FinalSequence != 2 || sealed.AcknowledgedThrough != 1 {
		t.Fatal(sealed, err)
	}
	if retry, err := a.SealHostedProject(ctx, fence); err != nil || retry != sealed {
		t.Fatal("seal retry changed cut", retry, err)
	}
	if err := owner.Release(); err != nil {
		t.Fatal(err)
	}
	// This worker has never opened the native engine. Recovery needs no producer.
	if len(b.Projects) != 0 {
		t.Fatal("second worker was not cold")
	}
	got, err := b.LookupHostedProjectExisting(ctx, key)
	if err != nil || got != scope {
		t.Fatal("sealed cold discovery", got, err)
	}
	next := acquireFenceSession(t, coordinator, key)
	nextFence := next.Fence(scope.StreamID)
	if err := b.AdvanceWriteFence(ctx, nextFence); !errors.Is(err, ErrProjectWriteFenceStale) {
		t.Fatal("sealed writer reopened", err)
	}
	if err := b.AdvanceRecoveryFence(ctx, nextFence); err != nil {
		t.Fatal(err)
	}
	page, err := b.PendingNativeEffects(ctx, scope, 0, 1)
	if err != nil || page.NativeHighWater != 2 || len(page.Evidence) != 1 || page.Evidence[0].Sequence != 1 {
		t.Fatal(page, err)
	}
	if _, err := b.Remote.ReadImmutableEffectObject(ctx, scope, page.Evidence[0].Effects.Manifest); err != nil {
		t.Fatal("original immutable effect unavailable", err)
	}
	if err := a.AcknowledgeNativeEffects(ctx, fence, scope, 2); !errors.Is(err, ErrProjectWriteFenceStale) {
		t.Fatal("old epoch ACK accepted", err)
	}
	if err := b.AcknowledgeNativeEffects(ctx, nextFence, scope, 2); err != nil {
		t.Fatal(err)
	}
	// Lost ACK responses and backward checkpoint replays converge monotonically.
	for _, through := range []int64{2, 1, 0} {
		if err := b.AcknowledgeNativeEffects(ctx, nextFence, scope, through); err != nil {
			t.Fatal(err)
		}
	}
	page, err = b.PendingNativeEffects(ctx, scope, 2, 1)
	if err != nil || page.AcknowledgedThrough != 2 {
		t.Fatal(page, err)
	}
	p := b.Projects[key.ProjectID]
	var proofs int
	if err := p.DB.QueryRowContext(ctx, `SELECT count(*) FROM lake.native_commit_proofs`).Scan(&proofs); err != nil || proofs != 2 {
		t.Fatal("ACK pruned proof", proofs, err)
	}
	if _, err := p.DB.ExecContext(ctx, `DELETE FROM lake.native_commit_proofs WHERE effect_seq=1`); err != nil {
		t.Fatal(err)
	}
	if err := b.AcknowledgeNativeEffects(ctx, nextFence, scope, 2); !errors.Is(err, ErrNativeEvidenceGap) {
		t.Fatal("ACK accepted missing retained history", err)
	}
	if _, err := b.PendingNativeEffects(ctx, scope, 2, 1); !errors.Is(err, ErrNativeEvidenceGap) {
		t.Fatal("covered cursor hid missing history", err)
	}
	if _, err := a.SealHostedProject(ctx, fence); !errors.Is(err, ErrProjectWriteFenceStale) {
		t.Fatal("stale seal accepted", err)
	}
}

func TestSharedPostgresCoordinatorReleaseJoinsNativeCall(t *testing.T) {
	t.Parallel()
	_, coordinator := nativeFenceControl(t)
	session := acquireFenceSession(t, coordinator, ProjectWriteKey{"drain-source", "drain-project"})
	entered, unblock, returned := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		returned <- session.RunNative(t.Context(), func(ctx context.Context) error {
			close(entered)
			<-unblock
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return nil
		})
	}()
	<-entered
	released := make(chan error, 1)
	go func() { released <- session.Release() }()
	<-session.stopProbe
	select {
	case err := <-released:
		t.Fatal("release confused cancellation with native return", err)
	default:
	}
	if err := session.RunNative(t.Context(), func(context.Context) error { t.Error("late native call ran"); return nil }); !errors.Is(err, ErrProjectWriteSessionLost) {
		t.Fatal(err)
	}
	close(unblock)
	select {
	case err := <-returned:
		if err != nil {
			t.Fatal("normal release canceled active native work", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("native callback did not return")
	}
	select {
	case err := <-released:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("release did not join actual native call")
	}
}

type blockedRecoveryStager struct {
	inner   LiveEffectStager
	entered chan struct{}
	resume  <-chan struct{}
}

func (s *blockedRecoveryStager) StageLiveEffects(ctx context.Context, selected SelectedLiveEffects) (StagedLiveEffects, error) {
	close(s.entered)
	<-s.resume
	return s.inner.StageLiveEffects(ctx, selected)
}

func TestDuckLakeS3SharedPostgresSealJoinsActiveCommit(t *testing.T) {
	t.Parallel()
	ctx, a, _, config := sharedNativeStores(t)
	control, coordinator := nativeFenceControl(t)
	if _, err := control.ExecContext(ctx, `CREATE SEQUENCE shared_event_id_sequence AS BIGINT INCREMENT BY 1024 MINVALUE 1 CACHE 1 NO CYCLE`); err != nil {
		t.Fatal(err)
	}
	key := ProjectWriteKey{config.SharedSourceID, "seal-active"}
	scope, err := a.InitializeHostedProject(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	owner := acquireFenceSession(t, coordinator, key)
	fence := owner.Fence(scope.StreamID)
	if err := a.AdvanceWriteFence(ctx, fence); err != nil {
		t.Fatal(err)
	}
	batch := LiveCommitBatch{ID: "active", TenantID: "tenant-a", Scope: scope, Inputs: []LiveCommitInput{newLiveInput(t, key.ProjectID, "viewed", event.NewID(), "person", `{}`)}}
	reservation, err := owner.ReserveEventIDs(ctx, scope.StreamID, batch.ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	resume := make(chan struct{})
	stager := &blockedRecoveryStager{inner: &liveProofStager{objects: a.Remote, attempt: fence.AcquisitionID}, entered: make(chan struct{}), resume: resume}
	committed := make(chan error, 1)
	go func() {
		committed <- owner.RunNative(ctx, func(ctx context.Context) error {
			_, err := a.CommitLive(ctx, fence, batch, reservation, stager)
			return err
		})
	}()
	<-stager.entered
	// A real commit is now holding p.write inside its actual native transaction.
	sealStarted := make(chan struct{})
	sealed := make(chan HostedProjectSeal, 1)
	sealErr := make(chan error, 1)
	go func() {
		sealErr <- owner.RunNative(ctx, func(ctx context.Context) error {
			close(sealStarted)
			cut, err := a.SealHostedProject(ctx, fence)
			sealed <- cut
			return err
		})
	}()
	<-sealStarted
	select {
	case err := <-sealErr:
		close(resume)
		t.Fatal("seal returned while native commit was active", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(resume)
	if err := <-committed; err != nil {
		t.Fatal(err)
	}
	if err := <-sealErr; err != nil {
		t.Fatal(err)
	}
	if cut := <-sealed; cut.FinalSequence != 1 {
		t.Fatal("seal omitted admitted active commit", cut)
	}
	later := LiveCommitBatch{ID: "after-seal", TenantID: "tenant-a", Scope: scope, Inputs: []LiveCommitInput{newLiveInput(t, key.ProjectID, "viewed", event.NewID(), "person", `{}`)}}
	laterReservation, err := owner.ReserveEventIDs(ctx, scope.StreamID, later.ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.CommitLive(ctx, fence, later, laterReservation, &liveProofStager{objects: a.Remote, attempt: fence.AcquisitionID}); !errors.Is(err, ErrProjectWriteFenceStale) {
		t.Fatal("post-seal commit accepted", err)
	}
	if cut, err := a.SealHostedProject(ctx, fence); err != nil || cut.FinalSequence != 1 {
		t.Fatal("final cut moved", cut, err)
	}
}

func TestDuckLakeS3SharedPostgresHostedLegacyPathsFailClosed(t *testing.T) {
	t.Parallel()
	ctx, a, b, config := sharedNativeStores(t)
	key := ProjectWriteKey{config.SharedSourceID, "guarded"}
	scope, err := a.InitializeHostedProject(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Apply(ctx, []EventProjection{lakeStorageProjection(t, key.ProjectID, "legacy-bypass", 1)}); !errors.Is(err, ErrHostedNativeGuardRequired) {
		t.Fatal("legacy mutation bypassed hosted protocol", err)
	}
	p, release, err := a.LeaseProject(ctx, key.ProjectID)
	if err != nil {
		t.Fatal(err)
	}
	stats := DuckLakeMaintenanceStats{}
	if err := p.MaintenanceCommand(ctx, "CALL ducklake_merge_adjacent_files('lake')", &stats); !errors.Is(err, ErrHostedNativeGuardRequired) {
		t.Fatal("unguarded hosted compaction ran", err)
	}
	release()
	if err := a.DeleteProject(ctx, key.ProjectID); !errors.Is(err, ErrHostedNativeGuardRequired) {
		t.Fatal("legacy deletion bypassed publication/drain", err)
	}
	got, err := b.LookupHostedProjectExisting(ctx, key)
	if err != nil || got != scope {
		t.Fatal("refused deletion damaged native scope", got, err)
	}
	// The old shared-native deletion contract still deletes only A and retains B.
	if err := a.Apply(ctx, []EventProjection{lakeStorageProjection(t, "legacy-a", "legacy-a", 1), lakeStorageProjection(t, "legacy-b", "legacy-b", 2)}); err != nil {
		t.Fatal(err)
	}
	if err := a.DeleteProject(ctx, "legacy-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.LookupHostedProjectExisting(ctx, ProjectWriteKey{config.SharedSourceID, "legacy-a"}); !errors.Is(err, errSharedNativeDeleted) {
		t.Fatal("deleted A reopened", err)
	}
	if got := lakeStorageCount(t, b, "legacy-b", "events"); got != 1 {
		t.Fatal("A deletion damaged B", got)
	}
}

func TestDuckLakeS3SharedPostgresRecoveryMetadataUnavailable(t *testing.T) {
	t.Parallel()
	ctx, a, b, config := sharedNativeStores(t)
	key := ProjectWriteKey{config.SharedSourceID, "metadata-unavailable"}
	if _, err := a.InitializeHostedProject(ctx, key); err != nil {
		t.Fatal(err)
	}
	if err := b.SharedCatalog.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := b.LookupHostedProjectExisting(ctx, key); err == nil {
		t.Fatal("recovery ignored unavailable metadata")
	}
	if _, err := b.ScanHostedProjects(ctx, "", 1); err == nil {
		t.Fatal("inventory ignored unavailable metadata")
	}
	if len(b.Projects) != 0 {
		t.Fatal("metadata failure opened engine")
	}
	if _, err := os.Stat(b.ProjectDirectory(key.ProjectID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("metadata failure allocated native project", err)
	}
}

func TestDuckLakeS3SharedPostgresRecoveryTwoColdWorkers(t *testing.T) {
	t.Parallel()
	ctx, a, b, config := sharedNativeStores(t)
	_, coordinator := nativeFenceControl(t)
	key := ProjectWriteKey{config.SharedSourceID, "two-cold"}
	scope, err := a.InitializeHostedProject(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	owner := acquireFenceSession(t, coordinator, key)
	if err := a.AdvanceWriteFence(ctx, owner.Fence(scope.StreamID)); err != nil {
		t.Fatal(err)
	}
	if cut, err := a.SealHostedProject(ctx, owner.Fence(scope.StreamID)); err != nil || cut.FinalSequence != 0 {
		t.Fatal(cut, err)
	}
	if err := owner.Release(); err != nil {
		t.Fatal(err)
	}
	config.Directory = filepath.Join(t.TempDir(), "third")
	store, err := NewDuckLake(config)
	if err != nil {
		t.Fatal(err)
	}
	c := store.(*DuckLakeStore)
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Error(err)
		}
	})
	var earlier ProjectWriteFence
	for _, worker := range []*DuckLakeStore{b, c} {
		if len(worker.Projects) != 0 {
			t.Fatal("worker already warm")
		}
		session := acquireFenceSession(t, coordinator, key)
		err := session.RunNative(ctx, func(ctx context.Context) error {
			got, err := worker.LookupHostedProjectExisting(ctx, key)
			if err != nil {
				return err
			}
			if got != scope {
				return errors.New("cold worker changed stream")
			}
			fence := session.Fence(got.StreamID)
			if err := worker.AdvanceRecoveryFence(ctx, fence); err != nil {
				return err
			}
			if err := worker.AcknowledgeNativeEffects(ctx, fence, got, 0); err != nil {
				return err
			}
			page, err := worker.PendingNativeEffects(ctx, got, 0, 1)
			if err != nil {
				return err
			}
			if page.NativeHighWater != 0 || page.AcknowledgedThrough != 0 {
				return errors.New("empty sealed stream changed during recovery")
			}
			if earlier.Epoch > 0 {
				if err := b.AcknowledgeNativeEffects(ctx, earlier, got, 0); !errors.Is(err, ErrProjectWriteFenceStale) {
					return fmt.Errorf("stale recovery worker accepted: %v", err)
				}
			}
			earlier = fence
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := session.Release(); err != nil {
			t.Fatal(err)
		}
	}
}
