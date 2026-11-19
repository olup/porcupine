package lake

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/event"
	identityapi "github.com/olup/porcupine/internal/identity"
)

// This component fixture uses the real immutable S3 object implementation. Its
// routing plan is deliberately explicit; it is not the operational PG provider.
type liveProofStager struct {
	objects  *DuckLakeObjectStore
	attempt  string
	selected [][]event.Event
	fail     bool
}

func (s *liveProofStager) StageLiveEffects(ctx context.Context, selected SelectedLiveEffects) (StagedLiveEffects, error) {
	s.selected = append(s.selected, append([]event.Event(nil), selected.Events...))
	for _, e := range selected.Events {
		if e.IdentitySnapshot != "" || e.Event == "$identify" || e.Event == "$set" {
			return StagedLiveEffects{}, errors.New("private identity leaked to outputs")
		}
	}
	raw, err := json.Marshal(selected)
	if err != nil {
		return StagedLiveEffects{}, err
	}
	ref, err := s.objects.PutImmutableEffectObject(ctx, selected.Scope, s.attempt, raw)
	if err != nil {
		return StagedLiveEffects{}, err
	}
	if s.fail {
		return StagedLiveEffects{}, errors.New("failure after immutable upload")
	}
	return StagedLiveEffects{PlanID: "explicit-component-plan-v1", Manifest: ref, OutputCount: int64(len(selected.Events))}, nil
}
func newLiveInput(t *testing.T, project, name, uuid, target, properties string) LiveCommitInput {
	t.Helper()
	e := event.Event{ProjectID: project, UUID: uuid, Event: name, DistinctID: target, Properties: json.RawMessage(properties), Timestamp: "2026-11-17T12:00:00Z", ReceivedAt: "2026-11-17T12:00:01Z"}
	input := LiveCommitInput{Event: e, AdmittedAt: time.Date(2026, 11, 16, 23, 59, 59, 0, time.UTC)}
	if name == "$identify" || name == "$set" {
		command, err := identityapi.Parse(name, target, e.Properties)
		if err != nil {
			t.Fatal(err)
		}
		input.Identity = &command
		input.Event.DistinctID = ""
		input.Event.Properties = json.RawMessage(`{}`)
	}
	return input
}
func commitLiveFixture(t *testing.T, store *DuckLakeStore, session *ProjectWriteSession, batch LiveCommitBatch, stager LiveEffectStager) LiveCommitResult {
	t.Helper()
	reservation, err := session.ReserveEventIDs(t.Context(), batch.Scope.StreamID, batch.ID, len(batch.Inputs))
	if err != nil {
		t.Fatal(err)
	}
	var result LiveCommitResult
	err = session.RunNative(t.Context(), func(ctx context.Context) error {
		var err error
		result, err = store.CommitLive(ctx, session.Fence(batch.Scope.StreamID), batch, reservation, stager)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}
func TestDuckLakeS3SharedPostgresLiveCommitProof(t *testing.T) {
	t.Parallel()
	ctx, a, b, config := sharedNativeStores(t)
	control, coordinator := nativeFenceControl(t)
	if _, err := control.ExecContext(ctx, `CREATE SEQUENCE shared_event_id_sequence AS BIGINT INCREMENT BY 1024 MINVALUE 1 CACHE 1 NO CYCLE`); err != nil {
		t.Fatal(err)
	}
	key := ProjectWriteKey{SourceID: config.SharedSourceID, ProjectID: "live-proof"}
	scope, err := a.InitializeHostedProject(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	owner := acquireFenceSession(t, coordinator, key)
	fence := owner.Fence(scope.StreamID)
	if err := a.AdvanceWriteFence(ctx, fence); err != nil {
		t.Fatal(err)
	}
	reservation, err := owner.ReserveEventIDs(ctx, scope.StreamID, "range-check", 1025)
	if err != nil || len(reservation.ids) != 1025 {
		t.Fatal(reservation, err)
	}
	for i, id := range reservation.ids {
		if id <= 0 || i > 0 && id <= reservation.ids[i-1] {
			t.Fatal("nonmonotone or reused ID", i, id)
		}
	}
	if reservation.matches(fence, "different-batch", 1) {
		t.Fatal("range was transferable to another batch")
	}
	firstTime := time.Date(2026, 11, 17, 12, 3, 4, 123, time.UTC)
	a.liveClock = func() time.Time { return firstTime }
	ordinaryID, identifyID, setID := event.NewID(), event.NewID(), event.NewID()
	ordinary := newLiveInput(t, key.ProjectID, "page_viewed", ordinaryID, "browser", `{"page":"/"}`)
	ordinary.Event.Timestamp = "2026-11-17T14:00:00+02:00"
	ordinary.Event.ReceivedAt = "2026-11-17T14:00:01+02:00"
	identify := newLiveInput(t, key.ProjectID, "$identify", identifyID, "person", `{"$anon_distinct_id":"browser","$set":{"plan":"first"}}`)
	set := newLiveInput(t, key.ProjectID, "$set", setID, "browser", `{"$set":{"plan":"new"}}`)
	batch := LiveCommitBatch{ID: event.NewID(), TenantID: "tenant-a", Scope: scope, Inputs: []LiveCommitInput{ordinary, ordinary, identify, set}}
	stager := &liveProofStager{objects: a.Remote, attempt: fence.AcquisitionID}
	result := commitLiveFixture(t, a, owner, batch, stager)
	committed, ok := result.(CommittedLiveBatch)
	if !ok || !committed.Fresh || committed.Evidence.AcceptedLive != 3 || committed.Evidence.Sequence != 1 || !committed.Evidence.AccountedAt.Equal(firstTime) {
		t.Fatal(result)
	}
	if len(stager.selected) != 1 || len(stager.selected[0]) != 1 || stager.selected[0][0].UUID != ordinaryID {
		t.Fatal("outputs were not restricted to new ordinary UUIDs", stager.selected)
	}
	if len(committed.Evidence.Totals) != 1 || committed.Evidence.Totals[0].Live != 3 || committed.Evidence.Totals[0].Day != "2026-11-17T00:00:00Z" {
		t.Fatal(committed.Evidence.Totals)
	}
	if stager.selected[0][0].Timestamp != "2026-11-17T12:00:00Z" || stager.selected[0][0].ReceivedAt != "2026-11-17T12:00:01Z" {
		t.Fatal("stager received noncanonical timestamp", stager.selected[0][0])
	}
	originalBody, err := a.Remote.ReadImmutableEffectObject(ctx, scope, committed.Evidence.Effects.Manifest)
	if err != nil || !json.Valid(originalBody) {
		t.Fatal(string(originalBody), err)
	}
	pa, release, err := a.LeaseProject(ctx, key.ProjectID)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	assertIdentity := func(expected string) {
		t.Helper()
		var traits string
		var identified bool
		if err := pa.DB.QueryRowContext(ctx, `SELECT traits,is_identified FROM lake.identity_profiles WHERE identified_id='person'`).Scan(&traits, &identified); err != nil || traits != expected || !identified {
			t.Fatal(traits, identified, err)
		}
	}
	assertIdentity(`{"plan":"new"}`)
	var firstID int64
	if err := pa.DB.QueryRowContext(ctx, `SELECT id FROM lake.current_events WHERE uuid=?`, ordinaryID).Scan(&firstID); err != nil {
		t.Fatal(err)
	}
	// An upload followed by failure leaves no canonical event, identity change,
	// usage increment or proof. Its orphan object is not a committed output.
	failed := LiveCommitBatch{ID: "failed-batch", TenantID: batch.TenantID, Scope: scope, Inputs: []LiveCommitInput{newLiveInput(t, key.ProjectID, "$set", event.NewID(), "person", `{"$set":{"plan":"bad"}}`)}}
	failedIDs, err := owner.ReserveEventIDs(ctx, scope.StreamID, failed.ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	stager.fail = true
	if _, err := a.CommitLive(ctx, fence, failed, failedIDs, stager); err == nil {
		t.Fatal("failed stage committed")
	}
	stager.fail = false
	assertIdentity(`{"plan":"new"}`)
	if fenceWitnessCount(t, pa, failed.Inputs[0].Event.UUID) != 0 {
		t.Fatal("failed stage retained UUID")
	}
	page, err := a.PendingNativeEffects(ctx, scope, 0, 10)
	if err != nil || page.NativeHighWater != 1 || len(page.Evidence) != 1 {
		t.Fatal(page, err)
	}
	if err := fenceWitnessTx(ctx, pa, fence, "deleted-uuid", nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	// Lose the physical owner and recover original commit evidence on an
	// independent engine with a fresh numeric reservation and a different clock.
	terminateFenceSession(t, control, owner)
	successor := acquireFenceSession(t, coordinator, key)
	next := successor.Fence(scope.StreamID)
	b.liveClock = func() time.Time { return firstTime.Add(24 * time.Hour) }
	recovered := &liveProofStager{objects: b.Remote, attempt: next.AcquisitionID}
	replay := commitLiveFixture(t, b, successor, batch, recovered)
	if got, ok := replay.(CommittedLiveBatch); !ok || got.Fresh || !reflect.DeepEqual(got.Evidence, committed.Evidence) {
		t.Fatal("replay changed original proof", replay, committed)
	}
	if len(recovered.selected) != 0 {
		t.Fatal("replay staged outputs again")
	}
	page, err = b.PendingNativeEffects(ctx, scope, 0, 10)
	if err != nil || len(page.Evidence) != 1 || !reflect.DeepEqual(page.Evidence[0], committed.Evidence) {
		t.Fatal(page, err)
	}
	// A different batch deduplicates both a retained UUID and a deleted head.
	changed := identify
	command, err := identityapi.Parse("$identify", "person", json.RawMessage(`{"$set":{"plan":"must-not-run"}}`))
	if err != nil {
		t.Fatal(err)
	}
	changed.Identity = &command
	second := LiveCommitBatch{ID: "live-01adcf5d-8600-7000-8000-000000000001", TenantID: batch.TenantID, Scope: scope, Inputs: []LiveCommitInput{changed, newLiveInput(t, key.ProjectID, "page_viewed", "deleted-uuid", "browser", `{}`), newLiveInput(t, key.ProjectID, "page_viewed", event.NewID(), "person", `{}`)}}
	secondIDs, err := successor.ReserveEventIDs(ctx, scope.StreamID, second.ID, len(second.Inputs))
	if err != nil {
		t.Fatal(err)
	}
	commitSecond := func(checkpoint NativeEffectCheckpoint) (LiveCommitResult, error) {
		return b.CommitLiveFromCheckpoint(ctx, next, second, secondIDs, recovered, checkpoint, func(_ context.Context, commit func() error) error {
			return commit()
		})
	}
	if _, err := commitSecond(NativeEffectCheckpoint{}); !errors.Is(err, ErrNativeEffectsPending) {
		t.Fatal("new fence skipped unpublished predecessor", err)
	}
	checkpoint := NativeEffectCheckpoint{Sequence: committed.Evidence.Sequence, Fingerprint: "wrong"}
	if _, err := commitSecond(checkpoint); !errors.Is(err, ErrNativeEvidenceGap) {
		t.Fatal("new fence accepted mismatched predecessor", err)
	}
	if len(recovered.selected) != 0 {
		t.Fatal("invalid checkpoint staged effects")
	}
	checkpoint.Fingerprint = committed.Evidence.ProofFingerprint
	secondResult, err := commitSecond(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.CommitLive(ctx, fence, failed, failedIDs, stager); !errors.Is(err, ErrProjectWriteFenceStale) {
		t.Fatal("combined write let a previous writer commit", err)
	}
	secondProof, ok := secondResult.(CommittedLiveBatch)
	if !ok || !secondProof.Fresh || secondProof.Evidence.AcceptedLive != 1 || secondProof.Evidence.Sequence != 2 || secondProof.Evidence.Totals[0].Live != 1 {
		t.Fatal(secondResult)
	}
	assertIdentity(`{"plan":"new"}`)
	// The engine without the new batch in its write cache must recover the
	// original prefixed proof even with a legacy proof in the same table.
	otherReader := &liveProofStager{objects: a.Remote, attempt: next.AcquisitionID}
	if got, ok := commitLiveFixture(t, a, successor, second, otherReader).(CommittedLiveBatch); !ok || got.Fresh || !reflect.DeepEqual(got.Evidence, secondProof.Evidence) || len(otherReader.selected) != 0 {
		t.Fatal("replay changed the prefixed proof", got, secondProof)
	}
	duplicate := batch
	duplicate.ID = "duplicate-only"
	if _, ok := commitLiveFixture(t, b, successor, duplicate, recovered).(DuplicateLiveBatch); !ok {
		t.Fatal("duplicate batch fabricated new proof")
	}
	page, err = b.PendingNativeEffects(ctx, scope, 0, 10)
	if err != nil || page.NativeHighWater != 2 || page.AcknowledgedThrough != 1 || len(page.Evidence) != 2 {
		t.Fatal(page, err)
	}
	var replayID int64
	if err := pa.DB.QueryRowContext(ctx, `SELECT id FROM lake.current_events WHERE uuid=?`, ordinaryID).Scan(&replayID); err != nil || replayID != firstID {
		t.Fatal("canonical ID changed", firstID, replayID, err)
	}
	if _, err := b.PendingNativeEffects(ctx, scope, 3, 10); !errors.Is(err, ErrNativeEvidenceGap) {
		t.Fatal("future checkpoint was accepted", err)
	}
	if one, err := b.PendingNativeEffects(ctx, scope, 0, 1); err != nil || len(one.Evidence) != 1 || one.Evidence[0].Sequence != 1 {
		t.Fatal("complete bounded page was rejected", one, err)
	}
	if _, err := pa.DB.ExecContext(ctx, `DELETE FROM lake.native_commit_proofs WHERE effect_seq=2`); err != nil {
		t.Fatal(err)
	}
	if _, err := b.PendingNativeEffects(ctx, scope, 0, 10); !errors.Is(err, ErrNativeEvidenceGap) {
		t.Fatal("short page masked missing final proof", err)
	}
	if _, err := pa.DB.ExecContext(ctx, `DELETE FROM lake.native_commit_proofs WHERE effect_seq=1`); err != nil {
		t.Fatal(err)
	}
	if _, err := b.PendingNativeEffects(ctx, scope, 0, 10); !errors.Is(err, ErrNativeEvidenceGap) {
		t.Fatal("missing proof history was skipped", err)
	}
}
