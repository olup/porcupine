package lake

import "testing"

func TestDuckLakeHostedDeletionAtomicTargets(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	s := openLakeStorageTest(t, t.TempDir(), 1)
	const project = "project"
	first := lakeStorageProjection(t, project, "first", 1)
	second := lakeStorageProjection(t, project, "second", 2)
	kept := lakeStorageProjection(t, project, "kept", 3)
	if err := s.Apply(ctx, []EventProjection{first, second, kept}); err != nil {
		t.Fatal(err)
	}
	p, release, err := s.LeaseProject(ctx, project)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	deletion := HostedEventDeletion{Revision: 2, Targets: []HostedEventDeletionTarget{
		{UUID: first.UUID}, {UUID: second.UUID, ExpectedRevision: 1},
	}}
	apply := func(wantErrorCode string) {
		t.Helper()
		tx, err := p.DB.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		input := HostedMutationInput{Scope: HostedProjectScope{Key: ProjectWriteKey{ProjectID: project}}, Deletion: &deletion}
		outcome, err := applyHostedMutation(ctx, tx, input, PreparedLakeProjection{})
		if err != nil || outcome.ErrorCode != wantErrorCode || outcome.MatchedCount == nil || *outcome.MatchedCount != 2 {
			t.Fatalf("outcome %+v, error %v, want error code %q", outcome, err, wantErrorCode)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	apply("head_conflict")
	if count := lakeStorageCount(t, s, project, "events"); count != 3 {
		t.Fatalf("stale selection changed event count to %d", count)
	}
	deletion.Targets[1].ExpectedRevision = second.Revision
	apply("")
	if count := lakeStorageCount(t, s, project, "events"); count != 1 {
		t.Fatalf("selected tombstones changed event count to %d, want 1", count)
	}
	for _, want := range []EventProjection{first, second} {
		got, err := s.Load(ctx, project, want.UUID)
		if err != nil || !got.IsDeleted || got.ID != want.ID || got.ReceivedAt != want.ReceivedAt || got.Revision != deletion.Revision {
			t.Fatalf("wrong tombstone for %s: %+v, %v", want.UUID, got, err)
		}
	}
}
