package porcupine

import (
	"database/sql"
	"errors"
	"testing"

	lakeapi "github.com/olup/porcupine/internal/lake"
)

func TestSharedHostedDeletionRequiresActualReturnAcrossWorkers(t *testing.T) {
	t.Parallel()
	s := sharedDeletionFixture(t)
	ctx := t.Context()
	db := s.control
	if err := beginHostedProjectActivity(ctx, db, "removed", "first-query"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO shared_query_permits
(permit_id,tenant_id,project_id,worker_id,expires_at)
VALUES('first-query','default','removed','owner-a',clock_timestamp()-interval '1 hour')`); err != nil {
		t.Fatal(err)
	}
	if err := s.beginControlProjectDeletion(ctx, "removed"); err != nil {
		t.Fatal(err)
	}
	if err := s.waitSharedProjectQueries(ctx, "removed"); err != nil {
		t.Fatal(err)
	}

	f := &hostedLiveFlusher{publisher: &hostedEffectPublisher{control: db}}
	key := lakeapi.ProjectWriteKey{SourceID: "source", ProjectID: "removed"}
	if err := f.sealDeletedProject(ctx, key); !errors.Is(err, errHostedProjectBusy) {
		t.Fatalf("expired permit let deletion pass actual-return barrier: %v", err)
	}
	if err := beginHostedProjectActivity(ctx, db, "removed", "late"); !errors.Is(err, errHostedProjectFenced) {
		t.Fatalf("deleted project admitted a late operation: %v", err)
	}
	if err := beginHostedProjectActivity(ctx, db, "kept", "other-query"); err != nil {
		t.Fatalf("fenced project blocked another project: %v", err)
	}
	for range 2 {
		if err := finishHostedProjectActivity(ctx, db, "removed", "first-query"); err != nil {
			t.Fatal(err)
		}
	}
	var kept string
	if err := db.QueryRowContext(ctx, `SELECT activity_id FROM hosted_project_activity`).Scan(&kept); err != nil || kept != "other-query" {
		t.Fatalf("actual return removed another project's ticket: %q, %v", kept, err)
	}
}

func TestSharedHostedDeletionWaiterRechecksCommittedFence(t *testing.T) {
	t.Parallel()
	s := sharedDeletionFixture(t)
	ctx := t.Context()
	db := s.control
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var id string
	if err := tx.QueryRowContext(ctx, `SELECT id FROM projects WHERE id='removed' FOR UPDATE`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO project_deletions(project_id,tenant_id,completed) VALUES('removed','default',0)`); err != nil {
		t.Fatal(err)
	}
	waiter := make(chan error, 1)
	// Start an admission on a separate connection while the deleting
	// transaction owns the project lock and its tombstone is uncommitted.
	started := make(chan struct{})
	go func() {
		admission, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
		if err != nil {
			close(started)
			waiter <- err
			return
		}
		defer admission.Rollback()
		close(started)
		waiter <- registerHostedProjectActivityTx(ctx, admission, "removed", "waiter")
	}()
	<-started
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := <-waiter; !errors.Is(err, errHostedProjectFenced) {
		t.Fatalf("admission did not see the committed deletion fence: %v", err)
	}
}
