package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	lakeapi "github.com/olup/porcupine/internal/lake"
)

func TestSharedHostedDeletionRequiresActualReturnAcrossWorkers(t *testing.T) {
	t.Parallel()
	s := sharedDeletionFixture(t)
	ctx := t.Context()
	db := s.control
	if err := (*hostedIncarnation)(nil).beginActivity(ctx, db, "removed", "first-query"); err != nil {
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
	if err := (*hostedIncarnation)(nil).beginActivity(ctx, db, "removed", "late"); !errors.Is(err, errHostedProjectFenced) {
		t.Fatalf("deleted project admitted a late operation: %v", err)
	}
	if err := (*hostedIncarnation)(nil).beginActivity(ctx, db, "kept", "other-query"); err != nil {
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
		waiter <- (*hostedIncarnation)(nil).registerTx(ctx, admission, "removed", "waiter")
	}()
	<-started
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := <-waiter; !errors.Is(err, errHostedProjectFenced) {
		t.Fatalf("admission did not see the committed deletion fence: %v", err)
	}
}

func TestSharedHostedCommitAllowsQueryAdmissionAndFencesDeletion(t *testing.T) {
	t.Parallel()
	s := sharedDeletionFixture(t)
	s.eventRules = &eventRules{clock: time.Now}
	ctx := t.Context()
	coordinator, err := lakeapi.NewPostgresProjectCoordinator(s.control, lakeapi.DefaultProjectCoordinatorConfig())
	if err != nil {
		t.Fatal(err)
	}
	f := &hostedLiveFlusher{coordinator: coordinator, publisher: &hostedEffectPublisher{control: s.control}}
	key := lakeapi.ProjectWriteKey{SourceID: "source", ProjectID: "removed"}
	session, err := coordinator.Acquire(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Release()
	err = f.runActiveHostedProject(ctx, session, key.ProjectID, func(ctx context.Context) error {
		return f.commitLiveIfActive(ctx, key.ProjectID, defaultTenantID, func() error {
			// The native commit has been entered but has not returned. A separate
			// PostgreSQL transaction must admit a query in this same project.
			bounded, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			start := time.Now()
			_, permit, err := s.acquireSharedQueryPermit(bounded, key.ProjectID, defaultTenantID)
			if err != nil {
				t.Fatalf("query admission blocked by paused live commit: %v", err)
			}
			t.Logf("query admission while commit paused: %s", time.Since(start))
			permit.release()
			if err := s.beginTenantDeletion(bounded, defaultTenantID); err != nil {
				t.Fatalf("tenant deletion intent blocked by paused live commit: %v", err)
			}
			_, permit, err = s.acquireSharedQueryPermit(bounded, key.ProjectID, defaultTenantID)
			if permit != nil {
				permit.release()
			}
			if queryStatus(err) != 404 {
				t.Fatalf("query admission did not observe tenant deletion fence: %v", err)
			}
			if err := f.sealDeletedProject(bounded, key); !errors.Is(err, errHostedProjectBusy) {
				t.Fatalf("seal did not wait for the live writer's ticket: %v", err)
			}
			if other, err := coordinator.TryAcquire(bounded, key); !errors.Is(err, lakeapi.ErrProjectWriteBusy) {
				if other != nil {
					_ = other.Release()
				}
				t.Fatalf("second writer entered during live commit: %v", err)
			}
			return nil
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	err = f.commitLiveIfActive(ctx, key.ProjectID, defaultTenantID, func() error {
		t.Fatal("commit entered after tenant deletion intent")
		return nil
	})
	if !errors.Is(err, errHostedProjectFenced) {
		t.Fatalf("commit did not observe deletion fence: %v", err)
	}
}
