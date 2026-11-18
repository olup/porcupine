package porcupine

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sync/atomic"
	"testing"
	"time"

	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/query"
)

var shortHostedIncarnation = hostedIncarnationTiming{lease: 600 * time.Millisecond, renew: 100 * time.Millisecond, grace: 400 * time.Millisecond}

// hostedIncarnationWorker opens a separate control pool, as another process would.
func hostedIncarnationWorker(t *testing.T, dsn string, failStop func()) (*sql.DB, *hostedIncarnation) {
	t.Helper()
	db, err := openSharedControl(t.Context(), SharedConfig{DatabaseURL: dsn})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	worker, err := startHostedIncarnation(t.Context(), db, shortHostedIncarnation, failStop)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { worker.stop(); <-worker.done })
	return db, worker
}

// crash stops renewal without returning tickets or closing the lease.
func (i *hostedIncarnation) crash(db *sql.DB) {
	i.stop()
	<-i.done
	db.Close()
}

func TestSharedHostedIncarnationReclaimsOnlyDeadWorkerTickets(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	db, dsn := sharedControlFixture(t)
	ctx := t.Context()
	queries, _ := query.Normalize(nil)
	if err := initializeSharedControl(ctx, db, adminToken, queries, false); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO projects(id,tenant_id,name,created_at,ingestion_mode) VALUES('removed','default','Removed','now','mixed'),('kept','default','Kept','now','mixed')`); err != nil {
		t.Fatal(err)
	}
	var stopped atomic.Int32
	deadDB, dead := hostedIncarnationWorker(t, dsn, func() { stopped.Add(1) })
	liveDB, live := hostedIncarnationWorker(t, dsn, func() { stopped.Add(1) })
	if err := dead.beginActivity(ctx, deadDB, "removed", "dead-query"); err != nil {
		t.Fatal(err)
	}
	if err := live.beginActivity(ctx, liveDB, "kept", "live-query"); err != nil {
		t.Fatal(err)
	}
	dead.crash(deadDB)
	s := &Service{control: db, shared: &sharedRuntime{workerID: "deletion-test"}}
	for _, project := range []string{"removed", "kept"} {
		if err := s.beginControlProjectDeletion(ctx, project); err != nil {
			t.Fatal(err)
		}
	}
	f := &hostedLiveFlusher{publisher: &hostedEffectPublisher{control: db}}
	removed := lakeapi.ProjectWriteKey{SourceID: "source", ProjectID: "removed"}
	if err := f.sealDeletedProject(ctx, removed); !errors.Is(err, errHostedProjectBusy) {
		t.Fatalf("seal passed a ticket whose lease is still current: %v", err)
	}

	// Several lease periods later, only the dead worker's ticket is reclaimed.
	time.Sleep(3 * (shortHostedIncarnation.lease + shortHostedIncarnation.grace))
	if err := f.sealDeletedProject(ctx, lakeapi.ProjectWriteKey{SourceID: "source", ProjectID: "kept"}); !errors.Is(err, errHostedProjectBusy) {
		t.Fatalf("seal passed a renewed worker's unreturned ticket: %v", err)
	}
	var tickets []string
	rows, err := db.QueryContext(ctx, `SELECT activity_id FROM hosted_project_activity ORDER BY activity_id`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var ticket string
		if err := rows.Scan(&ticket); err != nil {
			t.Fatal(err)
		}
		tickets = append(tickets, ticket)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil || len(tickets) != 1 || tickets[0] != "live-query" {
		t.Fatalf("tickets after reclamation = %v, %v", tickets, err)
	}
	if stopped.Load() != 0 {
		t.Fatal("a renewing worker stopped itself")
	}
	live.release("kept", "live-query")
	if err := finishHostedProjectActivity(ctx, liveDB, "kept", "live-query"); err != nil {
		t.Fatal(err)
	}
}

func TestSharedHostedIncarnationStopsBeforeItsTicketsAreReclaimable(t *testing.T) {
	t.Parallel()
	db, dsn := sharedControlFixture(t)
	ctx := t.Context()
	queries, _ := query.Normalize(nil)
	if err := initializeSharedControl(ctx, db, adminToken, queries, false); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO projects(id,tenant_id,name,created_at,ingestion_mode) VALUES('busy','default','Busy','now','mixed')`); err != nil {
		t.Fatal(err)
	}
	// The fail-stop observes PostgreSQL from another worker at that instant.
	fenced := make(chan error, 1)
	busyDB, busy := hostedIncarnationWorker(t, dsn, func() {
		var reclaimable bool
		err := db.QueryRow(`SELECT reclaim_after<=clock_timestamp() FROM hosted_worker_incarnations i
JOIN hosted_project_activity a USING(incarnation_id) WHERE a.activity_id='running'`).Scan(&reclaimable)
		if err == nil && reclaimable {
			err = errors.New("ticket became reclaimable before its worker stopped")
		}
		fenced <- err
	})
	readerDB, reader := hostedIncarnationWorker(t, dsn, func() { fenced <- errors.New("read-only query stopped its worker") })
	tx, err := readerDB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := reader.registerReadTx(ctx, tx, "busy", "read-only"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := busy.beginActivity(ctx, busyDB, "busy", "running"); err != nil {
		t.Fatal(err)
	}
	// Control becomes unreachable for both workers, while the call keeps running.
	busyDB.Close()
	readerDB.Close()
	select {
	case err := <-fenced:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("worker kept an outstanding call after its lease expired")
	}
	time.Sleep(shortHostedIncarnation.lease)
	select {
	case err := <-fenced:
		t.Fatal(err)
	default:
	}
	if _, err := reader.reserve("busy", "late", true); !errors.Is(err, errHostedIncarnationLost) {
		t.Fatalf("read-only worker without a current lease admitted a ticket: %v", err)
	}
	if _, err := busy.reserve("busy", "late", true); !errors.Is(err, errHostedIncarnationLost) {
		t.Fatalf("fenced worker admitted a ticket: %v", err)
	}
}

func TestSharedHostedDeletionCompletesAfterWorkerCrash(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	fixture := nativeS3Fixture(t)
	ctx := t.Context()
	dsn := sharedPostgresDSN(t)
	var control atomic.Pointer[sql.DB]
	lake := lakeapi.DuckLakeConfig{PostgresURL: dsn, S3: &fixture.storage, Threads: 1, MemoryLimitMB: 128, MaxOpenProjects: 2,
		MaintenanceInterval: time.Hour, CleanupGrace: 3 * time.Second,
		RestoreWindow: time.Second, RestoreMaxReaderTime: time.Second, RestoreSafetyMargin: time.Second}
	config := Config{Profile: ProfileHosted, AcceptanceMode: AcceptanceModeBuffered, BootstrapToken: adminToken,
		Shared: &SharedConfig{DatabaseURL: dsn}, DuckLake: &lake,
		HostedRecoveryFloor: func(ctx context.Context) (time.Time, error) {
			var floor time.Time
			err := control.Load().QueryRowContext(ctx, `SELECT max(inventory_at) FROM hosted_project_cleanup`).Scan(&floor)
			return floor, err
		}}
	b, host, _ := hostedCleanupWorker(t, config)
	control.Store(b.control)
	project, _ := sharedJourneyProject(t, host.URL, defaultTenantID, "worker crash")
	hostedCleanupWrite(t, b, project, "before crash")

	// An independent worker registers first. Terminate it without running
	// cleanup or returning the ticket, as happens on an OOM kill.
	stop := startHostedIncarnationChild(t, dsn, project)
	if err := b.beginTenantDeletion(ctx, defaultTenantID); err != nil {
		t.Fatal(err)
	}
	if err := b.hosted.flusher.sealProject(ctx, project); !errors.Is(err, errHostedProjectBusy) {
		t.Fatalf("seal ignored a ticket with a current lease: %v", err)
	}
	stop()
	deadline := time.Now().Add(30 * time.Second)
	for {
		err := b.hosted.flusher.sealProject(ctx, project)
		if err == nil {
			err = b.cleanHostedProject(ctx, project)
		}
		if err != nil && !errors.Is(err, errHostedProjectBusy) && !errors.Is(err, errHostedRecoveryFloor) {
			t.Fatal(err)
		}
		var complete int
		if err := b.control.QueryRowContext(ctx, `SELECT completed FROM project_deletions WHERE project_id=$1`, project).Scan(&complete); err != nil {
			t.Fatal(err)
		}
		if complete == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("deletion stayed blocked by a crashed worker's ticket", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err := b.finishTenantDeletion(ctx, defaultTenantID); err != nil {
		t.Fatal(err)
	}
	var state string
	if err := b.control.QueryRowContext(ctx, `SELECT state FROM tenant_deletions WHERE tenant_id=$1`, defaultTenantID).Scan(&state); err != nil || state != "logically_deleted" {
		t.Fatalf("tenant deletion state = %q, %v", state, err)
	}
}

func startHostedIncarnationChild(t *testing.T, dsn, project string) func() {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	t.Cleanup(cancel)
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, exe, "-test.run=^TestHostedIncarnationCrashChild$")
	cmd.Env = append(os.Environ(), "PORCUPINE_CRASH_CHILD_DSN="+dsn, "PORCUPINE_CRASH_CHILD_PROJECT="+project)
	cmd.Stderr = os.Stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		if scanner.Text() == "ticket committed" {
			return func() {
				if err := cmd.Process.Kill(); err != nil {
					t.Fatal(err)
				}
				var exit *exec.ExitError
				if err := cmd.Wait(); !errors.As(err, &exit) || exit.Exited() {
					t.Fatalf("worker did not die by signal: %v", err)
				}
			}
		}
		t.Log(scanner.Text())
	}
	t.Fatalf("worker did not commit its ticket: %v", scanner.Err())
	return nil
}

func TestHostedIncarnationCrashChild(t *testing.T) {
	t.Parallel()
	dsn, project := os.Getenv("PORCUPINE_CRASH_CHILD_DSN"), os.Getenv("PORCUPINE_CRASH_CHILD_PROJECT")
	if dsn == "" || project == "" {
		t.Skip("run by the crash recovery integration test")
	}
	db, worker := hostedIncarnationWorker(t, dsn, exitOnHostedLeaseLoss)
	if err := worker.beginActivity(t.Context(), db, project, "crash-child-in-flight"); err != nil {
		t.Fatal(err)
	}
	fmt.Println("ticket committed")
	<-time.After(30 * time.Second)
	t.Fatal("parent did not stop the worker")
}
