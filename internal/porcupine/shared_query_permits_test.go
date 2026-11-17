package porcupine

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"
)

func sharedQueryTestService(t *testing.T, dsn, worker string) *Service {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	local, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { local.Close() })
	config, _ := normalizeInteractiveQueries(nil)
	return &Service{db: local, control: db, shared: &sharedRuntime{config: SharedConfig{WorkerID: worker}}, interactiveConfig: config, querySlots: make(chan struct{}, config.MaxConcurrent)}
}

func sharedQueryTestFixture(t *testing.T) (string, *Service, *Service) {
	t.Helper()
	dsn := sharedPostgresDSN(t)
	first := sharedQueryTestService(t, dsn, "first")
	second := sharedQueryTestService(t, dsn, "second")
	_, err := first.control.Exec(`CREATE TABLE tenants(
 id TEXT PRIMARY KEY,name TEXT NOT NULL,created_at TEXT NOT NULL,ingestion_allowed INTEGER NOT NULL,
 ingestion_monthly_events BIGINT, hot_query_max_concurrent INTEGER NOT NULL,hot_query_timeout_ms INTEGER NOT NULL,
 hot_query_max_rows INTEGER NOT NULL,hot_query_max_result_bytes INTEGER NOT NULL);
 CREATE TABLE projects(id TEXT PRIMARY KEY,tenant_id TEXT NOT NULL REFERENCES tenants(id));
 CREATE TABLE project_deletions(project_id TEXT PRIMARY KEY,completed INTEGER NOT NULL);
 INSERT INTO tenants VALUES('one','One','2026-11-01',1,NULL,1,5000,1000,1048576),('two','Two','2026-11-01',1,NULL,1,5000,1000,1048576);
 INSERT INTO projects VALUES('a','one'),('b','one'),('c','two')`)
	if err != nil {
		t.Fatal(err)
	}
	initialized := make(chan error, 2)
	for _, s := range []*Service{first, second} {
		go func(s *Service) { initialized <- s.initializeSharedQueryPermits(context.Background()) }(s)
	}
	for range 2 {
		if err = <-initialized; err != nil {
			t.Fatal(err)
		}
	}
	return dsn, first, second
}

func TestSharedQueryPermitsAcrossServices(t *testing.T) {
	_, a, b := sharedQueryTestFixture(t)
	ctx := context.Background()
	_, _, release, err := a.beginInteractiveQuery(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err = b.beginInteractiveQuery(ctx, "b"); queryStatus(err) != 429 {
		t.Fatal("second process bypassed tenant cap", err)
	}
	_, _, otherRelease, err := b.beginInteractiveQuery(ctx, "c")
	if err != nil {
		t.Fatal("unrelated tenant blocked", err)
	}
	otherRelease()
	release()
	release()
	_, _, release, err = b.beginInteractiveQuery(ctx, "b")
	if err != nil {
		t.Fatal("cross-process release did not recover capacity", err)
	}
	release()
	if len(a.querySlots) != 0 || len(b.querySlots) != 0 {
		t.Fatal("local capacity leaked")
	}
	// A burst through independent pools must still produce exactly one permit.
	const contenders = 12
	start := make(chan struct{})
	releases := make(chan func(), contenders)
	var wait sync.WaitGroup
	for i := range contenders {
		wait.Add(1)
		go func(i int) {
			defer wait.Done()
			<-start
			s := a
			if i%2 == 1 {
				s = b
			}
			_, _, r, e := s.beginInteractiveQuery(ctx, "a")
			if e == nil {
				releases <- r
			} else if queryStatus(e) != 429 {
				t.Errorf("unexpected admission failure: %v", e)
			}
		}(i)
	}
	close(start)
	wait.Wait()
	close(releases)
	if len(releases) != 1 {
		t.Errorf("shared concurrency multiplied: %d", len(releases))
	}
	for r := range releases {
		r()
	}
	var count int
	if err = a.control.QueryRow(`SELECT count(*) FROM shared_query_permits`).Scan(&count); err != nil || count != 0 {
		t.Fatal("shared permits leaked", count, err)
	}
	// Current policy is read inside the locked admission transaction, rather than
	// trusting a caller's previously fetched tenant or project policy.
	if _, err = a.control.Exec(`UPDATE tenants SET hot_query_timeout_ms=250,hot_query_max_rows=7,hot_query_max_result_bytes=2048 WHERE id='one'`); err != nil {
		t.Fatal(err)
	}
	_, limits, r, err := b.beginInteractiveQuery(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	if limits.TimeoutMS != 250 || limits.MaxRows != 7 || limits.MaxResultBytes != 2048 {
		t.Fatal("shared admission used stale policy", limits)
	}
	r()
	if _, err = a.control.Exec(`INSERT INTO project_deletions(project_id,completed) VALUES('a',0)`); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err = b.beginInteractiveQuery(ctx, "a"); queryStatus(err) != 404 {
		t.Fatal("deleting project admitted", err)
	}
}

func TestSharedQueryPermitLifetimeReleaseAndFailure(t *testing.T) {
	_, a, b := sharedQueryTestFixture(t)
	a.lakeS3RequestTimeoutSeconds = 7
	a.lakeMetadataTimeoutSeconds = 10
	limits, first, err := a.acquireSharedQueryPermit(context.Background(), "a", "one")
	if err != nil {
		t.Fatal(err)
	}
	var remainingMS float64
	if err = a.control.QueryRow(`SELECT extract(epoch FROM expires_at-clock_timestamp())*1000 FROM shared_query_permits`).Scan(&remainingMS); err != nil {
		t.Fatal(err)
	}
	minimum := float64(sharedQueryPermitLifetime(limits, 7, 10).Milliseconds() - 1000)
	if remainingMS < minimum {
		t.Fatal("lease could expire while native query still runs", remainingMS, minimum)
	}
	// Simulated expiry permits a replacement; a late release must not erase it.
	if _, err = a.control.Exec(`UPDATE shared_query_permits SET expires_at=clock_timestamp()-interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	_, second, err := b.acquireSharedQueryPermit(context.Background(), "b", "one")
	if err != nil {
		t.Fatal(err)
	}
	first()
	first()
	var count int
	if err = a.control.QueryRow(`SELECT count(*) FROM shared_query_permits`).Scan(&count); err != nil || count != 1 {
		t.Fatal("stale release removed replacement", count, err)
	}
	second()
	second()
	if err = a.control.QueryRow(`SELECT count(*) FROM shared_query_permits`).Scan(&count); err != nil || count != 0 {
		t.Fatal(count, err)
	}
	b.control.Close()
	if _, _, _, err = b.beginInteractiveQuery(context.Background(), "a"); queryStatus(err) != 503 {
		t.Fatal("unavailable shared authority did not fail closed", err)
	}
	if len(b.querySlots) != 0 {
		t.Fatal("failed admission leaked local capacity")
	}
}

// The helper intentionally leaves a permit behind when its process is killed.
func TestSharedQueryPermitWorker(t *testing.T) {
	dsn := os.Getenv("PORCUPINE_SHARED_QUERY_HELPER_DSN")
	if dsn == "" {
		t.Skip("subprocess helper")
	}
	s := sharedQueryTestService(t, dsn, "crashed-worker")
	if _, _, err := s.acquireSharedQueryPermit(context.Background(), "a", "one"); err != nil {
		t.Fatal(err)
	}
	fmt.Println("shared-query-permit-ready")
	_, _ = io.ReadAll(os.Stdin)
}

func TestSharedQueryPermitCrashRecovery(t *testing.T) {
	dsn, a, b := sharedQueryTestFixture(t)
	// The 30-second fixed native safety margin is kept in the real crash proof.
	// There is no test-only short lease or production renewal shortcut.
	if _, err := a.control.Exec(`UPDATE tenants SET hot_query_timeout_ms=1 WHERE id='one'`); err != nil {
		t.Fatal(err)
	}
	child := exec.Command(os.Args[0], "-test.run=^TestSharedQueryPermitWorker$", "-test.v=false")
	child.Env = append(os.Environ(), "PORCUPINE_SHARED_QUERY_HELPER_DSN="+dsn)
	stdin, err := child.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer stdin.Close()
	output, err := child.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	child.Stderr = os.Stderr
	if err = child.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if child.Process != nil {
			_ = child.Process.Kill()
			_ = child.Wait()
		}
	}()
	ready := make(chan bool, 1)
	go func() {
		scanner := bufio.NewScanner(output)
		for scanner.Scan() {
			if scanner.Text() == "shared-query-permit-ready" {
				ready <- true
				return
			}
		}
		ready <- false
	}()
	select {
	case ok := <-ready:
		if !ok {
			t.Fatal("query worker failed before admission")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("worker did not acquire query permit")
	}
	if err = child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = child.Wait()
	if _, _, err = b.acquireSharedQueryPermit(context.Background(), "b", "one"); queryStatus(err) != 429 {
		t.Fatal("crash released permit before safety horizon", err)
	}
	deadline := time.Now().Add(35 * time.Second)
	for {
		_, release, e := b.acquireSharedQueryPermit(context.Background(), "b", "one")
		if e == nil {
			release()
			break
		}
		if queryStatus(e) != 429 || time.Now().After(deadline) {
			t.Fatal("crashed process permit did not expire", e)
		}
		time.Sleep(100 * time.Millisecond)
	}
	var count int
	if err = a.control.QueryRow(`SELECT count(*) FROM shared_query_permits`).Scan(&count); err != nil || count != 0 {
		t.Fatal("expired crash permit not cleaned", count, err)
	}
}

func TestSharedQueryProjectDeletionWaitsForReaders(t *testing.T) {
	_, a, b := sharedQueryTestFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, _, release, err := a.beginInteractiveQuery(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if _, err = a.control.Exec(`INSERT INTO project_deletions(project_id,completed) VALUES('a',0)`); err != nil {
		t.Fatal(err)
	}
	finished := make(chan error, 1)
	go func() { finished <- b.waitSharedProjectQueries(ctx, "a") }()
	select {
	case err := <-finished:
		t.Fatal("remote reader was not protected", err)
	case <-time.After(50 * time.Millisecond):
	}
	// The wait is project-scoped and must not block deleting another project.
	if err = b.waitSharedProjectQueries(ctx, "b"); err != nil {
		t.Fatal(err)
	}
	release()
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("released reader still blocked deletion")
	}
	if _, _, err = a.acquireSharedQueryPermit(ctx, "a", "one"); queryStatus(err) != 404 {
		t.Fatal("fenced project reacquired reader", err)
	}
	// A crashed reader is safe to ignore only after its database-clock expiry.
	if _, err = a.control.Exec(`DELETE FROM project_deletions WHERE project_id='a'`); err != nil {
		t.Fatal(err)
	}
	_, expiredRelease, err := a.acquireSharedQueryPermit(ctx, "a", "one")
	if err != nil {
		t.Fatal(err)
	}
	defer expiredRelease()
	short, stop := context.WithTimeout(ctx, 10*time.Millisecond)
	if err = b.waitSharedProjectQueries(short, "a"); err != context.DeadlineExceeded {
		t.Fatal("reader wait ignored cancellation", err)
	}
	stop()
	if _, err = a.control.Exec(`UPDATE shared_query_permits SET expires_at=clock_timestamp()-interval '1 second' WHERE project_id='a'`); err != nil {
		t.Fatal(err)
	}
	if err = b.waitSharedProjectQueries(ctx, "a"); err != nil {
		t.Fatal(err)
	}
}
