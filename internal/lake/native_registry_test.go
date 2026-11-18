package lake

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/event"
)

func TestNativeRegistryStandaloneDiscovery(t *testing.T) {
	t.Parallel()
	store, err := NewDuckLake(DuckLakeConfig{Directory: filepath.Join(t.TempDir(), "lake")})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	s := store.(*DuckLakeStore)
	if _, err := s.registry.ensure(t.Context(), "a"); err != nil {
		t.Fatal(err)
	}
	if err := s.registry.markReady(t.Context(), "a"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Registry.Exec(`INSERT INTO deleting_projects(project_id) VALUES('a'),('uncataloged')`); err != nil {
		t.Fatal(err)
	}
	page, err := s.registry.scan(t.Context(), "", 1)
	if err != nil || len(page) != 1 || page[0].ProjectID != "a" || !page[0].Ready || !page[0].Deleted {
		t.Fatal("standalone discovery lost catalog fence", page, err)
	}
	page, err = s.registry.scan(t.Context(), "a", 1)
	if err != nil || len(page) != 1 || page[0].ProjectID != "uncataloged" || page[0].Ready || !page[0].Deleted {
		t.Fatal("standalone discovery lost unfinished fence", page, err)
	}
	if _, err := s.registry.lookup(t.Context(), "uncataloged"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("unfinished fence became a catalog", err)
	}
}

func registryTestDB(t *testing.T) (*sql.DB, string) {
	t.Helper()
	dsn := sharedPostgresDSN(t)
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db, dsn
}

func registrySchemaShape(t *testing.T, db *sql.DB) string {
	t.Helper()
	var shape string
	if err := db.QueryRow(`SELECT coalesce(string_agg(table_name||':'||column_name||':'||data_type||':'||is_nullable||':'||coalesce(column_default,''),'|' ORDER BY table_name,ordinal_position),'') FROM information_schema.columns WHERE table_schema=current_schema() AND table_name LIKE 'pc\_native\_%' ESCAPE '\'`).Scan(&shape); err != nil {
		t.Fatal(err)
	}
	return shape
}

func TestSharedNativeRegistryNumberedMigrations(t *testing.T) {
	t.Parallel()
	fresh, _ := registryTestDB(t)
	if err := migrateNativeRegistry(t.Context(), fresh); err != nil {
		t.Fatal(err)
	}
	expected := registrySchemaShape(t, fresh)
	for start := 0; start <= len(nativeRegistrySteps); start++ {
		t.Run(fmt.Sprint(start), func(t *testing.T) {
			t.Parallel()
			db, _ := registryTestDB(t)
			for i := range start {
				if _, err := db.Exec(nativeRegistrySteps[i]); err != nil {
					t.Fatal(err)
				}
				if _, err := db.Exec(`INSERT INTO pc_native_registry_version VALUES(1,$1) ON CONFLICT(id) DO UPDATE SET version=excluded.version`, i+1); err != nil {
					t.Fatal(err)
				}
			}
			if err := migrateNativeRegistry(t.Context(), db); err != nil {
				t.Fatal(err)
			}
			if got := registrySchemaShape(t, db); got != expected {
				t.Fatal("numbered schema differs from initialization", got, expected)
			}
			if _, err := db.Exec(`UPDATE pc_native_registry_version SET version=$1`, len(nativeRegistrySteps)+1); err != nil {
				t.Fatal(err)
			}
			if err := migrateNativeRegistry(t.Context(), db); err == nil || !strings.Contains(err.Error(), "newer") {
				t.Fatal("future schema accepted", err)
			}
			if registrySchemaShape(t, db) != expected {
				t.Fatal("future version refusal modified schema")
			}
		})
	}
}

func TestSharedNativeRegistryLegacyAdoptionAndUnknownRefusal(t *testing.T) {
	t.Parallel()
	for _, unknown := range []bool{false, true} {
		t.Run(fmt.Sprint(unknown), func(t *testing.T) {
			t.Parallel()
			db, _ := registryTestDB(t)
			if _, err := db.Exec(nativeRegistrySchemaV1); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`INSERT INTO pc_native_sources VALUES('source','binding'); INSERT INTO pc_native_projects VALUES('source','project','catalog',1,1)`); err != nil {
				t.Fatal(err)
			}
			if unknown {
				if _, err := db.Exec(`ALTER TABLE pc_native_projects ADD COLUMN unknown text`); err != nil {
					t.Fatal(err)
				}
			}
			before := registrySchemaShape(t, db)
			err := migrateNativeRegistry(t.Context(), db)
			if unknown {
				if err == nil || registrySchemaShape(t, db) != before {
					t.Fatal("unknown format was adopted or modified", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			var ready, deleted int
			if err := db.QueryRow(`SELECT ready,deleted FROM pc_native_projects WHERE project_id='project'`).Scan(&ready, &deleted); err != nil || ready != 1 || deleted != 1 {
				t.Fatal("legacy fences lost", ready, deleted, err)
			}
		})
	}
}

func TestSharedNativeRegistryCleanupTakeoverAndReadOnly(t *testing.T) {
	t.Parallel()
	db, dsn := registryTestDB(t)
	if err := migrateNativeRegistry(t.Context(), db); err != nil {
		t.Fatal(err)
	}
	const binding = "opaque-binding"
	source := event.NewID()
	if _, err := db.Exec(`INSERT INTO pc_native_sources VALUES($1,$2)`, source, binding); err != nil {
		t.Fatal(err)
	}
	a := &postgresNativeRegistry{db: db, source: source, fingerprint: binding}
	b := &postgresNativeRegistry{db: db, source: source, fingerprint: binding}
	if _, err := a.lookup(t.Context(), "unknown"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("unknown lookup did not refuse", err)
	}
	if err := a.markReady(t.Context(), "unknown"); !errors.Is(err, errSharedNativeDeleted) {
		t.Fatal("unknown lookup created a project", err)
	}
	if _, err := a.ensure(t.Context(), "a"); err != nil {
		t.Fatal(err)
	}
	if err := a.markReady(t.Context(), "a"); err != nil {
		t.Fatal(err)
	}
	// B is registered and terminal. It never becomes a cleanup candidate.
	if _, err := b.ensure(t.Context(), "b"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE pc_native_projects SET ready=1,deleted=1 WHERE project_id='b'`); err != nil {
		t.Fatal(err)
	}
	page, err := b.scan(t.Context(), "", 1)
	if err != nil || len(page) != 1 || page[0].ProjectID != "a" || !page[0].Ready {
		t.Fatal("cold scan did not find first active project", page, err)
	}
	page, err = b.scan(t.Context(), page[0].ProjectID, 1)
	if err != nil || len(page) != 1 || page[0].ProjectID != "b" || !page[0].Deleted {
		t.Fatal("cold scan dropped terminal fence metadata", page, err)
	}
	at := time.Now()
	first, err := a.claimCleanup(t.Context(), at, at.Add(time.Minute))
	if err != nil || first.ProjectID != "a" {
		t.Fatal(first, err)
	}
	if _, err := b.claimCleanup(t.Context(), at, at.Add(time.Minute)); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("another worker stole a live attempt", err)
	}
	second, err := b.claimCleanup(t.Context(), at.Add(2*time.Minute), at.Add(3*time.Minute))
	if err != nil || second.ID == first.ID || second.ProjectID != "a" {
		t.Fatal("interrupted attempt could not recover", second, err)
	}
	if err := a.finishCleanup(t.Context(), first, "ok", at, at); err == nil {
		t.Fatal("stale worker overwrote recovery status")
	}
	if err := b.finishCleanup(t.Context(), second, "failed", time.Time{}, time.Time{}); err != nil {
		t.Fatal(err)
	}
	summary, err := a.cleanupSummary(t.Context())
	if err != nil || summary.Registered != 1 || summary.Failed != 1 || summary.Completed != 0 {
		t.Fatal("failure claimed completed cleanup", summary, err)
	}
	readonlyURL, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	options := readonlyURL.Query()
	options.Set("default_transaction_read_only", "on")
	readonlyURL.RawQuery = options.Encode()
	ro, err := sql.Open("pgx", readonlyURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	readonly := &postgresNativeRegistry{db: ro, source: source, fingerprint: binding}
	if _, err := readonly.sourceID(t.Context()); err != nil {
		t.Fatal("read-only authority could not be read", err)
	}
	if _, err := readonly.claimCleanup(t.Context(), at.Add(4*time.Minute), at.Add(5*time.Minute)); err == nil {
		t.Fatal("read-only cleanup claimed completion")
	}
	if after, err := a.cleanupSummary(t.Context()); err != nil || after != summary {
		t.Fatal("read-only attempt changed durable cleanup", after, err)
	}
	if deleted, err := b.deleting(context.Background(), "b"); err != nil || !deleted {
		t.Fatal("terminal project lost its fence", deleted, err)
	}
	if err := b.markReady(t.Context(), "b"); !errors.Is(err, errSharedNativeDeleted) {
		t.Fatal("terminal project reopened", err)
	}
}

func TestSharedNativeRegistryConcurrentCleanupClaims(t *testing.T) {
	t.Parallel()
	db, _ := registryTestDB(t)
	if err := migrateNativeRegistry(t.Context(), db); err != nil {
		t.Fatal(err)
	}
	source := event.NewID()
	if _, err := db.Exec(`INSERT INTO pc_native_sources VALUES($1,'binding')`, source); err != nil {
		t.Fatal(err)
	}
	r := &postgresNativeRegistry{db: db, source: source, fingerprint: "binding"}
	if _, err := r.ensure(t.Context(), "project"); err != nil {
		t.Fatal(err)
	}
	if err := r.markReady(t.Context(), "project"); err != nil {
		t.Fatal(err)
	}
	type outcome struct {
		attempt nativeCleanupAttempt
		err     error
	}
	for round := range 4 {
		at := time.Now().Add(time.Duration(round) * 2 * time.Minute)
		start := make(chan struct{})
		results := make(chan outcome, 16)
		for range 16 {
			go func() {
				<-start
				attempt, err := r.claimCleanup(t.Context(), at, at.Add(time.Minute))
				results <- outcome{attempt, err}
			}()
		}
		close(start)
		var claimed []nativeCleanupAttempt
		for range 16 {
			result := <-results
			if result.err == nil {
				claimed = append(claimed, result.attempt)
			} else if !errors.Is(result.err, sql.ErrNoRows) {
				t.Fatal(result.err)
			}
		}
		if len(claimed) != 1 {
			t.Fatal("concurrent workers stole an unexpired cleanup claim", len(claimed))
		}
		if err := r.finishCleanup(t.Context(), claimed[0], "ok", at, at.Add(time.Minute)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSharedNativeRegistryBoundsBlockedMetadata(t *testing.T) {
	t.Parallel()
	db, _ := registryTestDB(t)
	if err := migrateNativeRegistry(t.Context(), db); err != nil {
		t.Fatal(err)
	}
	source := event.NewID()
	if _, err := db.Exec(`INSERT INTO pc_native_sources VALUES($1,'binding')`, source); err != nil {
		t.Fatal(err)
	}
	r := &postgresNativeRegistry{db: db, source: source, fingerprint: "binding", timeout: time.Second}
	if _, err := r.ensure(t.Context(), "blocked"); err != nil {
		t.Fatal(err)
	}
	lock, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback()
	if _, err := lock.Exec(`SELECT 1 FROM pc_native_projects WHERE project_id='blocked' FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	var blocker int
	if err := lock.QueryRow(`SELECT pg_backend_pid()`).Scan(&blocker); err != nil {
		t.Fatal(err)
	}
	r.timeout = 10 * time.Second
	wait, stopWaiting := context.WithTimeout(t.Context(), 5*time.Second)
	defer stopWaiting()
	result := make(chan error, 1)
	go func() {
		result <- r.markReady(wait, "blocked")
	}()
	poll := time.NewTicker(10 * time.Millisecond)
	defer poll.Stop()
	for {
		var blocked bool
		if err := db.QueryRowContext(wait, `SELECT EXISTS (
SELECT 1 FROM pg_locks WHERE NOT granted AND $1 = ANY(pg_blocking_pids(pid))
)`, blocker).Scan(&blocked); err != nil {
			t.Fatal("observing the metadata lock wait", err)
		}
		if blocked {
			break
		}
		select {
		case err := <-result:
			t.Fatal("metadata update returned before waiting for the lock", err)
		case <-wait.Done():
			t.Fatal("metadata update did not wait for the lock", wait.Err())
		case <-poll.C:
		}
	}
	stopWaiting()
	if err := <-result; err == nil {
		t.Fatal("cancelled metadata update ignored its context")
	}

	r.timeout = 50 * time.Millisecond
	work, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	started := time.Now()
	if err := r.markReady(work, "blocked"); err == nil {
		t.Fatal("blocked metadata update ignored its bound")
	}
	if time.Since(started) > time.Second {
		t.Fatal("blocked metadata exceeded configured timeout")
	}
	if err := lock.Rollback(); err != nil {
		t.Fatal(err)
	}
	r.timeout = time.Second
	if record, err := r.lookup(t.Context(), "blocked"); err != nil || record.Ready {
		t.Fatal("expired metadata update published readiness", record, err)
	}
	if err := r.markReady(t.Context(), "blocked"); err != nil {
		t.Fatal("retrying readiness after the lock was released", err)
	}
	if record, err := r.lookup(t.Context(), "blocked"); err != nil || !record.Ready {
		t.Fatal("successful retry did not publish readiness", record, err)
	}
}
