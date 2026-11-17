package porcupine

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/olup/porcupine/internal/auth"
	"net/netip"
	"path/filepath"
	"sync"
	"testing"
	"time"

	eventapi "github.com/olup/porcupine/internal/event"
	ingestfmt "github.com/olup/porcupine/internal/ingest"
)

func sharedIngestionTestService(t *testing.T, dsn, worker string) *Service {
	t.Helper()
	s, err := Open(Config{DBPath: filepath.Join(t.TempDir(), "worker.db"), BootstrapToken: adminToken, QueueMaxEvents: 2000})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	// This component fixture attaches shared dependencies after Open. Stop its
	// local recovery reader before changing fields that production initializes
	// before starting any background work.
	s.deletionCancel()
	<-s.deletionDone
	s.control, err = openSharedControl(t.Context(), SharedConfig{DatabaseURL: dsn})
	if err != nil {
		t.Fatal(err)
	}
	s.shared = &sharedRuntime{config: SharedConfig{DatabaseURL: dsn, WorkerID: worker}}
	if err = initializeSharedControl(t.Context(), s.control, adminToken, s.interactiveConfig, false); err != nil {
		t.Fatal(err)
	}
	if err = s.initializeSharedIngestion(t.Context()); err != nil {
		t.Fatal(err)
	}
	return s
}

func sharedIngestionProject(t *testing.T, s *Service, id string) {
	t.Helper()
	_, err := s.db.Exec(`INSERT INTO projects(id,tenant_id,name,created_at,ingestion_mode) VALUES($1,$2,$1,$3,'mixed')`, id, defaultTenantID, now())
	if err != nil {
		t.Fatal(err)
	}
}

func sharedIngestionBatch(project, prefix string, n int) preparedLiveBatch {
	batch := preparedLiveBatch{projectID: project}
	for i := range n {
		batch.events = append(batch.events, eventapi.Event{ProjectID: project, UUID: fmt.Sprintf("%s-%d", prefix, i), Event: "visited", DistinctID: "visitor", Properties: []byte(`{}`), Timestamp: "2026-11-01T00:00:00Z", ReceivedAt: now()})
	}
	return batch
}

func sharedUsageCount(t *testing.T, s *Service) int64 {
	t.Helper()
	period, _ := tenantMonth(s.clock())
	var count int64
	if err := s.control.QueryRow(`SELECT coalesce((SELECT accepted_events FROM tenant_usage WHERE tenant_id=$1 AND period_start=$2),0)`, defaultTenantID, period.Format(time.RFC3339)).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func TestSharedIngestionLocalDurabilityAndIdempotentHighWater(t *testing.T) {
	t.Parallel()
	dsn := sharedPostgresDSN(t)
	a, b := sharedIngestionTestService(t, dsn, "writer-a"), sharedIngestionTestService(t, dsn, "writer-b")
	sharedIngestionProject(t, a, "a")
	sharedIngestionProject(t, b, "b")
	if _, err := a.control.Exec(`UPDATE tenants SET ingestion_monthly_events=10 WHERE id=$1`, defaultTenantID); err != nil {
		t.Fatal(err)
	}
	for _, s := range []*Service{a, b} {
		if err := s.syncSharedIngestion(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	first, err := a.acceptance.Accept(t.Context(), sharedIngestionBatch("a", "a", 6), ingestionPeer{}, true)
	if err != nil || first.accepted != 6 {
		t.Fatal(first, err)
	}
	second, err := b.acceptance.Accept(t.Context(), sharedIngestionBatch("b", "b", 6), ingestionPeer{}, true)
	if err != nil || second.accepted != 6 {
		t.Fatal(second, err)
	}
	if first.receipts[0].EventID == second.receipts[0].EventID {
		t.Fatal("independent workers reused a global ID")
	}
	if got := sharedUsageCount(t, a); got != 0 {
		t.Fatal("acceptance unexpectedly synchronously published each event", got)
	}
	for _, s := range []*Service{a, b, a, b} {
		if err := s.syncSharedIngestion(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if got := sharedUsageCount(t, a); got != 12 {
		t.Fatal("retrying publication double-counted accepted rows", got)
	}
	_, err = a.acceptance.Accept(t.Context(), sharedIngestionBatch("a", "overflow", 1), ingestionPeer{}, true)
	var quota *tenantIngestionError
	if !errors.As(err, &quota) || quota.code != "ingestion_budget_exceeded" {
		t.Fatal("refreshed global usage did not stop new events", err)
	}
	duplicate, err := a.acceptance.Accept(t.Context(), sharedIngestionBatch("a", "a", 6), ingestionPeer{}, true)
	if err != nil || duplicate.accepted != 0 || duplicate.duplicates != 6 || duplicate.receipts[0].EventID != first.receipts[0].EventID {
		t.Fatal("owner retry changed stable IDs or consumed quota", duplicate, err)
	}
	var count int
	if err = a.db.QueryRow(`SELECT count(*) FROM events`).Scan(&count); err != nil || count != 6 {
		t.Fatal("canonical events were not locally durable", count, err)
	}
	for _, table := range []string{"events", "identity_profiles", "event_receipts", "mutation_outbox"} {
		var absent bool
		if err = a.control.QueryRow(`SELECT to_regclass($1) IS NULL`, table).Scan(&absent); err != nil || !absent {
			t.Fatal("event state leaked into PostgreSQL", table, err)
		}
	}
}

func TestSharedIngestionPublicationRecoveryAndRestartedAllocator(t *testing.T) {
	t.Parallel()
	dsn := sharedPostgresDSN(t)
	s := sharedIngestionTestService(t, dsn, "persistent-writer")
	sharedIngestionProject(t, s, "recover")
	first, err := s.acceptance.Accept(t.Context(), sharedIngestionBatch("recover", "before", 3), ingestionPeer{}, true)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.control.Close(); err != nil {
		t.Fatal(err)
	}
	// A ready ID range and fresh policy allow durable local acceptance without
	// another PostgreSQL call on each event. Publication can recover later.
	if _, err = s.acceptance.Accept(t.Context(), sharedIngestionBatch("recover", "offline", 2), ingestionPeer{}, true); err != nil {
		t.Fatal(err)
	}
	if err = s.syncSharedIngestion(t.Context()); err == nil {
		t.Fatal("publication claimed success while the authority was unavailable")
	}
	s.control, err = openSharedControl(t.Context(), SharedConfig{DatabaseURL: dsn})
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err = s.syncSharedIngestion(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if got := sharedUsageCount(t, s); got != 5 {
		t.Fatal("replayed high-water publication lost or duplicated usage", got)
	}
	// Restart the allocator without reusing abandoned values from its old range.
	s.sharedIngestion.nextID, s.sharedIngestion.endID = 0, 0
	last, err := s.acceptance.Accept(t.Context(), sharedIngestionBatch("recover", "after", 1), ingestionPeer{}, true)
	if err != nil || last.receipts[0].EventID == first.receipts[0].EventID {
		t.Fatal(last, err)
	}
	if err = s.syncSharedIngestion(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := sharedUsageCount(t, s); got != 6 {
		t.Fatal(got)
	}
	if _, err = s.db.Exec(`UPDATE shared_tenant_cache SET refreshed_at_ms=0`); err != nil {
		t.Fatal(err)
	}
	if err = s.control.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = s.acceptance.Accept(t.Context(), sharedIngestionBatch("recover", "stale", 1), ingestionPeer{}, true); err == nil {
		t.Fatal("stale policy failed open")
	}
}

func TestSharedIngestionCookielessKeysAndUTCPeriods(t *testing.T) {
	t.Parallel()
	dsn := sharedPostgresDSN(t)
	a, b := sharedIngestionTestService(t, dsn, "cookie-a"), sharedIngestionTestService(t, dsn, "cookie-b")
	for _, s := range []*Service{a, b} {
		sharedIngestionProject(t, s, "cookie")
	}
	at := time.Date(2026, 11, 30, 23, 59, 59, 0, time.UTC)
	a.clock = func() time.Time { return at }
	b.clock = a.clock
	peer := ingestionPeer{address: netip.MustParseAddr("192.0.2.8"), userAgent: "fixture"}
	accept := func(s *Service, prefix string) eventapi.Event {
		t.Helper()
		batch := sharedIngestionBatch("cookie", prefix, 1)
		batch.cookieless = true
		batch.events[0].DistinctID = ingestfmt.CookielessSentinel
		if _, err := s.acceptance.Accept(t.Context(), batch, peer, true); err != nil {
			t.Fatal(err)
		}
		var event eventapi.Event
		var properties string
		if err := s.db.QueryRow(`SELECT distinct_id,properties FROM events WHERE uuid=$1`, prefix+"-0").Scan(&event.DistinctID, &properties); err != nil {
			t.Fatal(err)
		}
		event.Properties = []byte(properties)
		return event
	}
	beforeA, beforeB := accept(a, "before-a"), accept(b, "before-b")
	if beforeA.DistinctID != beforeB.DistinctID {
		t.Fatal("worker routing changed daily identity")
	}
	at = at.Add(time.Second)
	afterA, afterB := accept(a, "after-a"), accept(b, "after-b")
	if afterA.DistinctID != afterB.DistinctID || afterA.DistinctID == beforeA.DistinctID {
		t.Fatal("shared daily key did not rotate once")
	}
	for _, s := range []*Service{a, b} {
		if err := s.syncSharedIngestion(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	for _, period := range []string{"2026-11-01T00:00:00Z", "2026-12-01T00:00:00Z"} {
		var n int64
		if err := a.control.QueryRow(`SELECT accepted_events FROM tenant_usage WHERE tenant_id=$1 AND period_start=$2`, defaultTenantID, period).Scan(&n); err != nil || n != 2 {
			t.Fatal("period publication reset or lost historical usage", period, n, err)
		}
	}
	var secret []byte
	if err := a.control.QueryRow(`SELECT secret FROM shared_cookieless_keys WHERE day='2026-12-01'`).Scan(&secret); err != nil {
		t.Fatal(err)
	}
	for _, s := range []*Service{a, b} {
		var local []byte
		if err := s.db.QueryRow(`SELECT secret FROM cookieless_keys WHERE day='2026-12-01'`).Scan(&local); err != nil || !bytes.Equal(local, secret) {
			t.Fatal("local daily-key cache diverged", err)
		}
	}
	at = at.Add(-time.Second)
	batch := sharedIngestionBatch("cookie", "rollback", 1)
	batch.cookieless = true
	batch.events[0].DistinctID = ingestfmt.CookielessSentinel
	if _, err := a.acceptance.Accept(t.Context(), batch, peer, true); err == nil {
		t.Fatal("expired identity day was resurrected")
	}
}

func TestSharedIngestionExplicitIDHookKeepsAtomicIdentity(t *testing.T) {
	t.Parallel()
	s := service(t, 20)
	project, _ := project(t, s)
	var next int64 = 100
	s.allocateID = func(context.Context, *sql.Tx) (int64, error) { next++; return next, nil }
	batch := sharedIngestionBatch(project, "identity", 1)
	batch.events[0].Event = "$identify"
	batch.events[0].DistinctID = "known"
	batch.events[0].Properties = []byte(`{"$anon_distinct_id":"anonymous","$set":{"plan":"team"}}`)
	result, err := s.acceptance.Accept(t.Context(), batch, ingestionPeer{}, true)
	if err != nil || result.receipts[0].EventID != "101" {
		t.Fatal(result, err)
	}
	var version int64
	if err = s.db.QueryRow(`SELECT version FROM identity_profiles WHERE project_id=$1 AND identified_id='known'`, project).Scan(&version); err != nil || version != 101 {
		t.Fatal("identity version diverged from accepted event ID", version, err)
	}
}

func TestSharedIngestionConcurrentIDRangesAndUsagePublication(t *testing.T) {
	t.Parallel()
	dsn := sharedPostgresDSN(t)
	a, b := sharedIngestionTestService(t, dsn, "range-a"), sharedIngestionTestService(t, dsn, "range-b")
	sharedIngestionProject(t, a, "range-a")
	sharedIngestionProject(t, b, "range-b")
	var mu sync.Mutex
	ids := map[int64]bool{}
	var work sync.WaitGroup
	for _, s := range []*Service{a, b} {
		work.Add(1)
		go func() {
			defer work.Done()
			var previous int64
			for range int(sharedEventIDBlockSize + 3) {
				id, err := s.nextSharedEventID(t.Context(), nil)
				if err != nil || id <= previous {
					t.Error("worker IDs did not remain monotonic", id, previous, err)
					return
				}
				previous = id
				mu.Lock()
				if ids[id] {
					t.Error("workers reused a reserved ID", id)
				}
				ids[id] = true
				mu.Unlock()
			}
		}()
	}
	work.Wait()
	if len(ids) != 2*int(sharedEventIDBlockSize+3) {
		t.Fatal("range allocation lost identifiers", len(ids))
	}
	for _, pair := range []struct {
		s       *Service
		project string
	}{{a, "range-a"}, {b, "range-b"}} {
		if _, err := pair.s.acceptance.Accept(t.Context(), sharedIngestionBatch(pair.project, "usage", 3), ingestionPeer{}, true); err != nil {
			t.Fatal(err)
		}
	}
	for _, s := range []*Service{a, b, a, b} {
		work.Add(1)
		go func() {
			defer work.Done()
			if err := s.syncSharedIngestion(t.Context()); err != nil {
				t.Error(err)
			}
		}()
	}
	work.Wait()
	if got := sharedUsageCount(t, a); got != 6 {
		t.Fatal("concurrent publishers charged a highwater twice", got)
	}
}

func TestSharedImportAuthorityRejectsStaleLocalGrant(t *testing.T) {
	t.Parallel()
	dsn := sharedPostgresDSN(t)
	s := sharedIngestionTestService(t, dsn, "import-authority")
	sharedIngestionProject(t, s, "import")
	for _, db := range []*sql.DB{s.db, s.control} {
		_, err := db.Exec(`INSERT INTO access_keys(id,name,token_hash,permissions,projects,created_at) VALUES('import-actor','importer',$1,'["imports:write","events:write"]','["import"]',$2)`, hash("test-import-key"), now())
		if err != nil {
			t.Fatal(err)
		}
	}
	s.imports.control = s.control
	tx, err := s.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err = importCheckAuthority(t.Context(), s.imports.authorityQuery(tx), auth.Principal{ID: "import-actor"}, "import", s.clock()); err != nil {
		t.Fatal(err)
	}
	if _, err = s.control.Exec(`UPDATE access_keys SET revoked_at=$1 WHERE id='import-actor'`, now()); err != nil {
		t.Fatal(err)
	}
	// The local row remains an FK anchor, never the authority for a shared key.
	if err = importCheckAuthority(t.Context(), tx, auth.Principal{ID: "import-actor"}, "import", s.clock()); err != nil {
		t.Fatal("fixture did not keep a stale local grant", err)
	}
	if err = importCheckAuthority(t.Context(), s.imports.authorityQuery(tx), auth.Principal{ID: "import-actor"}, "import", s.clock()); err == nil {
		t.Fatal("shared revocation was hidden by the local grant anchor")
	}
}
