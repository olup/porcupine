package porcupine

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type receiptHistoryFixture struct {
	mu                           sync.Mutex
	c                            *clickHouse
	facts                        map[receiptKey]bool
	probes                       int
	unavailable                  bool
	response                     string
	definition                   string
	restrictive, policies, allow int
}

func receiptHistory(t *testing.T) *receiptHistoryFixture {
	f := &receiptHistoryFixture{facts: map[receiptKey]bool{}, definition: "CREATE TABLE events_v2 ENGINE = ReplacingMergeTree(revision,is_deleted) SETTINGS allow_experimental_replacing_merge_with_cleanup = 0"}
	f.c = testClickHouse(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		sql := string(body)
		f.mu.Lock()
		defer f.mu.Unlock()
		if strings.HasPrefix(sql, "/* porcupine:receipts */") {
			f.probes++
			if f.unavailable {
				http.Error(w, "unavailable", 503)
				return
			}
			if f.response != "" {
				io.WriteString(w, f.response)
				return
			}
			if !strings.Contains(sql, "final=0") || !strings.Contains(sql, "FORMAT JSON") || strings.Contains(sql, "JSONEachRow") {
				t.Error("history probe lacks complete physical result contract")
			}
			facts := [][]string{}
			for k := range f.facts {
				projectLiteral := "project_id=unhex('" + hex.EncodeToString([]byte(k.ProjectID)) + "')"
				uuidLiteral := "unhex('" + hex.EncodeToString([]byte(k.UUID)) + "')"
				if strings.Contains(sql, projectLiteral) && strings.Contains(sql, uuidLiteral) {
					facts = append(facts, []string{k.ProjectID, k.UUID})
				}
			}
			json.NewEncoder(w).Encode(map[string]any{"meta": []any{}, "data": []any{map[string]any{"source_uuid": "receipt-source", "source_engine": "ReplacingMergeTree", "sorting_key": "project_id, uuid", "definition": f.definition, "policy_count": fmt.Sprint(f.policies), "restrictive_count": fmt.Sprint(f.restrictive), "allow_count": fmt.Sprint(f.allow), "facts": facts}}, "rows": 1, "statistics": map[string]any{}})
			return
		}
		if strings.HasPrefix(r.URL.Query().Get("query"), "INSERT INTO ") {
			decoder := json.NewDecoder(strings.NewReader(sql))
			for {
				var e clickHouseEvent
				if decoder.Decode(&e) != nil {
					break
				}
				f.facts[receiptKey{e.ProjectID, e.UUID}] = true
			}
		}
	})
	return f
}
func finiteReceiptService(t *testing.T, limit int64) (*Service, *receiptHistoryFixture) {
	t.Helper()
	s := service(t, 10000)
	f := receiptHistory(t)
	s.ch = f.c
	s.hotReceiptLimit = limit
	return s, f
}
func deliverReceipt(t *testing.T, s *Service, token, uuid string) {
	t.Helper()
	request(t, s, "POST", "/capture/", "", captureBody(token, uuid), 200)
	if err := s.flushDelivery(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestReceiptsFiniteExactRestartAndDowngrade(t *testing.T) {
	s, f := finiteReceiptService(t, 2)
	projectID, token := project(t, s)
	for _, uuid := range []string{"old", "middle", "latest"} {
		deliverReceipt(t, s, token, uuid)
	}
	if receiptCount(t, s) != 2 {
		t.Fatal("hot ledger is not bounded")
	}
	var old int
	s.db.QueryRow(`SELECT count(*) FROM event_receipts WHERE uuid='old'`).Scan(&old)
	if old != 0 {
		t.Fatal("old receipt not evicted")
	}
	result := request(t, s, "POST", "/capture/", "", captureBody(token, "old"), 200)
	if result["duplicates"] != float64(1) {
		t.Fatal("evicted identity was accepted")
	}
	// The archive lookup includes tombstones and all receipt partitions; row data
	// is deliberately irrelevant to the membership proof.
	f.mu.Lock()
	f.facts[receiptKey{projectID, "deleted-cold"}] = true
	f.mu.Unlock()
	result = request(t, s, "POST", "/capture/", "", captureBody(token, "deleted-cold"), 200)
	if result["duplicates"] != float64(1) {
		t.Fatal("physical archived identity lost")
	}
	_, err := s.db.Exec(`INSERT INTO events(project_id,uuid,event,distinct_id,properties,timestamp,received_at) VALUES(?,'legacy','e','d','{}','','')`, projectID)
	if err == nil || !strings.Contains(err.Error(), "protocol-aware") {
		t.Fatal("legacy writer bypassed archive protocol", err)
	}
	path := s.dbPathForReceiptTest(t)
	s.Close()
	if _, err := Open(Config{DBPath: path}); !errors.Is(err, errReceiptHistory) {
		t.Fatal("missing authoritative source admitted", err)
	}
	changed := f.c.config
	changed.User = "other"
	if _, err := Open(Config{DBPath: path, ClickHouse: &changed}); !errors.Is(err, errReceiptHistory) {
		t.Fatal("changed source admitted", err)
	}
	// Turning eviction off cannot turn authoritative historical lookup off.
	restarted, err := Open(Config{DBPath: path, ClickHouse: &f.c.config})
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	result = request(t, restarted, "POST", "/capture/", "", captureBody(token, "old"), 200)
	if result["duplicates"] != float64(1) {
		t.Fatal("restart/downgrade lost archived identity")
	}
}
func (s *Service) dbPathForReceiptTest(t *testing.T) string {
	t.Helper()
	var seq int
	var name, path string
	if err := s.db.QueryRow(`PRAGMA database_list`).Scan(&seq, &name, &path); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestReceiptsNoRemoteUntilEvictionAndOutage(t *testing.T) {
	s, f := finiteReceiptService(t, 2)
	_, token := project(t, s)
	f.mu.Lock()
	f.unavailable = true
	f.mu.Unlock()
	request(t, s, "POST", "/capture/", "", captureBody(token, "first"), 200)
	if f.probes != 0 {
		t.Fatal("complete local history performed a remote preflight")
	}
	f.mu.Lock()
	f.unavailable = false
	f.mu.Unlock()
	if err := s.flushDelivery(context.Background()); err != nil {
		t.Fatal(err)
	}
	deliverReceipt(t, s, token, "second")
	deliverReceipt(t, s, token, "third")
	f.mu.Lock()
	f.unavailable = true
	f.mu.Unlock()
	request(t, s, "POST", "/capture/", "", captureBody(token, "third"), 200)
	request(t, s, "POST", "/capture/", "", captureBody(token, "first"), 503)
	request(t, s, "POST", "/capture/", "", captureBody(token, "new"), 503)
	if eventCount(t, s) != 0 {
		t.Fatal("outage accepted unknown history")
	}
}
func TestReceiptsLookupEvictionAckRaceAndHighChurn(t *testing.T) {
	s, _ := finiteReceiptService(t, 1)
	p, token := project(t, s)
	deliverReceipt(t, s, token, "first")
	deliverReceipt(t, s, token, "second")
	// An in-flight negative lookup pins its UUID against receipt eviction until
	// its final SQLite transaction has checked both local and remote evidence.
	lookup, err := s.prepareReceiptLookup(context.Background(), p, []Event{{UUID: "race"}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.releaseReceiptLookup(lookup)
	request(t, s, "POST", "/capture/", "", captureBody(token, "race"), 200)
	if err = s.flushDelivery(context.Background()); err != nil {
		t.Fatal(err)
	}
	for i := range 20 {
		deliverReceipt(t, s, token, fmt.Sprintf("churn-%d", i))
	}
	tx, err := s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err = s.validateReceiptLookup(context.Background(), tx, lookup); err != nil {
		t.Fatal("unrelated churn invalidated bounded lookup", err)
	}
	exists, err := localReceipt(context.Background(), tx, p, "race")
	tx.Rollback()
	if err != nil || !exists {
		t.Fatal("ack/eviction raced past lookup lease", err)
	}
	s.releaseReceiptLookup(lookup)
	deliverReceipt(t, s, token, "after-release")
	if receiptCount(t, s) != 1 {
		t.Fatal("lease cleanup did not converge")
	}
}
func TestReceiptsFirstEvictionRaceAndLeaseExpiry(t *testing.T) {
	s, _ := finiteReceiptService(t, 1)
	p, token := project(t, s)
	deliverReceipt(t, s, token, "a")
	lookup, err := s.prepareReceiptLookup(context.Background(), p, []Event{{UUID: "unknown"}})
	if err != nil {
		t.Fatal(err)
	}
	deliverReceipt(t, s, token, "b")
	tx, _ := s.db.Begin()
	err = s.validateReceiptLookup(context.Background(), tx, lookup)
	tx.Rollback()
	if !errors.Is(err, errReceiptHistory) {
		t.Fatal("first eviction race admitted stale local-only view")
	}
	lookup, err = s.prepareReceiptLookup(context.Background(), p, []Event{{UUID: "unknown"}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.releaseReceiptLookup(lookup)
	s.clock = func() time.Time { return time.Now().Add(time.Minute) }
	tx, _ = s.db.Begin()
	err = s.validateReceiptLookup(context.Background(), tx, lookup)
	tx.Rollback()
	if !errors.Is(err, errReceiptHistory) {
		t.Fatal("expired lookup admitted")
	}
}
func TestReceiptsMigrationOnlyVerifiedHistoryAndPinnedHeads(t *testing.T) {
	s, f := finiteReceiptService(t, 0)
	p, _ := project(t, s)
	for i := range 600 {
		uuid := fmt.Sprintf("legacy-%04d", i)
		if _, err := s.db.Exec(`INSERT INTO event_receipts(project_id,uuid) VALUES(?,?)`, p, uuid); err != nil {
			t.Fatal(err)
		}
		f.facts[receiptKey{p, uuid}] = true
	}
	if _, err := s.db.Exec(`INSERT INTO event_receipts(project_id,uuid) VALUES(?,'unproven')`, p); err != nil {
		t.Fatal(err)
	}
	s.hotReceiptLimit = 2
	for range 5 {
		progress, err := s.reconcileReceipts(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if !progress {
			break
		}
	}
	if receiptCount(t, s) != 2 {
		t.Fatal("upgraded ledger did not converge")
	}
	var n int
	s.db.QueryRow(`SELECT count(*) FROM event_receipts WHERE uuid='unproven'`).Scan(&n)
	if n != 1 {
		t.Fatal("unproven legacy receipt was discarded")
	}
	// A local mutation head protects an identity before a tombstone has any CH
	// history, and permits removing its redundant local receipt.
	_, err := s.db.Exec(`INSERT INTO event_heads(project_id,uuid,revision,is_deleted,pending) VALUES(?,'unproven',1,1,1)`, p)
	if err != nil {
		t.Fatal(err)
	}
	s.hotReceiptLimit = 1
	if _, err = s.reconcileReceipts(context.Background()); err != nil {
		t.Fatal(err)
	}
	exists, err := localReceipt(context.Background(), s.db, p, "unproven")
	if err != nil || !exists {
		t.Fatal("pending deleted identity lost")
	}
}
func TestReceiptsUnprovenLegacyFailsClosedAndHistoricalProbeBound(t *testing.T) {
	s, _ := finiteReceiptService(t, 1)
	p, token := project(t, s)
	s.db.Exec(`INSERT INTO event_receipts(project_id,uuid) VALUES(?,'lost1'),(?,'lost2')`, p, p)
	if _, err := s.reconcileReceipts(context.Background()); !errors.Is(err, errReceiptBudget) {
		t.Fatal("missing legacy history pruned", err)
	}
	request(t, s, "POST", "/capture/", "", captureBody(token, "new"), 503)
	request(t, s, "POST", "/capture/", "", captureBody(token, "lost1"), 200)
	s.db.Exec(`INSERT INTO metadata(key,value) VALUES('receipt_fallback_required','1')`)
	s.receiptSlots <- struct{}{}
	s.receiptSlots <- struct{}{}
	request(t, s, "POST", "/capture/", "", captureBody(token, "unknown"), 503)
	<-s.receiptSlots
	<-s.receiptSlots
	if receiptCount(t, s) != 2 {
		t.Fatal("unproven identities lost")
	}
}
func TestReceiptsInvalidConfiguration(t *testing.T) {
	for _, limit := range []int64{-1, 1} {
		s, err := Open(Config{DBPath: filepath.Join(t.TempDir(), "events.db"), BootstrapToken: adminToken, HotReceiptLimit: limit})
		if err == nil {
			if s != nil {
				s.Close()
			}
			t.Fatal("invalid finite receipt configuration accepted")
		}
	}
}

func TestReceiptsPreEvictionMutationHeadActivatesLegacyGuard(t *testing.T) {
	s, _ := finiteReceiptService(t, 10)
	p, token := project(t, s)
	s.db.Exec(`INSERT INTO event_heads(project_id,uuid,revision,is_deleted,pending) VALUES(?,'pending-delete',1,1,1)`, p)
	tx, err := s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err = s.recordReceipt(context.Background(), tx, p, "pending-delete", ""); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if receiptCount(t, s) != 0 {
		t.Fatal("redundant head receipt consumed finite budget")
	}
	_, err = s.db.Exec(`INSERT INTO events(project_id,uuid,event,distinct_id,properties,timestamp,received_at) VALUES(?,'pending-delete','e','d','{}','','')`, p)
	if err == nil || !strings.Contains(err.Error(), "protocol-aware") {
		t.Fatal("omitted head receipt allowed a legacy writer", err)
	}
	result := request(t, s, "POST", "/capture/", "", captureBody(token, "pending-delete"), 200)
	if result["duplicates"] != float64(1) {
		t.Fatal("pending deletion lost its identity")
	}
}

func TestReceiptsAdditiveMigrationPreservesLegacyRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	s, err := Open(Config{DBPath: path, BootstrapToken: adminToken})
	if err != nil {
		t.Fatal(err)
	}
	p, token := project(t, s)
	request(t, s, "POST", "/capture/", "", captureBody(token, "pending"), 200)
	if _, err = s.db.Exec(`DROP TRIGGER receipt_protocol_guard; DROP TRIGGER receipt_head_protocol_guard; ALTER TABLE events DROP COLUMN capture_protocol; INSERT INTO event_receipts(project_id,uuid) VALUES(?,'legacy-receipt')`, p); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s, err = Open(Config{DBPath: path})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if eventCount(t, s) != 1 || receiptCount(t, s) != 1 {
		t.Fatal("migration changed historical logical facts")
	}
	for _, uuid := range []string{"pending", "legacy-receipt"} {
		result := request(t, s, "POST", "/capture/", "", captureBody(token, uuid), 200)
		if result["duplicates"] != float64(1) {
			t.Fatal("migration lost identity", uuid)
		}
	}
	request(t, s, "POST", "/capture/", "", captureBody(token, "new"), 200)
}
