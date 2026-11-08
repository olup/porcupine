package porcupine

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func receiptCount(t *testing.T, s *Service) int {
	t.Helper()
	var count int
	if err := s.db.QueryRow(`SELECT count(*) FROM event_receipts`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func TestDeliveryStatusProjectIsolation(t *testing.T) {
	s := service(t, 20)
	s.ch = testClickHouse(t, func(http.ResponseWriter, *http.Request) {})
	a, ingestA := project(t, s)
	b, ingestB := project(t, s)
	request(t, s, "POST", "/capture/", "", captureBody(ingestA, "a"), 200)
	if err := s.flushDelivery(context.Background()); err != nil {
		t.Fatal(err)
	}
	aStatus, err := s.deliveryStatus(context.Background(), a)
	if err != nil || aStatus.LastSuccess == "" || aStatus.OldestPendingReceivedAt != "" {
		t.Fatal("project A delivery was not recorded")
	}
	s.recordDeliveryResult(context.Background(), io.EOF)
	bStatus, err := s.deliveryStatus(context.Background(), b)
	if err != nil || bStatus.LastSuccess != "" || bStatus.Error != "" {
		t.Fatal("project B observed another project's delivery activity")
	}
	request(t, s, "POST", "/capture/", "", captureBody(ingestB, "b"), 200)
	bStatus, err = s.deliveryStatus(context.Background(), b)
	if err != nil || bStatus.LastSuccess != "" || bStatus.OldestPendingReceivedAt == "" || bStatus.Error != "ClickHouse delivery failed" {
		t.Fatal("pending project did not receive generic delivery health")
	}
}

func TestDeliveryLostAcknowledgmentRetryCleanupAndRestart(t *testing.T) {
	var insertBodies []string
	var bodiesMu sync.Mutex
	var loseAck atomic.Bool
	loseAck.Store(true)
	c := testClickHouse(t, func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Query().Get("query"), "INSERT INTO ") {
			return
		}
		body, _ := io.ReadAll(r.Body)
		bodiesMu.Lock()
		insertBodies = append(insertBodies, string(body))
		bodiesMu.Unlock()
		if loseAck.Load() {
			// ClickHouse may have committed the insert even if its reply is lost.
			connection, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			connection.Close()
		}
	})
	path := filepath.Join(t.TempDir(), "events.db")
	s, err := Open(Config{DBPath: path, BootstrapToken: adminToken, QueueMaxEvents: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if s != nil {
			s.Close()
		}
	}()
	s.ch = c
	projectID, ingest := project(t, s)
	request(t, s, "POST", "/capture/", "", captureBody(ingest, "original"), 200)
	if err = s.flushDelivery(context.Background()); err == nil {
		t.Fatal("lost acknowledgment was accepted")
	}
	if eventCount(t, s) != 1 || receiptCount(t, s) != 0 {
		t.Fatal("failed delivery removed pending payload")
	}
	status, err := s.deliveryStatus(context.Background(), projectID)
	if err != nil || status.Pending != 1 || !status.Enabled || status.Error != "ClickHouse delivery failed" || status.LastSuccess != "" {
		t.Fatalf("wrong failure status: %+v %v", status, err)
	}
	request(t, s, "POST", "/capture/", "", captureBody(ingest, "next"), 503)
	loseAck.Store(false)
	if err = s.flushDelivery(context.Background()); err != nil {
		t.Fatal(err)
	}
	bodiesMu.Lock()
	stable := len(insertBodies) == 2 && insertBodies[0] == insertBodies[1]
	bodiesMu.Unlock()
	if !stable {
		t.Fatal("retry changed event projection")
	}
	if eventCount(t, s) != 0 || receiptCount(t, s) != 1 {
		t.Fatal("ack did not replace payload with receipt")
	}
	status, err = s.deliveryStatus(context.Background(), projectID)
	if err != nil || status.Pending != 0 || status.Error != "" || status.LastSuccess == "" {
		t.Fatalf("wrong success status: %+v %v", status, err)
	}
	v := request(t, s, "POST", "/capture/", "", captureBody(ingest, "original"), 200)
	if v["duplicates"] != float64(1) || v["accepted"] != float64(0) || eventCount(t, s) != 0 {
		t.Fatal("receipt did not deduplicate original UUID")
	}
	request(t, s, "POST", "/capture/", "", captureBody(ingest, "next"), 200)
	if eventCount(t, s) != 1 {
		t.Fatal("acknowledgment did not free queue capacity")
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s = nil
	s, err = Open(Config{DBPath: path, QueueMaxEvents: 1})
	if err != nil {
		t.Fatal(err)
	}
	s.ch = c
	v = request(t, s, "POST", "/capture/", "", captureBody(ingest, "original"), 200)
	if v["duplicates"] != float64(1) || eventCount(t, s) != 1 || receiptCount(t, s) != 1 {
		t.Fatal("restart lost receipt or pending event")
	}
	if err = s.flushDelivery(context.Background()); err != nil {
		t.Fatal(err)
	}
	if eventCount(t, s) != 0 || receiptCount(t, s) != 2 {
		t.Fatal("restart did not deliver pending event")
	}
}

func TestDeliveryBoundedBatchesAndProjectStatus(t *testing.T) {
	var rowCounts []int
	c := testClickHouse(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Query().Get("query"), "INSERT INTO ") {
			body, _ := io.ReadAll(r.Body)
			if len(body) > 4<<20 {
				t.Error("batch exceeded byte bound")
			}
			rowCounts = append(rowCounts, strings.Count(string(body), "\n"))
		}
	})
	s := service(t, 200)
	s.ch = c
	a, _ := project(t, s)
	b, _ := project(t, s)
	tx, err := s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for i := range 105 {
		projectID := a
		if i == 104 {
			projectID = b
		}
		_, err = tx.Exec(`INSERT INTO events(project_id,uuid,event,distinct_id,properties,timestamp,received_at) VALUES(?,?,?,?,?,?,?)`, projectID, newID(), "event", "person", `{}`, "2026-11-07T21:00:00Z", "2026-11-07T21:00:00Z")
		if err != nil {
			tx.Rollback()
			t.Fatal(err)
		}
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	status, err := s.deliveryStatus(context.Background(), b)
	if err != nil || status.Pending != 1 {
		t.Fatalf("project pending count incorrect: %+v %v", status, err)
	}
	if err = s.flushDelivery(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(rowCounts) != 1 || rowCounts[0] != 100 || eventCount(t, s) != 5 {
		t.Fatal("delivery did not bound event count")
	}
	if err = s.flushDelivery(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(rowCounts) != 2 || rowCounts[1] != 5 || receiptCount(t, s) != 105 {
		t.Fatal("remaining events were not delivered")
	}
	large := strings.Repeat("x", 900000)
	for range 4 {
		props, _ := json.Marshal(map[string]string{"large": large})
		_, err = s.db.Exec(`INSERT INTO events(project_id,uuid,event,distinct_id,properties,timestamp,received_at) VALUES(?,?,?,?,?,?,?)`, a, newID(), "event", "person", string(props), now(), now())
		if err != nil {
			t.Fatal(err)
		}
	}
	if err = s.flushDelivery(context.Background()); err != nil {
		t.Fatal(err)
	}
	if rowCounts[len(rowCounts)-1] != 1 || eventCount(t, s) != 3 {
		t.Fatal("delivery did not apply byte bound")
	}
}

func TestDeliveryLoopRetriesSchemaFailureAndStops(t *testing.T) {
	var mu sync.Mutex
	attempts := 0
	inserted := make(chan struct{}, 1)
	c := testClickHouse(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Query().Get("query"), "INSERT INTO ") {
			select {
			case inserted <- struct{}{}:
			default:
			}
			return
		}
		mu.Lock()
		attempts++
		n := attempts
		mu.Unlock()
		if n == 1 {
			w.WriteHeader(503)
			io.WriteString(w, "secret failed schema SQL")
		}
	})
	s := service(t, 20)
	s.ch = c
	_, ingest := project(t, s)
	request(t, s, "POST", "/capture/", "", captureBody(ingest, "retry"), 200)
	s.startDelivery()
	select {
	case <-inserted:
	case <-time.After(4 * time.Second):
		t.Fatal("worker did not retry initialization")
	}
	s.deliveryCancel()
	select {
	case <-s.deliveryDone:
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not stop")
	}
}

func TestDeliveryReceiptAndPayloadCleanupAreAtomic(t *testing.T) {
	c := testClickHouse(t, func(w http.ResponseWriter, r *http.Request) {})
	s := service(t, 20)
	s.ch = c
	_, ingest := project(t, s)
	request(t, s, "POST", "/capture/", "", captureBody(ingest, "atomic"), 200)
	if _, err := s.db.Exec(`CREATE TRIGGER fail_receipt_cleanup BEFORE DELETE ON events BEGIN SELECT RAISE(FAIL,'cannot delete'); END;`); err != nil {
		t.Fatal(err)
	}
	if err := s.flushDelivery(context.Background()); err == nil {
		t.Fatal("cleanup error ignored")
	}
	if eventCount(t, s) != 1 || receiptCount(t, s) != 0 {
		t.Fatal("failed cleanup partially committed")
	}
	if _, err := s.db.Exec(`DROP TRIGGER fail_receipt_cleanup`); err != nil {
		t.Fatal(err)
	}
	if err := s.flushDelivery(context.Background()); err != nil {
		t.Fatal(err)
	}
	if eventCount(t, s) != 0 || receiptCount(t, s) != 1 {
		t.Fatal("retry did not acknowledge atomically")
	}
}

func TestDeliveryLargeLegacyEventDoesNotBlockQueue(t *testing.T) {
	var rowCounts []int
	c := testClickHouse(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Query().Get("query"), "INSERT INTO ") {
			body, _ := io.ReadAll(r.Body)
			rowCounts = append(rowCounts, strings.Count(string(body), "\n"))
		}
	})
	s := service(t, 20)
	s.ch = c
	projectID, ingest := project(t, s)
	// Ingestion originally escaped HTML while serializing properties, expanding
	// an accepted body below 1 MiB into a stored value above the batch target.
	properties, _ := json.Marshal(map[string]string{"html": strings.Repeat("<", 900000)})
	if len(properties) < 4<<20 {
		t.Fatal("test did not produce a large legacy value")
	}
	if _, err := s.db.Exec(`INSERT INTO events(project_id,uuid,event,distinct_id,properties,timestamp,received_at) VALUES(?,?,?,?,?,?,?)`, projectID, "legacy", "event", "person", string(properties), now(), now()); err != nil {
		t.Fatal(err)
	}
	request(t, s, "POST", "/capture/", "", captureBody(ingest, "later"), 200)
	if err := s.flushDelivery(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(rowCounts) != 1 || rowCounts[0] != 1 || eventCount(t, s) != 1 {
		t.Fatal("large legacy event was not delivered alone")
	}
	if err := s.flushDelivery(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(rowCounts) != 2 || rowCounts[1] != 1 || eventCount(t, s) != 0 || receiptCount(t, s) != 2 {
		t.Fatal("large legacy event blocked subsequent delivery")
	}
}
