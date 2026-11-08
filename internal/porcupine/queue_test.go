package porcupine

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func queueUsage(t *testing.T, s *Service) (count, bytes int64) {
	t.Helper()
	if err := s.db.QueryRow(`SELECT pending_count,pending_bytes FROM queue_usage WHERE id=1`).Scan(&count, &bytes); err != nil {
		t.Fatal(err)
	}
	return
}

func storedQueueBytes(t *testing.T, s *Service) int64 {
	t.Helper()
	var value int64
	if err := s.db.QueryRow(`SELECT COALESCE(SUM(64+length(CAST(project_id AS BLOB))+length(CAST(uuid AS BLOB))+length(CAST(event AS BLOB))+length(CAST(distinct_id AS BLOB))+length(CAST(properties AS BLOB))+length(CAST(timestamp AS BLOB))+length(CAST(received_at AS BLOB))),0) FROM events WHERE delivered=0`).Scan(&value); err != nil {
		t.Fatal(err)
	}
	return value
}

func queueBody(token, uuid string) string {
	value, _ := json.Marshal(map[string]any{"api_key": token, "uuid": uuid, "event": "クリック", "distinct_id": "訪問者", "properties": map[string]any{"city": "東京"}, "timestamp": "2026-11-01T00:00:00Z"})
	return string(value)
}

func fixedQueueClock(s *Service) {
	s.clock = func() time.Time { return time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC) }
}

func TestQueueByteCapacityAndDuplicateRetry(t *testing.T) {
	s := service(t, 100)
	fixedQueueClock(s)
	id, ingest := project(t, s)
	size := int64(64 + len(id) + len("one") + len("クリック") + len("訪問者") + len(`{"city":"東京"}`) + 2*len("2026-11-01T00:00:00Z"))
	s.queueMaxBytes = size
	request(t, s, "POST", "/capture/", "", queueBody(ingest, "one"), 200)
	count, bytes := queueUsage(t, s)
	if count != 1 || bytes != size || bytes != storedQueueBytes(t, s) {
		t.Fatalf("UTF-8 queue accounting wrong: count=%d bytes=%d expected=%d", count, bytes, size)
	}
	duplicate := request(t, s, "POST", "/capture/", "", queueBody(ingest, "one"), 200)
	if duplicate["accepted"] != float64(0) || duplicate["duplicates"] != float64(1) {
		t.Fatal("full byte queue rejected a free retry")
	}
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, httptest.NewRequest("POST", "/capture/", strings.NewReader(queueBody(ingest, "two"))))
	if w.Code != 503 || w.Header().Get("Retry-After") != "1" {
		t.Fatal("byte limit did not apply backpressure")
	}
	if count, bytes := queueUsage(t, s); count != 1 || bytes != size {
		t.Fatal("rejected event changed accounting")
	}
}

func TestQueueByteBatchRollbackAndGlobalScope(t *testing.T) {
	s := service(t, 100)
	fixedQueueClock(s)
	a, ingestA := project(t, s)
	_, ingestB := project(t, s)
	single := int64(64 + len(a) + len("one") + len("クリック") + len("訪問者") + len(`{"city":"東京"}`) + 2*len("2026-11-01T00:00:00Z"))
	s.queueMaxBytes = 2*single - 1
	var one, two map[string]any
	json.Unmarshal([]byte(queueBody(ingestA, "one")), &one)
	json.Unmarshal([]byte(queueBody(ingestA, "two")), &two)
	batch, _ := json.Marshal(map[string]any{"api_key": ingestA, "batch": []any{one, two}})
	request(t, s, "POST", "/batch/", "", string(batch), 503)
	if count, bytes := queueUsage(t, s); count != 0 || bytes != 0 || eventCount(t, s) != 0 {
		t.Fatal("partially accepted byte-limited batch")
	}
	request(t, s, "POST", "/capture/", "", queueBody(ingestA, "one"), 200)
	request(t, s, "POST", "/capture/", "", queueBody(ingestB, "two"), 503)
	if count, bytes := queueUsage(t, s); count != 1 || bytes != single {
		t.Fatal("projects did not share one byte budget")
	}
	s.queueMaxBytes = single
	request(t, s, "POST", "/capture/", "", queueBody(ingestA, "one"), 200)
	if count, bytes := queueUsage(t, s); count != 1 || bytes != single {
		t.Fatal("duplicate used queue bytes")
	}
}

func TestQueueByteAccountingUpdatesDeletesAndRollback(t *testing.T) {
	s := service(t, 100)
	_, ingest := project(t, s)
	request(t, s, "POST", "/capture/", "", queueBody(ingest, "one"), 200)
	if _, err := s.db.Exec(`UPDATE events SET properties=?`, `{"city":"東京東京東京"}`); err != nil {
		t.Fatal(err)
	}
	if count, bytes := queueUsage(t, s); count != 1 || bytes != storedQueueBytes(t, s) {
		t.Fatal("payload update did not adjust byte accounting")
	}
	if _, err := s.db.Exec(`UPDATE events SET delivered=1`); err != nil {
		t.Fatal(err)
	}
	if count, bytes := queueUsage(t, s); count != 0 || bytes != 0 {
		t.Fatal("delivered transition did not release pending bytes")
	}
	if _, err := s.db.Exec(`UPDATE events SET properties='{}'`); err != nil {
		t.Fatal(err)
	}
	if count, bytes := queueUsage(t, s); count != 0 || bytes != 0 {
		t.Fatal("historical delivered row was charged")
	}
	if _, err := s.db.Exec(`UPDATE events SET delivered=0`); err != nil {
		t.Fatal(err)
	}
	before := storedQueueBytes(t, s)
	tx, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`DELETE FROM events`); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if count, bytes := queueUsage(t, s); count != 1 || bytes != before {
		t.Fatal("rollback lost queue accounting")
	}
	if _, err := s.db.Exec(`DELETE FROM events`); err != nil {
		t.Fatal(err)
	}
	if count, bytes := queueUsage(t, s); count != 0 || bytes != 0 {
		t.Fatal("deletion did not release byte capacity")
	}
}

func TestQueueByteCapacityReleasedByDeliveryAndMutation(t *testing.T) {
	for _, mode := range []string{"delivery", "mutation"} {
		t.Run(mode, func(t *testing.T) {
			s := service(t, 100)
			fixedQueueClock(s)
			id, ingest := project(t, s)
			request(t, s, "POST", "/capture/", "", queueBody(ingest, "one"), 200)
			_, bytes := queueUsage(t, s)
			s.queueMaxBytes = bytes
			s.ch = testClickHouse(t, func(http.ResponseWriter, *http.Request) {})
			if mode == "delivery" {
				if delivered, err := s.deliverBatch(context.Background()); err != nil || !delivered {
					t.Fatal("delivery failed", err)
				}
			} else {
				request(t, s, "DELETE", "/api/v1/projects/"+id+"/events/one", adminToken, `{"idempotency_key":"delete-queued","expected_revision":"0"}`, 202)
			}
			if count, bytes := queueUsage(t, s); count != 0 || bytes != 0 {
				t.Fatal("accepted payload removal did not release accounting")
			}
			retried := request(t, s, "POST", "/capture/", "", queueBody(ingest, "one"), 200)
			if retried["duplicates"] != float64(1) {
				t.Fatal("receipt retry was not free")
			}
			request(t, s, "POST", "/capture/", "", queueBody(ingest, "two"), 200)
			if count, current := queueUsage(t, s); count != 1 || current != bytes {
				t.Fatal("released capacity was not reusable")
			}
		})
	}
}

func TestQueueByteAccountingMigrationAndRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	s, err := Open(Config{DBPath: path, BootstrapToken: adminToken})
	if err != nil {
		t.Fatal(err)
	}
	_, ingest := project(t, s)
	request(t, s, "POST", "/capture/", "", queueBody(ingest, "one"), 200)
	request(t, s, "POST", "/capture/", "", queueBody(ingest, "two"), 200)
	if _, err := s.db.Exec(`UPDATE events SET delivered=1 WHERE uuid='two'; DROP TRIGGER queue_usage_insert; DROP TRIGGER queue_usage_delete; DROP TRIGGER queue_usage_update; DROP TABLE queue_usage;`); err != nil {
		s.Close()
		t.Fatal(err)
	}
	expected := storedQueueBytes(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		s, err = Open(Config{DBPath: path, QueueMaxBytes: 1})
		if err != nil {
			t.Fatal(err)
		}
		if count, bytes := queueUsage(t, s); count != 1 || bytes != expected {
			s.Close()
			t.Fatal("existing queue was not backfilled once or restart double-counted it")
		}
		request(t, s, "POST", "/capture/", "", queueBody(ingest, "three"), 503)
		request(t, s, "POST", "/capture/", "", queueBody(ingest, "one"), 200)
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestConcurrentQueueByteCapacity(t *testing.T) {
	s := service(t, 100)
	fixedQueueClock(s)
	_, ingest := project(t, s)
	request(t, s, "POST", "/capture/", "", queueBody(ingest, "000"), 200)
	_, single := queueUsage(t, s)
	if _, err := s.db.Exec(`DELETE FROM events`); err != nil {
		t.Fatal(err)
	}
	s.queueMaxBytes = 3 * single
	var group sync.WaitGroup
	codes := make(chan int, 16)
	for i := 0; i < 16; i++ {
		uuid := string([]byte{'a', byte('a' + i), 'a'})
		group.Go(func() {
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, httptest.NewRequest("POST", "/capture/", strings.NewReader(queueBody(ingest, uuid))))
			codes <- w.Code
		})
	}
	group.Wait()
	close(codes)
	accepted := 0
	for code := range codes {
		if code == 200 {
			accepted++
		} else if code != 503 {
			t.Fatalf("unexpected concurrent response %d", code)
		}
	}
	if count, bytes := queueUsage(t, s); accepted != 3 || count != 3 || bytes != 3*single {
		t.Fatal("concurrent captures exceeded the byte budget")
	}
}

func TestQueueByteConfigurationDefaultAndInvalid(t *testing.T) {
	s := service(t, 100)
	if s.queueMaxBytes != DefaultQueueMaxBytes {
		t.Fatal("default queue byte budget changed")
	}
	if opened, err := Open(Config{DBPath: filepath.Join(t.TempDir(), "invalid.db"), BootstrapToken: adminToken, QueueMaxBytes: -1}); err == nil {
		opened.Close()
		t.Fatal("negative queue byte configuration accepted")
	}
}

func TestQueueByteAccountingDeliveryAcknowledgementRollback(t *testing.T) {
	s := service(t, 100)
	_, ingest := project(t, s)
	request(t, s, "POST", "/capture/", "", queueBody(ingest, "one"), 200)
	_, before := queueUsage(t, s)
	s.ch = testClickHouse(t, func(http.ResponseWriter, *http.Request) {})
	if _, err := s.db.Exec(`CREATE TRIGGER fail_queue_ack BEFORE DELETE ON events BEGIN SELECT RAISE(ABORT,'test ack failure'); END`); err != nil {
		t.Fatal(err)
	}
	if delivered, err := s.deliverBatch(context.Background()); err == nil || delivered {
		t.Fatal("forced acknowledgment failure did not roll back")
	}
	if count, bytes := queueUsage(t, s); count != 1 || bytes != before {
		t.Fatal("failed acknowledgement released queue bytes")
	}
	if _, err := s.db.Exec(`DROP TRIGGER fail_queue_ack`); err != nil {
		t.Fatal(err)
	}
	if delivered, err := s.deliverBatch(context.Background()); err != nil || !delivered {
		t.Fatal("delivery retry failed", err)
	}
	if count, bytes := queueUsage(t, s); count != 0 || bytes != 0 {
		t.Fatal("delivery retry did not release bytes exactly once")
	}
}

func TestQueueByteCapacityAcrossServiceInstances(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shared.db")
	first, err := Open(Config{DBPath: path, BootstrapToken: adminToken, QueueMaxEvents: 100})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { first.Close() })
	fixedQueueClock(first)
	_, ingest := project(t, first)
	request(t, first, "POST", "/capture/", "", queueBody(ingest, "000"), 200)
	_, single := queueUsage(t, first)
	if _, err := first.db.Exec(`DELETE FROM events`); err != nil {
		t.Fatal(err)
	}
	first.queueMaxBytes = 2 * single
	second, err := Open(Config{DBPath: path, QueueMaxEvents: 100, QueueMaxBytes: 2 * single})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { second.Close() })
	fixedQueueClock(second)
	var group sync.WaitGroup
	codes := make(chan int, 12)
	for i := 0; i < 12; i++ {
		target := first
		if i%2 != 0 {
			target = second
		}
		uuid := string([]byte{'b', byte('a' + i), 'b'})
		group.Go(func() {
			w := httptest.NewRecorder()
			target.Handler().ServeHTTP(w, httptest.NewRequest("POST", "/capture/", strings.NewReader(queueBody(ingest, uuid))))
			codes <- w.Code
		})
	}
	group.Wait()
	close(codes)
	accepted := 0
	for code := range codes {
		if code == 200 {
			accepted++
		} else if code != 503 {
			t.Fatalf("unexpected shared queue result %d", code)
		}
	}
	if count, bytes := queueUsage(t, second); accepted != 2 || count != 2 || bytes != 2*single {
		t.Fatal("separate services overfilled shared queue byte capacity")
	}
}
