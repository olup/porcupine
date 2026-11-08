package porcupine

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

type isolationBackend struct {
	requests atomic.Int32
	code     atomic.Int32
	status   atomic.Int32
	allRows  atomic.Bool
	mu       sync.Mutex
	accepted []clickHouseEvent
}

func isolationCH(t *testing.T, backend *isolationBackend) *clickHouse {
	t.Helper()
	return testClickHouse(t, func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Query().Get("query"), "INSERT INTO ") {
			return
		}
		backend.requests.Add(1)
		decoder := json.NewDecoder(r.Body)
		rows := []clickHouseEvent{}
		for {
			var row clickHouseEvent
			err := decoder.Decode(&row)
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Error(err)
				w.WriteHeader(400)
				return
			}
			rows = append(rows, row)
		}
		code, status := int(backend.code.Load()), int(backend.status.Load())
		bad := false
		for _, row := range rows {
			if row.Event == "poison" || backend.allRows.Load() {
				bad = true
			}
		}
		if bad && status != 0 {
			if code != 0 {
				w.Header().Set("X-ClickHouse-Exception-Code", fmt.Sprint(code))
			}
			w.WriteHeader(status)
			io.WriteString(w, "private rejected property and storage details")
			return
		}
		backend.mu.Lock()
		backend.accepted = append(backend.accepted, rows...)
		backend.mu.Unlock()
	})
}

func poisonBody(token, uuid string) string {
	return strings.Replace(captureBody(token, uuid), `"purchase"`, `"poison"`, 1)
}

func quarantineCount(t *testing.T, s *Service) int {
	t.Helper()
	var n int
	if err := s.db.QueryRow(`SELECT count(*) FROM delivery_quarantine`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestDeliveryQuarantineRetainsPayloadDedupCapacityAndOperatorRetry(t *testing.T) {
	for _, code := range []int32{321, 469} {
		s := service(t, 1)
		backend := &isolationBackend{}
		backend.code.Store(code)
		backend.status.Store(400)
		s.ch = isolationCH(t, backend)
		projectID, ingest := project(t, s)
		other, _ := project(t, s)
		request(t, s, "POST", "/capture/", "", poisonBody(ingest, "bad"), 200)
		before, err := s.deliveryBatch(context.Background())
		if err != nil || len(before) != 1 {
			t.Fatal("accepted original unavailable")
		}
		if err = s.flushDelivery(context.Background()); err != nil {
			t.Fatal(err)
		}
		if backend.requests.Load() != 2 || quarantineCount(t, s) != 1 || eventCount(t, s) != 1 || receiptCount(t, s) != 0 {
			t.Fatal("row was not confirmed twice and preserved")
		}
		var retainedCount, retainedBytes int64
		if err = s.db.QueryRow(`SELECT pending_count,pending_bytes FROM queue_usage WHERE id=1`).Scan(&retainedCount, &retainedBytes); err != nil || retainedCount != 1 || retainedBytes != queueEventBytes(before[0]) {
			t.Fatal("quarantine changed accepted payload capacity accounting", err)
		}
		status, err := s.deliveryStatus(context.Background(), projectID)
		if err != nil || status.Pending != 1 || status.Quarantined != 1 || status.Error != "Some accepted events require delivery repair" {
			t.Fatalf("quarantine status missing: %+v %v", status, err)
		}
		foreign, err := s.deliveryStatus(context.Background(), other)
		if err != nil || foreign.Quarantined != 0 || foreign.Error != "" {
			t.Fatal("quarantine details crossed project scope")
		}
		pending, err := s.deliveryBatch(context.Background())
		if err != nil || len(pending) != 0 {
			t.Fatal("quarantined row remained delivery-eligible")
		}
		duplicate := request(t, s, "POST", "/capture/", "", poisonBody(ingest, "bad"), 200)
		if duplicate["duplicates"] != float64(1) {
			t.Fatal("quarantine broke ingestion deduplication")
		}
		request(t, s, "POST", "/capture/", "", captureBody(ingest, "later"), 503)
		if changed, err := s.retryQuarantinedDelivery(context.Background(), other, before[0].ID); err != nil || changed {
			t.Fatal("operator retry crossed project scope")
		}
		backend.status.Store(0)
		if changed, err := s.retryQuarantinedDelivery(context.Background(), projectID, before[0].ID); err != nil || !changed {
			t.Fatal("operator retry did not restore eligibility", err)
		}
		if err = s.flushDelivery(context.Background()); err != nil {
			t.Fatal(err)
		}
		if quarantineCount(t, s) != 0 || eventCount(t, s) != 0 || receiptCount(t, s) != 1 {
			t.Fatal("retry did not acknowledge original")
		}
		backend.mu.Lock()
		rows := append([]clickHouseEvent{}, backend.accepted...)
		backend.mu.Unlock()
		if len(rows) != 1 || rows[0].EventID != uint64(before[0].ID) || rows[0].UUID != before[0].UUID || rows[0].ReceivedAt != before[0].ReceivedAt || rows[0].Revision != 0 {
			t.Fatal("retry changed accepted identity or receipt")
		}
		request(t, s, "POST", "/capture/", "", captureBody(ingest, "later"), 200)
	}
}

func TestDeliveryIsolationCursorReachesLateHealthyRowsAcrossProjects(t *testing.T) {
	s := service(t, 200)
	backend := &isolationBackend{}
	backend.code.Store(469)
	backend.status.Store(400)
	s.ch = isolationCH(t, backend)
	a, keyA := project(t, s)
	b, keyB := project(t, s)
	for i := range 100 {
		request(t, s, "POST", "/capture/", "", poisonBody(keyA, fmt.Sprintf("bad-%d", i)), 200)
	}
	request(t, s, "POST", "/capture/", "", captureBody(keyA, "late-same-project"), 200)
	request(t, s, "POST", "/capture/", "", captureBody(keyB, "late-other-project"), 200)
	if err := s.flushDelivery(context.Background()); err != nil {
		t.Fatal(err)
	}
	if backend.requests.Load() > deliveryIsolationProbes || quarantineCount(t, s) == 0 {
		t.Fatal("isolation exceeded bounded probe budget or made no progress")
	}
	if err := s.flushDelivery(context.Background()); err != nil {
		t.Fatal(err)
	}
	if receiptCount(t, s) != 2 || eventCount(t, s) != 100 {
		t.Fatal("failed prefix starved later healthy rows")
	}
	statusA, _ := s.deliveryStatus(context.Background(), a)
	statusB, _ := s.deliveryStatus(context.Background(), b)
	if statusA.Quarantined == 0 || statusA.Pending != 100 || statusB.Pending != 0 || statusB.Quarantined != 0 || statusB.Error != "" {
		t.Fatal("project delivery counts were not isolated")
	}
}

func TestDeliveryIsolationDoesNotQuarantineInfrastructureOrUnclassifiedErrors(t *testing.T) {
	for _, failure := range []struct{ status, code int32 }{{503, 0}, {500, 241}, {403, 497}, {404, 60}, {500, 27}, {500, 117}, {429, 469}, {503, 321}} {
		s := service(t, 10)
		backend := &isolationBackend{}
		backend.status.Store(failure.status)
		backend.code.Store(failure.code)
		backend.allRows.Store(true)
		s.ch = isolationCH(t, backend)
		_, ingest := project(t, s)
		request(t, s, "POST", "/capture/", "", poisonBody(ingest, "bad"), 200)
		if err := s.flushDelivery(context.Background()); err == nil {
			t.Fatal("unclassified failure ignored")
		}
		if backend.requests.Load() != 1 || quarantineCount(t, s) != 0 || eventCount(t, s) != 1 || receiptCount(t, s) != 0 {
			t.Fatal("outage or global failure quarantined/dropped accepted data")
		}
		backend.status.Store(0)
		if err := s.flushDelivery(context.Background()); err != nil {
			t.Fatal(err)
		}
		if eventCount(t, s) != 0 || receiptCount(t, s) != 1 {
			t.Fatal("outage recovery did not retry retained event")
		}
	}
}

func TestDeliveryQuarantineRestartAndAdditiveReadyMigration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	s, err := Open(Config{DBPath: path, BootstrapToken: adminToken})
	if err != nil {
		t.Fatal(err)
	}
	projectID, ingest := project(t, s)
	request(t, s, "POST", "/capture/", "", poisonBody(ingest, "persist"), 200)
	backend := &isolationBackend{}
	backend.code.Store(469)
	backend.status.Store(400)
	s.ch = isolationCH(t, backend)
	if err = s.flushDelivery(context.Background()); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s, err = Open(Config{DBPath: path})
	if err != nil {
		t.Fatal(err)
	}
	if status, err := s.deliveryStatus(context.Background(), projectID); err != nil || status.Quarantined != 1 {
		t.Fatal("restart lost quarantine")
	}
	if rows, err := s.deliveryBatch(context.Background()); err != nil || len(rows) != 0 {
		t.Fatal("restart reactivated quarantined payload")
	}
	if changed, err := s.retryQuarantinedDelivery(context.Background(), projectID, 1); err != nil || !changed {
		t.Fatal("retry after restart failed", err)
	}
	s.Close()
	// Model the prior schema without derived ready/quarantine tables. Existing
	// accepted payloads, IDs, receipts and capacity accounting remain untouched.
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`DROP TRIGGER delivery_ready_insert;DROP TRIGGER delivery_ready_update;DROP TRIGGER delivery_quarantine_insert;DROP TRIGGER delivery_quarantine_retry;DROP TABLE delivery_ready;DROP TABLE delivery_quarantine;`)
	db.Close()
	if err != nil {
		t.Fatal(err)
	}
	s, err = Open(Config{DBPath: path})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	rows, err := s.deliveryBatch(context.Background())
	if err != nil || len(rows) != 1 || rows[0].ID != 1 || rows[0].UUID != "persist" || rows[0].Event != "poison" {
		t.Fatal("additive startup migration changed accepted data", err)
	}
	backend.status.Store(0)
	s.ch = isolationCH(t, backend)
	if err = s.flushDelivery(context.Background()); err != nil {
		t.Fatal(err)
	}
	if eventCount(t, s) != 0 || receiptCount(t, s) != 1 {
		t.Fatal("migrated pending event failed to deliver")
	}
}

func TestQuarantinedOriginalMutationAckAndLateReplay(t *testing.T) {
	s := service(t, 10)
	backend := &isolationBackend{}
	backend.code.Store(469)
	backend.status.Store(400)
	s.ch = isolationCH(t, backend)
	a, ingest := project(t, s)
	request(t, s, "POST", "/capture/", "", poisonBody(ingest, "mutate"), 200)
	originals, err := s.deliveryBatch(context.Background())
	if err != nil || len(originals) != 1 {
		t.Fatal(err)
	}
	if err = s.flushDelivery(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err = s.editEvent(mutationContext("operator"), mutationEdit(a, "mutate", "repair", "0")); err != nil {
		t.Fatal(err)
	}
	if err = s.flushDelivery(context.Background()); err != nil {
		t.Fatal(err)
	}
	if eventCount(t, s) != 0 || quarantineCount(t, s) != 0 || receiptCount(t, s) != 1 || mutationCount(t, s, "mutation_outbox") != 0 {
		t.Fatal("acknowledged correction left original poison blocking its watermark")
	}
	backend.status.Store(0)
	if err = s.ch.insert(context.Background(), originals); err != nil {
		t.Fatal(err)
	}
	if err = s.acknowledgeOriginals(context.Background(), originals); err != nil {
		t.Fatal(err)
	}
	if changed, err := s.retryQuarantinedDelivery(context.Background(), a, originals[0].ID); err != nil || changed {
		t.Fatal("late retry recreated a removed original")
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	var winner clickHouseEvent
	for _, row := range backend.accepted {
		if row.UUID == "mutate" && row.Revision >= winner.Revision {
			winner = row
		}
	}
	if winner.Revision == 0 || winner.Event != "corrected" || winner.OriginalSnapshot == "" {
		t.Fatal("late original could supersede correction or erase evidence")
	}
}

func TestFailedMutationDoesNotBlockHealthyOriginals(t *testing.T) {
	s := service(t, 10)
	a, keyA := project(t, s)
	_, keyB := project(t, s)
	request(t, s, "POST", "/capture/", "", poisonBody(keyA, "mutation-source"), 200)
	s.ch = testClickHouse(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Query().Get("query"), "INSERT INTO ") {
			w.WriteHeader(503)
		}
	})
	if _, err := s.editEvent(mutationContext("operator"), mutationEdit(a, "mutation-source", "pending", "0")); err != nil {
		t.Fatal(err)
	}
	request(t, s, "POST", "/capture/", "", captureBody(keyB, "healthy-original"), 200)
	s.ch = testClickHouse(t, func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Query().Get("query"), "INSERT INTO ") {
			return
		}
		body, _ := io.ReadAll(r.Body)
		if strings.Contains(string(body), `"revision":1`) {
			w.WriteHeader(503)
		}
	})
	if err := s.flushDelivery(context.Background()); err == nil {
		t.Fatal("mutation failure was hidden")
	}
	if mutationCount(t, s, "mutation_outbox") != 1 || eventCount(t, s) != 0 || receiptCount(t, s) != 2 {
		t.Fatal("failed mutation blocked original delivery")
	}
}

func TestDeliveryIsolationRequiresConsistentSingletonConfirmation(t *testing.T) {
	s := service(t, 10)
	_, ingest := project(t, s)
	request(t, s, "POST", "/capture/", "", poisonBody(ingest, "bad"), 200)
	var requests atomic.Int32
	s.ch = testClickHouse(t, func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Query().Get("query"), "INSERT INTO ") {
			return
		}
		switch requests.Add(1) {
		case 1:
			w.Header().Set("X-ClickHouse-Exception-Code", "469")
			w.WriteHeader(400)
		case 2:
			w.WriteHeader(503)
		}
	})
	if err := s.flushDelivery(context.Background()); err == nil {
		t.Fatal("outage during confirmation was hidden")
	}
	if quarantineCount(t, s) != 0 || eventCount(t, s) != 1 || receiptCount(t, s) != 0 {
		t.Fatal("one row rejection plus an outage was treated as confirmed poison")
	}
	if err := s.flushDelivery(context.Background()); err != nil {
		t.Fatal(err)
	}
	if eventCount(t, s) != 0 || receiptCount(t, s) != 1 {
		t.Fatal("original was not recoverable after failed confirmation")
	}
}

func TestDeliveryQuarantinePreservesProjectExportWatermarks(t *testing.T) {
	s := service(t, 10)
	a, ingest := project(t, s)
	b, _ := project(t, s)
	backend := &isolationBackend{}
	backend.code.Store(469)
	backend.status.Store(400)
	s.ch = isolationCH(t, backend)
	request(t, s, "POST", "/capture/", "", poisonBody(ingest, "barrier"), 200)
	if err := s.flushDelivery(context.Background()); err != nil {
		t.Fatal(err)
	}
	enableTestExports(s, 100)
	s.exports.config.QueueThreshold = 1000
	ctx := exportContext(t, s, adminToken)
	blocked, err := s.createEventExport(ctx, exportInput(a, "quarantine-barrier"))
	if err != nil {
		t.Fatal(err)
	}
	healthy, err := s.createEventExport(ctx, exportInput(b, "healthy-project"))
	if err != nil {
		t.Fatal(err)
	}
	for _, check := range []struct {
		project, id string
		want        bool
	}{{a, blocked.Body.ID, false}, {b, healthy.Body.ID, true}} {
		job, err := s.loadExport(ctx, check.project, check.id)
		if err != nil {
			t.Fatal(err)
		}
		ready, err := s.exportAdmission(ctx, job)
		if err != nil || ready != check.want {
			t.Fatal("quarantine was silently skipped by a delivery watermark or blocked another project", err)
		}
	}
}
