package porcupine

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

type mutationProjectionStore struct {
	mu          sync.Mutex
	rows        map[string]clickHouseEvent
	bodies      []string
	failAfter   atomic.Int32
	beforeWrite func()
}

func mutationClickHouse(t *testing.T) (*clickHouse, *mutationProjectionStore) {
	t.Helper()
	store := &mutationProjectionStore{rows: map[string]clickHouseEvent{}}
	c := testClickHouse(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if strings.HasPrefix(r.URL.Query().Get("query"), "INSERT INTO ") {
			if store.beforeWrite != nil {
				store.beforeWrite()
			}
			store.mu.Lock()
			defer store.mu.Unlock()
			store.bodies = append(store.bodies, string(body))
			decoder := json.NewDecoder(strings.NewReader(string(body)))
			inserted := 0
			for decoder.More() {
				var row clickHouseEvent
				if err := decoder.Decode(&row); err != nil {
					t.Error(err)
					return
				}
				key := row.ProjectID + "/" + row.UUID
				if previous, ok := store.rows[key]; !ok || previous.Revision <= row.Revision {
					store.rows[key] = row
				}
				inserted++
				if fail := store.failAfter.Load(); fail > 0 && inserted >= int(fail) {
					w.WriteHeader(503)
					return
				}
			}
			return
		}
		if strings.HasPrefix(string(body), "SELECT event_id,") {
			decodeParameter := func(marker string) string {
				_, after, ok := strings.Cut(string(body), marker+"=unhex('")
				if !ok {
					return ""
				}
				value, _, _ := strings.Cut(after, "')")
				raw, _ := hex.DecodeString(value)
				return string(raw)
			}
			key := decodeParameter("project_id") + "/" + decodeParameter("uuid")
			store.mu.Lock()
			row, ok := store.rows[key]
			store.mu.Unlock()
			if ok {
				json.NewEncoder(w).Encode(row)
			}
			return
		}
		t.Errorf("unexpected mutation ClickHouse query %q", string(body))
	})
	return c, store
}

func TestMutationDeliveryPartialAckRetryRestartAndPayloadCleanup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	s, err := Open(Config{DBPath: path, BootstrapToken: adminToken})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { s.Close() }()
	ctx := mutationContext("actor")
	c, store := mutationClickHouse(t)
	s.ch = c
	projectID, ingest := project(t, s)
	for _, uuid := range []string{"one", "two"} {
		request(t, s, "POST", "/capture/", "", captureBody(ingest, uuid), 200)
		if _, err := s.editEvent(ctx, mutationEdit(projectID, uuid, "request-"+uuid, "0")); err != nil {
			t.Fatal(err)
		}
	}
	store.beforeWrite = func() {
		// Network delivery must not retain a SQLite transaction or connection.
		var count int
		if err := s.db.QueryRow(`SELECT count(*) FROM mutation_outbox`).Scan(&count); err != nil || count != 2 {
			t.Errorf("SQLite inaccessible during HTTP insert: count=%d err=%v", count, err)
		}
	}
	store.failAfter.Store(1)
	if delivered, err := s.deliverMutations(context.Background()); err == nil || delivered {
		t.Fatal("partial ClickHouse acknowledgment was accepted")
	}
	if mutationCount(t, s, "mutation_outbox") != 2 {
		t.Fatal("failed projection dropped pending payload")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(Config{DBPath: path})
	if err != nil {
		t.Fatal(err)
	}
	s.ch = c
	store.failAfter.Store(0)
	if delivered, err := s.deliverMutations(context.Background()); err != nil || !delivered {
		t.Fatal("mutation retry failed", err)
	}
	store.beforeWrite = nil
	store.mu.Lock()
	stable := len(store.bodies) == 2 && store.bodies[0] == store.bodies[1]
	store.mu.Unlock()
	if !stable || mutationCount(t, s, "mutation_outbox") != 0 || mutationCount(t, s, "mutation_audit") != 2 {
		t.Fatal("retry projection changed or acknowledgment lost durable audit")
	}
	var pending, applied int
	if err := s.db.QueryRow(`SELECT sum(pending) FROM event_heads`).Scan(&pending); err != nil || pending != 0 {
		t.Fatal("ack did not clear head pending state", err)
	}
	if err := s.db.QueryRow(`SELECT count(*) FROM mutation_audit WHERE state='applied' AND applied_at<>''`).Scan(&applied); err != nil || applied != 2 {
		t.Fatal("ack did not record applied status", err)
	}
	current, err := s.getEvent(ctx, &EventGetInput{ID: projectID, UUID: "one"})
	if err != nil || current.Body.Pending || current.Body.Event.Event != "corrected" {
		t.Fatal("applied current projection lookup failed", err)
	}
	retry, err := s.editEvent(ctx, mutationEdit(projectID, "one", "request-one", "0"))
	if err != nil || retry.Body.State != "applied" || retry.Body.Revision != "1" {
		t.Fatal("applied idempotency retry lost original revision", err)
	}
	// The audit and event heads store metadata and hashes only after ACK.
	rows, err := s.db.Query(`SELECT actor_id,operation,idempotency_key_hash,request_hash FROM mutation_audit`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var actor, operation, keyHash, requestHash string
		if err := rows.Scan(&actor, &operation, &keyHash, &requestHash); err != nil {
			t.Fatal(err)
		}
		if actor != "actor" || operation != "edit" || len(keyHash) != 64 || len(requestHash) != 64 || strings.Contains(keyHash, "request-") {
			t.Fatal("audit retained request payload instead of metadata hashes")
		}
	}
}

func TestMutationDeliveryAckCleanupIsAtomic(t *testing.T) {
	s := service(t, 20)
	ctx := mutationContext("actor")
	c, store := mutationClickHouse(t)
	s.ch = c
	projectID, ingest := project(t, s)
	request(t, s, "POST", "/capture/", "", captureBody(ingest, "atomic"), 200)
	response, err := s.editEvent(ctx, mutationEdit(projectID, "atomic", "atomic-edit", "0"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`CREATE TRIGGER fail_ack BEFORE UPDATE ON event_heads BEGIN SELECT RAISE(FAIL,'disk failure'); END`); err != nil {
		t.Fatal(err)
	}
	if delivered, err := s.deliverMutations(context.Background()); err == nil || delivered {
		t.Fatal("local ACK failure was ignored")
	}
	status, err := s.getEventMutation(ctx, &EventMutationInput{ID: projectID, MutationID: response.Body.MutationID})
	if err != nil || status.Body.State != "pending" || mutationCount(t, s, "mutation_outbox") != 1 {
		t.Fatal("ACK failure partially committed audit or payload cleanup", err)
	}
	if _, err := s.db.Exec(`DROP TRIGGER fail_ack`); err != nil {
		t.Fatal(err)
	}
	if delivered, err := s.deliverMutations(context.Background()); err != nil || !delivered {
		t.Fatal("ACK recovery failed", err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.bodies) != 2 || store.bodies[0] != store.bodies[1] {
		t.Fatal("local ACK failure changed retried projection")
	}
}

func TestMutationDeleteWinsAgainstLateOriginalAndCannotResurrect(t *testing.T) {
	s := service(t, 20)
	ctx := mutationContext("actor")
	c, store := mutationClickHouse(t)
	s.ch = c
	projectID, ingest := project(t, s)
	request(t, s, "POST", "/capture/", "", captureBody(ingest, "deleted"), 200)
	originals, err := s.deliveryBatch(context.Background())
	if err != nil || len(originals) != 1 {
		t.Fatal("cannot simulate in-flight original", err)
	}
	response, err := s.deleteEvent(ctx, &EventDeleteInput{ID: projectID, UUID: "deleted", Body: EventDeleteBody{IdempotencyKey: "delete", ExpectedRevision: "0"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.deliverMutations(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := c.insert(context.Background(), originals); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	row := store.rows[projectID+"/deleted"]
	store.mu.Unlock()
	if row.Revision == 0 || row.IsDeleted != 1 || row.Event != "" || row.DistinctID != "" || row.Timestamp != "" || row.Properties != "{}" || row.OriginalSnapshot != "" {
		t.Fatal("late revision-zero original resurrected tombstoned event")
	}
	_, err = s.getEvent(ctx, &EventGetInput{ID: projectID, UUID: "deleted"})
	mutationErrorStatus(t, err, 410)
	_, err = s.editEvent(ctx, mutationEdit(projectID, "deleted", "resurrect", response.Body.Revision))
	mutationErrorStatus(t, err, 410)
	retry, err := s.deleteEvent(ctx, &EventDeleteInput{ID: projectID, UUID: "deleted", Body: EventDeleteBody{IdempotencyKey: "delete", ExpectedRevision: "0"}})
	if err != nil || retry.Body.State != "applied" || retry.Body.Revision != response.Body.Revision {
		t.Fatal("applied delete retry did not preserve result", err)
	}
}

func TestMutationRestoredSQLiteAdvancesAboveClickHouseRevision(t *testing.T) {
	s := service(t, 20)
	ctx := mutationContext("actor")
	c, store := mutationClickHouse(t)
	s.ch = c
	projectID, ingest := project(t, s)
	request(t, s, "POST", "/capture/", "", captureBody(ingest, "restored"), 200)
	originals, err := s.deliveryBatch(context.Background())
	if err != nil || len(originals) != 1 {
		t.Fatal(err)
	}
	event := originals[0]
	snapshot, err := originalSnapshot(event)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.insertProjections(context.Background(), []eventProjection{{Event: event, Revision: 900, OriginalSnapshot: snapshot}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`DELETE FROM events`); err != nil {
		t.Fatal(err)
	}
	input := mutationEdit(projectID, "restored", "after-restore", "900")
	input.Body.Snapshot.Properties["token"] = "must-never-persist"
	response, err := s.editEvent(ctx, input)
	if err != nil || response.Body.Revision != "901" {
		t.Fatalf("restored local sequence did not advance: %+v %v", response, err)
	}
	if _, err := s.deliverMutations(context.Background()); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	row := store.rows[projectID+"/restored"]
	store.mu.Unlock()
	if row.Revision != 901 || row.Event != "corrected" || strings.Contains(row.Properties, "must-never-persist") || row.OriginalSnapshot != snapshot {
		t.Fatal("restored mutation lost original snapshot, SDK secret removal, or revision ordering")
	}
	// An unchecked future client revision cannot advance the SQLite sequence.
	_, err = s.editEvent(ctx, mutationEdit(projectID, "restored", "bad-future", "9999"))
	mutationErrorStatus(t, err, 409)
	var seq int64
	if err := s.db.QueryRow(`SELECT seq FROM sqlite_sequence WHERE name='mutation_audit'`).Scan(&seq); err != nil || seq != 901 {
		t.Fatal("unchecked expected_revision advanced mutation sequence", err)
	}
}

func TestMutationRepeatedEditsPreserveFirstPrivateOriginal(t *testing.T) {
	s := service(t, 20)
	ctx := mutationContext("actor")
	c, store := mutationClickHouse(t)
	s.ch = c
	projectID, ingest := project(t, s)
	request(t, s, "POST", "/capture/", "", captureBody(ingest, "twice"), 200)
	first, err := s.editEvent(ctx, mutationEdit(projectID, "twice", "first-edit", "0"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.deliverMutations(context.Background()); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	before := store.rows[projectID+"/twice"]
	store.mu.Unlock()
	second := mutationEdit(projectID, "twice", "second-edit", first.Body.Revision)
	second.Body.Snapshot.Event = "corrected-again"
	if _, err := s.editEvent(ctx, second); err != nil {
		t.Fatal(err)
	}
	if _, err := s.deliverMutations(context.Background()); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	after := store.rows[projectID+"/twice"]
	store.mu.Unlock()
	if after.Revision <= before.Revision || after.Event != "corrected-again" || before.OriginalSnapshot == "" || after.OriginalSnapshot != before.OriginalSnapshot || after.EventID != before.EventID || after.ReceivedAt != before.ReceivedAt {
		t.Fatal("second edit changed private original or immutable event metadata")
	}
}
