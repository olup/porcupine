package porcupine

import (
	"context"
	"strings"
	"testing"
)

func TestMutationDeliveryAckRetryRestartAndPayloadCleanup(t *testing.T) {
	s, config := nativeCoreService(t)
	ctx := mutationContext("actor")
	projectID, ingest := project(t, s)
	for _, uuid := range []string{"one", "two"} {
		request(t, s, "POST", "/capture/", "", captureBody(ingest, uuid), 200)
		if _, err := s.editEvent(ctx, mutationEdit(projectID, uuid, "request-"+uuid, "0")); err != nil {
			t.Fatal(err)
		}
	}
	var originalPayload string
	if err := s.db.QueryRow(`SELECT group_concat(payload,'') FROM mutation_outbox ORDER BY seq`).Scan(&originalPayload); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`CREATE TRIGGER fail_mutation_ack BEFORE DELETE ON mutation_outbox BEGIN SELECT RAISE(FAIL,'simulated acknowledgment loss'); END`); err != nil {
		t.Fatal(err)
	}
	if delivered, err := s.deliverMutations(context.Background()); err == nil || delivered {
		t.Fatal("failed SQLite acknowledgment was reported as successful")
	}
	if mutationCount(t, s, "mutation_outbox") != 2 {
		t.Fatal("lost acknowledgment removed pending mutation payloads")
	}
	projected, err := s.lake.Load(ctx, projectID, "one")
	if err != nil || projected.Event.Event != "corrected" {
		t.Fatal("native commit must precede failed local acknowledgment", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	stopCoreDelivery(s)
	var replayPayload string
	if err := s.db.QueryRow(`SELECT group_concat(payload,'') FROM mutation_outbox ORDER BY seq`).Scan(&replayPayload); err != nil {
		t.Fatal(err)
	}
	if replayPayload != originalPayload {
		t.Fatal("restart changed immutable pending projections")
	}
	if _, err := s.db.Exec(`DROP TRIGGER fail_mutation_ack`); err != nil {
		t.Fatal(err)
	}
	if delivered, err := s.deliverMutations(context.Background()); err != nil || !delivered {
		t.Fatal("native mutation retry failed", err)
	}
	if mutationCount(t, s, "mutation_outbox") != 0 || mutationCount(t, s, "mutation_audit") != 2 {
		t.Fatal("retry lost payload cleanup or durable audit")
	}
	var pending, applied int
	if err := s.db.QueryRow(`SELECT sum(pending) FROM event_heads`).Scan(&pending); err != nil || pending != 0 {
		t.Fatal("ack did not clear head pending state", err)
	}
	if err := s.db.QueryRow(`SELECT count(*) FROM mutation_audit WHERE state='applied' AND applied_at<>''`).Scan(&applied); err != nil || applied != 2 {
		t.Fatal("applied audit state missing", err)
	}
	current, err := s.getEvent(ctx, &EventGetInput{ID: projectID, UUID: "one"})
	if err != nil || current.Body.Pending || current.Body.Event.Event != "corrected" {
		t.Fatal("applied current projection lookup failed", err)
	}
	retry, err := s.editEvent(ctx, mutationEdit(projectID, "one", "request-one", "0"))
	if err != nil || retry.Body.State != "applied" || retry.Body.Revision != "1" {
		t.Fatal("applied retry lost original revision", err)
	}
}

func TestMutationDeliveryAckCleanupIsAtomic(t *testing.T) {
	s, _ := nativeCoreService(t)
	ctx := mutationContext("actor")
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
}

func TestMutationDeleteWinsAgainstLateOriginalAndCannotResurrect(t *testing.T) {
	s, _ := nativeCoreService(t)
	ctx := mutationContext("actor")
	projectID, ingest := project(t, s)
	request(t, s, "POST", "/capture/", "", captureBody(ingest, "deleted"), 200)
	originals, err := s.deliveryBatch(context.Background())
	if err != nil || len(originals) != 1 {
		t.Fatal("missing in-flight original", err)
	}
	response, err := s.deleteEvent(ctx, &EventDeleteInput{ID: projectID, UUID: "deleted", Body: EventDeleteBody{IdempotencyKey: "delete", ExpectedRevision: "0"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.deliverMutations(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.lake.Apply(ctx, duckLakeJourneyProjections(t, originals)); err != nil {
		t.Fatal(err)
	}
	row, err := s.lake.Load(ctx, projectID, "deleted")
	if err != nil || row.Revision == 0 || !row.IsDeleted || row.Event.Event != "" || row.DistinctID != "" || row.OriginalSnapshot != "" {
		t.Fatal("late original resurrected a deleted native event", row, err)
	}
	_, err = s.getEvent(ctx, &EventGetInput{ID: projectID, UUID: "deleted"})
	mutationErrorStatus(t, err, 410)
	_, err = s.editEvent(ctx, mutationEdit(projectID, "deleted", "resurrect", response.Body.Revision))
	mutationErrorStatus(t, err, 410)
	retry, err := s.deleteEvent(ctx, &EventDeleteInput{ID: projectID, UUID: "deleted", Body: EventDeleteBody{IdempotencyKey: "delete", ExpectedRevision: "0"}})
	if err != nil || retry.Body.State != "applied" || retry.Body.Revision != response.Body.Revision {
		t.Fatal("delete retry changed its result", err)
	}
}

func TestMutationRestoredSQLiteAdvancesAboveNativeRevision(t *testing.T) {
	s, _ := nativeCoreService(t)
	ctx := mutationContext("actor")
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
	if err := s.lake.Apply(ctx, []eventProjection{{Event: event, Revision: 900, OriginalSnapshot: snapshot}}); err != nil {
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
	if _, err := s.deliverMutations(ctx); err != nil {
		t.Fatal(err)
	}
	row, err := s.lake.Load(ctx, projectID, "restored")
	if err != nil || row.Revision != 901 || row.Event.Event != "corrected" || strings.Contains(string(row.Properties), "must-never-persist") || row.OriginalSnapshot != snapshot {
		t.Fatal("restored mutation lost original, secret stripping or revision ordering", err)
	}
	_, err = s.editEvent(ctx, mutationEdit(projectID, "restored", "bad-future", "9999"))
	mutationErrorStatus(t, err, 409)
	var seq int64
	if err := s.db.QueryRow(`SELECT seq FROM sqlite_sequence WHERE name='mutation_audit'`).Scan(&seq); err != nil || seq != 901 {
		t.Fatal("unverified revision advanced sequence", err)
	}
}

func TestMutationRepeatedEditsPreserveFirstPrivateOriginal(t *testing.T) {
	s, _ := nativeCoreService(t)
	ctx := mutationContext("actor")
	projectID, ingest := project(t, s)
	request(t, s, "POST", "/capture/", "", captureBody(ingest, "twice"), 200)
	first, err := s.editEvent(ctx, mutationEdit(projectID, "twice", "first-edit", "0"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.deliverMutations(ctx); err != nil {
		t.Fatal(err)
	}
	before, err := s.lake.Load(ctx, projectID, "twice")
	if err != nil {
		t.Fatal(err)
	}
	second := mutationEdit(projectID, "twice", "second-edit", first.Body.Revision)
	second.Body.Snapshot.Event = "corrected-again"
	if _, err := s.editEvent(ctx, second); err != nil {
		t.Fatal(err)
	}
	if _, err := s.deliverMutations(ctx); err != nil {
		t.Fatal(err)
	}
	after, err := s.lake.Load(ctx, projectID, "twice")
	if err != nil || after.Revision <= before.Revision || after.Event.Event != "corrected-again" || before.OriginalSnapshot == "" || after.OriginalSnapshot != before.OriginalSnapshot || after.ID != before.ID || after.ReceivedAt != before.ReceivedAt {
		t.Fatal("repeated edit changed immutable event facts", err)
	}
}
