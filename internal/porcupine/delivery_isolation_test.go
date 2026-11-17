package porcupine

import (
	"context"
	"strings"
	"testing"
)

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

// Persisted quarantine remains repairable; native storage failures themselves
// do not infer that a row is invalid or discard a previously accepted record.
func TestDeliveryQuarantineRetainsPayloadDedupCapacityAndOperatorRetry(t *testing.T) {
	t.Parallel()
	s, config := nativeCoreService(t)
	s.queueMax = 1
	projectID, ingest := project(t, s)
	request(t, s, "POST", "/capture/", "", captureBody(ingest, "retained"), 200)
	batch, err := s.deliveryBatch(context.Background())
	if err != nil || len(batch) != 1 {
		t.Fatal(err)
	}
	if changed, err := s.quarantineOriginal(context.Background(), batch[0], 321); err != nil || !changed {
		t.Fatal(err)
	}
	if err := s.flushDelivery(context.Background()); err != nil {
		t.Fatal(err)
	}
	if eventCount(t, s) != 1 || quarantineCount(t, s) != 1 || receiptCount(t, s) != 0 {
		t.Fatal("quarantine discarded accepted evidence")
	}
	retry := request(t, s, "POST", "/capture/", "", captureBody(ingest, "retained"), 200)
	if retry["duplicates"] != float64(1) {
		t.Fatal("quarantined UUID lost deduplication")
	}
	request(t, s, "POST", "/capture/", "", captureBody(ingest, "new"), 503)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	stopCoreDelivery(s)
	if quarantineCount(t, s) != 1 {
		t.Fatal("restart forgot accepted quarantine")
	}
	if changed, err := s.retryQuarantinedDelivery(context.Background(), projectID, batch[0].ID); err != nil || !changed {
		t.Fatal("operator could not retry original", err)
	}
	if err := s.flushDelivery(context.Background()); err != nil {
		t.Fatal(err)
	}
	if eventCount(t, s) != 0 || receiptCount(t, s) != 1 || quarantineCount(t, s) != 0 {
		t.Fatal("repaired delivery did not preserve cleanup semantics")
	}
	row, err := s.lake.Load(context.Background(), projectID, "retained")
	if err != nil || row.ID != batch[0].ID {
		t.Fatal("operator retry changed stable event identity", err)
	}
}

func TestQuarantinedOriginalMutationAckAndLateReplay(t *testing.T) {
	t.Parallel()
	for _, operation := range []string{"edit", "delete"} {
		t.Run(operation, func(t *testing.T) {
			s, _ := nativeCoreService(t)
			ctx := mutationContext("actor")
			projectID, ingest := project(t, s)
			request(t, s, "POST", "/capture/", "", captureBody(ingest, "retained"), 200)
			originals, err := s.deliveryBatch(ctx)
			if err != nil || len(originals) != 1 {
				t.Fatal(err)
			}
			if _, err := s.quarantineOriginal(ctx, originals[0], 321); err != nil {
				t.Fatal(err)
			}
			if operation == "edit" {
				_, err = s.editEvent(ctx, mutationEdit(projectID, "retained", "correct", "0"))
			} else {
				_, err = s.deleteEvent(ctx, &EventDeleteInput{ID: projectID, UUID: "retained", Body: EventDeleteBody{IdempotencyKey: "remove", ExpectedRevision: "0"}})
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.deliverMutations(ctx); err != nil {
				t.Fatal(err)
			}
			if quarantineCount(t, s) != 0 || eventCount(t, s) != 0 || receiptCount(t, s) != 1 {
				t.Fatal("mutation ACK lost atomic receipt/quarantine cleanup")
			}
			if err := s.lake.Apply(ctx, duckLakeJourneyProjections(t, originals)); err != nil {
				t.Fatal(err)
			}
			row, err := s.lake.Load(ctx, projectID, "retained")
			if err != nil || row.ID != originals[0].ID || row.Revision == 0 {
				t.Fatal("late replay changed accepted head", err)
			}
			if operation == "edit" && row.Event.Event != "corrected" || operation == "delete" && !row.IsDeleted {
				t.Fatal("late original replaced its correction/deletion", row)
			}
		})
	}
}
