package porcupine

import (
	"context"
	"io"
	"testing"

	"github.com/olup/porcupine/internal/acceptance"
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
	t.Parallel()
	s, _ := nativeCoreService(t)
	a, ingestA := project(t, s)
	b, ingestB := project(t, s)
	request(t, s, "POST", "/capture/", "", captureBody(ingestA, "a"), 200)
	if err := s.flushDelivery(context.Background()); err != nil {
		t.Fatal(err)
	}
	aStatus, err := s.deliveryStatus(context.Background(), a)
	if err != nil || aStatus.LastSuccess == "" || aStatus.OldestPendingReceivedAt != "" {
		t.Fatal("project A delivery was not recorded", err)
	}
	s.recordDeliveryResult(context.Background(), io.EOF)
	bStatus, err := s.deliveryStatus(context.Background(), b)
	if err != nil || bStatus.LastSuccess != "" || bStatus.Error != "" {
		t.Fatal("project B observed another project's delivery", err)
	}
	request(t, s, "POST", "/capture/", "", captureBody(ingestB, "b"), 200)
	bStatus, err = s.deliveryStatus(context.Background(), b)
	if err != nil || bStatus.LastSuccess != "" || bStatus.OldestPendingReceivedAt == "" || bStatus.Error != "DuckLake delivery failed" {
		t.Fatal("pending project did not receive generic health", err)
	}
}
func TestDeliveryLostAcknowledgmentRetryCleanupAndRestart(t *testing.T) {
	t.Parallel()
	s, config := nativeCoreService(t)
	projectID, ingest := project(t, s)
	request(t, s, "POST", "/capture/", "", captureBody(ingest, "original"), 200)
	var id int64
	if err := s.db.QueryRow(`SELECT id FROM events WHERE uuid='original'`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`CREATE TRIGGER lose_delivery_ack BEFORE DELETE ON events BEGIN SELECT RAISE(FAIL,'simulated local acknowledgment loss'); END`); err != nil {
		t.Fatal(err)
	}
	if err := s.flushDelivery(context.Background()); err == nil {
		t.Fatal("lost acknowledgment was reported successful")
	}
	if eventCount(t, s) != 1 || receiptCount(t, s) != 0 {
		t.Fatal("lost acknowledgment partially committed cleanup")
	}
	projection, err := s.lake.Load(context.Background(), projectID, "original")
	if err != nil || projection.ID != id {
		t.Fatal("native transaction did not commit before local acknowledgment", err)
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
	request(t, s, "POST", "/capture/", "", captureBody(ingest, "original"), 200)
	if eventCount(t, s) != 1 {
		t.Fatal("uncertain-commit retry duplicated the accepted original")
	}
	if _, err := s.db.Exec(`DROP TRIGGER lose_delivery_ack`); err != nil {
		t.Fatal(err)
	}
	if err := s.flushDelivery(context.Background()); err != nil {
		t.Fatal(err)
	}
	if eventCount(t, s) != 0 || receiptCount(t, s) != 1 {
		t.Fatal("native retry did not replace payload with permanent receipt")
	}
	stored, err := acceptance.LocalReceiptEventID(context.Background(), s.db, projectID, "original")
	if err != nil || stored != id {
		t.Fatal("receipt changed acceptance ID", stored, err)
	}
	projection, err = s.lake.Load(context.Background(), projectID, "original")
	if err != nil || projection.ID != id {
		t.Fatal("analytical replay changed stable ID", err)
	}
}
func TestDeliveryBoundedBatchesAndProjectStatus(t *testing.T) {
	t.Parallel()
	s, _ := nativeCoreService(t)
	s.deliveryMaxEvents = 2
	a, ingestA := project(t, s)
	b, ingestB := project(t, s)
	for _, row := range []struct{ token, id string }{{ingestA, "a1"}, {ingestB, "b1"}, {ingestA, "a2"}, {ingestB, "b2"}, {ingestA, "a3"}} {
		request(t, s, "POST", "/capture/", "", captureBody(row.token, row.id), 200)
	}
	if err := s.flushDelivery(context.Background()); err != nil {
		t.Fatal(err)
	}
	if eventCount(t, s) != 3 || receiptCount(t, s) != 2 {
		t.Fatal("delivery selection ignored its row limit")
	}
	for range 2 {
		if err := s.flushDelivery(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if eventCount(t, s) != 0 || receiptCount(t, s) != 5 {
		t.Fatal("bounded passes lost or duplicated originals")
	}
	for _, id := range []string{a, b} {
		status, err := s.deliveryStatus(context.Background(), id)
		if err != nil || status.Pending != 0 || status.LastSuccess == "" {
			t.Fatal("project-specific delivery status missing", status, err)
		}
	}
}
