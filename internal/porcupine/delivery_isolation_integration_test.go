package porcupine

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestClickHouseIntegrationDeliveryQuarantine(t *testing.T) {
	config := integrationClickHouseConfig(t)
	ch, err := newClickHouse(config)
	if err != nil {
		t.Fatal(err)
	}
	s := service(t, 100)
	configureAnalyticsTest(s, ch)
	ctx := context.Background()
	if err = ch.initialize(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = ch.admin(ctx, "ALTER TABLE "+config.Database+"."+eventTable+" ADD CONSTRAINT reject_synthetic_poison CHECK event != 'poison'"); err != nil {
		t.Fatal(err)
	}
	a, keyA := project(t, s)
	b, keyB := project(t, s)
	request(t, s, "POST", "/capture/", "", poisonBody(keyA, "poison-original"), 200)
	request(t, s, "POST", "/capture/", "", captureBody(keyB, "healthy-original"), 200)
	originals, err := s.deliveryBatch(ctx)
	if err != nil || len(originals) != 2 {
		t.Fatal("missing queued originals", err)
	}
	if rejection := ch.insert(ctx, originals[:1]); rejection == nil {
		t.Fatal("synthetic constraint did not reject row")
	} else if code, specific := deliveryRowError(rejection); !specific || code != 469 {
		t.Fatalf("real constraint returned an unclassified error: %v", rejection)
	}
	if err = s.flushDelivery(ctx); err != nil {
		t.Fatal(err)
	}
	statusA, _ := s.deliveryStatus(ctx, a)
	statusB, _ := s.deliveryStatus(ctx, b)
	if statusA.Quarantined != 1 || statusA.Pending != 1 || statusB.Pending != 0 || statusB.Quarantined != 0 {
		t.Fatal("real row constraint blocked healthy project or lost accepted row")
	}
	query, err := s.analyticsQuery(ctx, queryInput(b, "SELECT count() AS total FROM events"))
	if err != nil || len(query.Body.Result.Data) != 1 || string(query.Body.Result.Data[0]["total"]) != `"1"` {
		t.Fatal("healthy project not queryable", err)
	}
	if _, err = s.editEvent(mutationContext("operator"), mutationEdit(a, "poison-original", "repair", "0")); err != nil {
		t.Fatal(err)
	}
	if err = s.flushDelivery(ctx); err != nil {
		t.Fatal(err)
	}
	if eventCount(t, s) != 0 || quarantineCount(t, s) != 0 || receiptCount(t, s) != 2 {
		t.Fatal("real mutation ACK did not resolve quarantine")
	}
	if _, err = ch.admin(ctx, "ALTER TABLE "+config.Database+"."+eventTable+" DROP CONSTRAINT reject_synthetic_poison"); err != nil {
		t.Fatal(err)
	}
	if err = ch.insert(ctx, originals[:1]); err != nil {
		t.Fatal(err)
	}
	result, err := s.analyticsQuery(ctx, queryInput(a, "SELECT event,revision FROM events"))
	if err != nil || len(result.Body.Result.Data) != 1 || string(result.Body.Result.Data[0]["event"]) != `"corrected"` || string(result.Body.Result.Data[0]["revision"]) == `"0"` {
		t.Fatal("late real original resurrected stale payload", err)
	}
	deleted, err := s.deleteEvent(mutationContext("operator"), &EventDeleteInput{ID: a, UUID: "poison-original", Body: EventDeleteBody{ExpectedRevision: strings.Trim(string(result.Body.Result.Data[0]["revision"]), `"`), IdempotencyKey: "delete-after-repair"}})
	if err != nil {
		t.Fatal(err)
	}
	if deleted.Body.State != "pending" {
		t.Fatal("delete was not durable")
	}
	if err = s.flushDelivery(ctx); err != nil {
		t.Fatal(err)
	}
	if err = ch.insert(ctx, originals[:1]); err != nil {
		t.Fatal(err)
	}
	result, err = s.analyticsQuery(ctx, queryInput(a, "SELECT count() AS total FROM events"))
	if err != nil {
		t.Fatal(err)
	}
	var count string
	if err = json.Unmarshal(result.Body.Result.Data[0]["total"], &count); err != nil || count != "0" {
		t.Fatal("late real original resurrected deleted event", err)
	}
}
