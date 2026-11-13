package porcupine

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestClickHouseIntegrationCatalogPropertyFiltersAndCursor(t *testing.T) {
	s := service(t, 20)
	ch, err := newClickHouse(integrationClickHouseConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	configureAnalyticsTest(s, ch)
	a, _ := project(t, s)
	b, _ := project(t, s)
	if err = ch.initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	props := map[string]any{"threadId": "private value", "threadTurnId": "another value", "": "empty key value"}
	for i := 0; i < 220; i++ {
		props[fmt.Sprintf("$sdk_%03d", i)] = "not a schema value"
	}
	raw, _ := json.Marshal(props)
	rows := []Event{{ID: 1, ProjectID: a, UUID: "catalog-a", Event: "page", DistinctID: "actor", Properties: raw, Timestamp: "2026-11-01T00:00:00Z", ReceivedAt: "2026-11-01T00:00:00Z"}, {ID: 1, ProjectID: b, UUID: "catalog-b", Event: "page", DistinctID: "actor", Properties: json.RawMessage(`{"foreign":"private foreign value"}`), Timestamp: "2026-11-01T00:00:00Z", ReceivedAt: "2026-11-01T00:00:00Z"}}
	if err = ch.insert(context.Background(), rows); err != nil {
		t.Fatal(err)
	}
	input := EventCatalogInput{ID: a, Event: "page", PropertyPrefix: "thread", PropertyKind: "custom", PropertyLimit: 1}
	first, err := s.eventCatalog(context.Background(), &input)
	if err != nil || len(first.Body.Properties) != 1 || first.Body.Properties[0].Key != "threadId" || first.Body.NextPropertyCursor == "" {
		t.Fatalf("filtered page: %v %v", first, err)
	}
	input.PropertyCursor = first.Body.NextPropertyCursor
	second, err := s.eventCatalog(context.Background(), &input)
	if err != nil || len(second.Body.Properties) != 1 || second.Body.Properties[0].Key != "threadTurnId" || second.Body.NextPropertyCursor != "" {
		t.Fatalf("next page: %v %v", second, err)
	}
	encoded, _ := json.Marshal(second)
	if strings.Contains(string(encoded), "private") || strings.Contains(string(encoded), "foreign") {
		t.Fatal("catalog exposed values or another project")
	}
	input.ID = b
	if _, err = s.eventCatalog(context.Background(), &input); queryStatus(err) != 400 {
		t.Fatal("cursor accepted in another project")
	}
	input = EventCatalogInput{ID: a, Event: "page", PropertyKind: "custom", PropertyPrefix: "thread' OR 1=1 --"}
	empty, err := s.eventCatalog(context.Background(), &input)
	if err != nil || len(empty.Body.Properties) != 0 {
		t.Fatalf("prefix not treated as literal: %v %v", empty, err)
	}
}
