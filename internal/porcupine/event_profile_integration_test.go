package porcupine

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestClickHouseIntegrationEventProfile(t *testing.T) {
	config := integrationClickHouseConfig(t)
	s := service(t, 20)
	ch, err := newClickHouse(config)
	if err != nil {
		t.Fatal(err)
	}
	configureAnalyticsTest(s, ch)
	ctx := context.Background()
	a, _ := project(t, s)
	b, _ := project(t, s)
	_, reader := key(t, s, []string{"events:read"}, []string{a})
	asOf := time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC)
	s.clock = func() time.Time { return asOf }
	if err = ch.initialize(ctx); err != nil {
		t.Fatal(err)
	}
	rows := []Event{}
	for i, item := range []struct {
		at    time.Time
		props string
	}{
		{asOf.Add(-30 * 24 * time.Hour), `{"language":"fr","amount":9007199254740993}`},
		{asOf.Add(-time.Hour), `{"language":null}`},
		{asOf.Add(time.Hour), `{}`},
	} {
		rows = append(rows, Event{ID: int64(i + 1), ProjectID: a, UUID: fmt.Sprintf("profile-%d", i), Event: "profile' bound", DistinctID: "actor", Properties: json.RawMessage(item.props), Timestamp: item.at.Format(time.RFC3339Nano), ReceivedAt: asOf.Format(time.RFC3339Nano)})
	}
	rows = append(rows, Event{ID: 1, ProjectID: b, UUID: "foreign", Event: "profile' bound", DistinctID: "foreign", Properties: json.RawMessage(`{"foreign":"foreign-secret"}`), Timestamp: asOf.Format(time.RFC3339Nano), ReceivedAt: asOf.Format(time.RFC3339Nano)})
	if err = ch.insert(ctx, rows); err != nil {
		t.Fatal(err)
	}
	profile, err := s.eventProfile(ctx, &EventProfileInput{ID: a, Event: "profile' bound", IncludeValues: true, ExampleLimit: 3})
	if err != nil {
		t.Fatal(err)
	}
	p := profile.Body
	if p.Activity.Total != "3" || p.Activity.Last24Hours != "1" || p.Activity.Last7Days != "1" || p.Activity.FutureEvents != "1" || p.Sampling.ProfiledEvents != 3 || len(p.Examples) != 3 || p.Examples[0].UUID != "profile-2" {
		t.Fatalf("incorrect restricted-reader profile: %+v", p)
	}
	var language *EventProfileProperty
	for i := range p.Properties {
		if p.Properties[i].Key == "language" {
			language = &p.Properties[i]
		}
	}
	if language == nil || language.Present != 2 || language.Missing != 1 || language.Null != 1 || language.DistinctValues != 1 {
		t.Fatalf("incorrect property denominator: %+v", language)
	}
	encoded, _ := json.Marshal(p)
	if strings.Contains(string(encoded), "foreign-secret") || !strings.Contains(string(encoded), "9007199254740993") {
		t.Fatal("profile lost numeric precision or project isolation")
	}
	request(t, s, "GET", "/api/v1/projects/"+b+"/event-profile?event=profile", reader, "", 403)
	empty, err := s.eventProfile(ctx, &EventProfileInput{ID: a, Event: "missing"})
	if err != nil || empty.Body.Activity.Total != "0" || empty.Body.Activity.FirstOccurredAt != nil || len(empty.Body.Examples) != 0 {
		t.Fatalf("empty profile: %v %v", empty, err)
	}

	// The SQL transfer budget must apply before the restricted reader's 1 MiB result limit.
	large := []Event{}
	for i := 0; i < 1000; i++ {
		props, _ := json.Marshal(map[string]any{"note": strings.Repeat("x", 15000), "amount": i})
		large = append(large, Event{ID: int64(i + 10), ProjectID: a, UUID: fmt.Sprintf("bulk-profile-%d", i), Event: "bulk-profile", DistinctID: "actor", Properties: props, Timestamp: asOf.Add(-time.Minute).Format(time.RFC3339Nano), ReceivedAt: asOf.Format(time.RFC3339Nano)})
	}
	if err = ch.insert(ctx, large); err != nil {
		t.Fatal(err)
	}
	bulk, err := s.eventProfile(ctx, &EventProfileInput{ID: a, Event: "bulk-profile"})
	if err != nil {
		t.Fatal(err)
	}
	if bulk.Body.Activity.Total != "1000" || bulk.Body.Sampling.SampledEvents != 1000 || bulk.Body.Sampling.ByteBudgetExcludedEvents == 0 || bulk.Body.Sampling.ProfiledEvents >= 1000 {
		t.Fatalf("missing transfer budget metadata: %+v", bulk.Body.Sampling)
	}
	encoded, _ = json.Marshal(bulk.Body)
	if len(encoded) > analyticsMaxBytes {
		t.Fatal("profile exceeded the response byte budget")
	}
}
