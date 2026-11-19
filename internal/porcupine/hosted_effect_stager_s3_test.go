package porcupine

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/olup/porcupine/internal/delivery"
	eventapi "github.com/olup/porcupine/internal/event"
	lakeapi "github.com/olup/porcupine/internal/lake"
)

func TestDuckLakeS3HostedImmutableEffects(t *testing.T) {
	t.Parallel()
	fixture := nativeS3Fixture(t)
	runtime, err := lakeapi.NewDuckLake(lakeapi.DuckLakeConfig{Directory: filepath.Join(t.TempDir(), "lake"), S3: &fixture.storage, DisableCleanup: true})
	if err != nil {
		t.Fatal(err)
	}
	store := runtime.(*lakeapi.DuckLakeStore)
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	ctx := context.Background()
	source, err := store.SourceID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	key := lakeapi.ProjectWriteKey{SourceID: source, ProjectID: "project"}
	fence := lakeapi.ProjectWriteFence{Key: key, StreamID: eventapi.NewID(), Epoch: 1, AcquisitionID: eventapi.NewID()}
	scope := lakeapi.HostedProjectScope{Key: key, CatalogID: lakeapi.SharedNativeCatalogID(source, key.ProjectID), StreamID: fence.StreamID}
	ordinary := eventapi.Event{ID: 11, ProjectID: key.ProjectID, UUID: eventapi.NewID(), Event: "page_viewed", DistinctID: "person", Properties: json.RawMessage(`{"page":"/"}`), Context: json.RawMessage(`{}`), Timestamp: "2026-11-17T12:00:00Z", ReceivedAt: "2026-11-17T12:00:01Z", SourceID: "native", MessageID: "message", EventType: "track"}
	second := ordinary
	second.ID = 12
	second.UUID = eventapi.NewID()
	excluded := ordinary
	excluded.Event = "private_event"
	selected := lakeapi.SelectedLiveEffects{Scope: scope, TenantID: "tenant", BatchID: "batch", Sequence: 1, Events: []eventapi.Event{ordinary, excluded, second}}
	for _, mode := range []string{"event", "batch"} {
		t.Run(mode, func(t *testing.T) {
			target := frozenWebhookTarget{PipelineID: "pipeline", PipelineIncarnation: "incarnation", RuleRevision: "rule-v1", DestinationRevision: "destination-v1", Rule: delivery.WebhookRule{DeliveryMode: mode, IncludeEvents: []string{"page_viewed"}}}
			if mode == "batch" {
				target.Rule.IntervalSeconds = 60
			}
			plan := frozenLiveOutputPlan{Scope: scope, TenantID: selected.TenantID, Targets: []frozenWebhookTarget{target}, MaxEventReferences: 10}
			stager, err := newHostedImmutableEffects(store.Remote, fence, plan)
			if err != nil {
				t.Fatal(err)
			}
			prepared, err := stager.prepare(ctx, selected)
			expected := 2
			if mode == "batch" {
				expected = 1
			}
			if err != nil || len(prepared) != expected {
				t.Fatal(prepared, err)
			}
			replay, err := stager.prepare(ctx, selected)
			if err != nil || !reflect.DeepEqual(replay, prepared) {
				t.Fatal("immutable restaging changed references", replay, prepared, err)
			}
			members := []string{}
			for _, o := range prepared {
				if o.Incarnation != "incarnation" || o.DestinationRevision != "destination-v1" {
					t.Fatal("routing revision changed", o)
				}
				for _, m := range o.Members {
					members = append(members, m.UUID)
				}
				body, err := store.Remote.ReadImmutableEffectObject(ctx, scope, nativeRef(o.Body))
				if err != nil {
					t.Fatal(err)
				}
				if mode == "event" {
					var payload delivery.WebhookPayload
					if err := json.Unmarshal(body, &payload); err != nil {
						t.Fatal(err)
					}
					if o.Fragment != nil || payload.DeliveryID != o.ID || payload.PipelineID != "pipeline" || payload.ProjectID != key.ProjectID || payload.Event == nil || len(payload.Events) != 0 {
						t.Fatal("event contract changed", payload)
					}
					continue
				}
				var events []eventapi.Event
				if err := json.Unmarshal(body, &events); err != nil {
					t.Fatal(err)
				}
				if o.Fragment == nil || len(events) != 2 || len(o.Members) != 2 || o.Fragment.WindowEndMS-o.Fragment.WindowStartMS != 60000 {
					t.Fatal(o, events)
				}
				for i, e := range events {
					encoded, err := delivery.EncodeJSON(e)
					if err != nil || int64(len(encoded)) != o.Members[i].Bytes {
						t.Fatal("fragment byte metadata diverged", err)
					}
				}
			}
			if len(members) != 2 || members[0] != ordinary.UUID || members[1] != second.UUID {
				t.Fatal("output membership changed", members)
			}
			wrong := scope
			wrong.Key.ProjectID = "another-project"
			wrong.CatalogID = lakeapi.SharedNativeCatalogID(source, wrong.Key.ProjectID)
			if _, err := store.Remote.ReadImmutableEffectObject(ctx, wrong, nativeRef(prepared[0].Body)); err == nil {
				t.Fatal("cross-project body access accepted")
			}
			private := selected
			private.Events = append([]eventapi.Event(nil), selected.Events...)
			private.Events[0].IdentitySnapshot = `{"private":true}`
			if _, err := stager.prepare(ctx, private); err == nil {
				t.Fatal("private identity snapshot reached staging")
			}
			bounded := plan
			bounded.MaxEventReferences = 1
			limited, err := newHostedImmutableEffects(store.Remote, fence, bounded)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := limited.prepare(ctx, selected); !errors.Is(err, delivery.ErrWebhookCapacity) {
				t.Fatal("fanout capacity was not enforced atomically", err)
			}
		})
	}
}
