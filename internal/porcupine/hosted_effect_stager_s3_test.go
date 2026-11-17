package porcupine

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
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
			staged, err := stager.StageLiveEffects(ctx, selected)
			expected := int64(2)
			if mode == "batch" {
				expected = 1
			}
			if err != nil || staged.OutputCount != expected {
				t.Fatal(staged, err)
			}
			replay, err := stager.StageLiveEffects(ctx, selected)
			if err != nil || replay != staged {
				t.Fatal("immutable restaging changed reference", replay, staged, err)
			}
			raw, err := store.Remote.ReadImmutableEffectObject(ctx, scope, staged.Manifest)
			if err != nil {
				t.Fatal(err)
			}
			var manifest immutableLiveEffectManifest
			if err := json.Unmarshal(raw, &manifest); err != nil {
				t.Fatal(err)
			}
			if manifest.PlanID != staged.PlanID || manifest.Sequence != selected.Sequence || manifest.Scope != scope {
				t.Fatal(manifest)
			}
			members := []string{}
			for _, output := range manifest.Outputs {
				if output.Target.PipelineIncarnation != "incarnation" || output.Target.DestinationRevision != "destination-v1" {
					t.Fatal("routing revision changed", output)
				}
				members = append(members, output.EventUUIDs...)
				body, err := store.Remote.ReadImmutableEffectObject(ctx, scope, output.Body)
				if err != nil {
					t.Fatal(err)
				}
				var payload delivery.WebhookPayload
				if err := json.Unmarshal(body, &payload); err != nil {
					t.Fatal(err)
				}
				if payload.DeliveryID != output.DeliveryID || payload.PipelineID != "pipeline" || payload.ProjectID != key.ProjectID {
					t.Fatal(payload)
				}
				if mode == "event" && (payload.Event == nil || len(payload.Events) != 0) {
					t.Fatal("event contract changed", payload)
				}
			}
			if mode == "batch" && (len(manifest.Outputs) != 0 || len(manifest.BatchFragments) != 1) {
				t.Fatal("batch fragment was prematurely made a delivery", manifest)
			}
			for _, fragment := range manifest.BatchFragments {
				members = append(members, fragment.EventUUIDs...)
				body, err := store.Remote.ReadImmutableEffectObject(ctx, scope, fragment.Body)
				if err != nil {
					t.Fatal(err)
				}
				var events []eventapi.Event
				if err := json.Unmarshal(body, &events); err != nil {
					t.Fatal(err)
				}
				if len(events) != 2 || len(fragment.EventBytes) != 2 || fragment.WindowEndMS-fragment.WindowStartMS != 60000 {
					t.Fatal(fragment, events)
				}
				for i, e := range events {
					encoded, err := delivery.EncodeJSON(e)
					if err != nil || int64(len(encoded)) != fragment.EventBytes[i] {
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
			if _, err := store.Remote.ReadImmutableEffectObject(ctx, wrong, staged.Manifest); err == nil {
				t.Fatal("cross-project body access accepted")
			}
			private := selected
			private.Events = append([]eventapi.Event(nil), selected.Events...)
			private.Events[0].IdentitySnapshot = `{"private":true}`
			if _, err := stager.StageLiveEffects(ctx, private); err == nil {
				t.Fatal("private identity snapshot reached staging")
			}
			bounded := plan
			bounded.MaxEventReferences = 1
			limited, err := newHostedImmutableEffects(store.Remote, fence, bounded)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := limited.StageLiveEffects(ctx, selected); !errors.Is(err, delivery.ErrWebhookCapacity) {
				t.Fatal("fanout capacity was not enforced atomically", err)
			}
		})
	}
}
