package porcupine

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/olup/porcupine/internal/delivery"
	eventapi "github.com/olup/porcupine/internal/event"
	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/output"
)

func TestDuckLakeS3HostedImmutableEffects(t *testing.T) {
	t.Parallel()
	fixture := nativeS3Fixture(t)
	store, err := lakeapi.NewDuckLake(lakeapi.DuckLakeConfig{Directory: filepath.Join(t.TempDir(), "lake"), S3: &fixture.storage, DisableCleanup: true})
	if err != nil {
		t.Fatal(err)
	}
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
			target := output.FrozenWebhookTarget{PipelineID: "pipeline", PipelineIncarnation: "incarnation", RuleRevision: "rule-v1", DestinationRevision: "destination-v1", Rule: delivery.WebhookRule{DeliveryMode: mode, IncludeEvents: []string{"page_viewed"}}}
			if mode == "batch" {
				target.Rule.IntervalSeconds = 60
			}
			plan := output.HostedWebhookPlan{Scope: scope, TenantID: selected.TenantID, Targets: []output.FrozenWebhookTarget{target}, MaxEventReferences: 10}
			stager, err := output.NewHostedWebhookPreparation(store.Remote, fence, plan)
			if err != nil {
				t.Fatal(err)
			}
			prepared, objects, err := stager.Prepare(selected)
			expected := 2
			if mode == "batch" {
				expected = 1
			}
			if err != nil || len(prepared) != expected || len(objects) != expected {
				t.Fatal(prepared, err)
			}
			replay, replayObjects, err := stager.Prepare(selected)
			if err != nil || !reflect.DeepEqual(replay, prepared) || !reflect.DeepEqual(replayObjects, objects) {
				t.Fatal("immutable restaging changed references", replay, prepared, err)
			}
			list, err := fixture.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: &fixture.storage.Bucket, Prefix: &objects[0].Ref.Key})
			if err != nil {
				t.Fatal(err)
			}
			if len(list.Contents) != 0 {
				t.Fatal("preparation wrote an object before upload")
			}
			members := []string{}
			for i, o := range prepared {
				if o.Body != pipelineRef(objects[i].Ref) {
					t.Fatal("prepared metadata differs from the body descriptor", o.Body, objects[i].Ref)
				}
				ref := uploadPreparedWebhookBody(t, store.Remote, scope, fence, objects[i])
				replayed := uploadPreparedWebhookBody(t, store.Remote, scope, fence, replayObjects[i])
				if ref != replayed {
					t.Fatal("retry changed the stored object reference", ref, replayed)
				}
				prepared[i].Body = pipelineRef(ref)
				if o.Incarnation != "incarnation" || o.DestinationRevision != "destination-v1" {
					t.Fatal("routing revision changed", o)
				}
				var queued int64
				for _, m := range o.Members {
					members = append(members, m.UUID)
					queued += m.Bytes + 1024
				}
				charge := output.PipelineDemand{References: int64(len(o.Members)), QueuedBytes: ref.Bytes, MetadataRows: int64(1 + len(o.Members)), ArtifactBytes: ref.Bytes}
				if mode == "batch" {
					charge.QueuedBytes = max(queued, ref.Bytes)
				}
				if o.Charge != charge {
					t.Fatal("webhook charge differs from the uploaded body and member metadata", o.Charge, charge)
				}
				body, err := store.Remote.ReadImmutableEffectObject(ctx, scope, ref)
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
			if _, _, err := stager.Prepare(private); err == nil {
				t.Fatal("private identity snapshot reached staging")
			}
			bounded := plan
			bounded.MaxEventReferences = 1
			limited, err := output.NewHostedWebhookPreparation(store.Remote, fence, bounded)
			if err != nil {
				t.Fatal(err)
			}
			if outputs, objects, err := limited.Prepare(selected); !errors.Is(err, delivery.ErrWebhookCapacity) || outputs != nil || objects != nil {
				t.Fatal("fanout capacity was not enforced atomically", err)
			}
		})
	}
}

func uploadPreparedWebhookBody(t *testing.T, objects *lakeapi.DuckLakeObjectStore, scope lakeapi.HostedProjectScope, fence lakeapi.ProjectWriteFence, o output.PreparedEffectObject) lakeapi.ImmutableObjectRef {
	t.Helper()
	ref, err := objects.PutImmutableEffectObject(t.Context(), scope, fence.AcquisitionID, o.Body)
	if err != nil {
		t.Fatal(err)
	}
	descriptor := ref
	descriptor.VersionID = ""
	if descriptor != o.Ref {
		t.Fatal("uploaded body differs from its descriptor", ref, o.Ref)
	}
	return ref
}
