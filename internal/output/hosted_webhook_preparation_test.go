package output

import (
	"errors"
	"maps"
	"testing"

	"github.com/olup/porcupine/internal/delivery"
	eventapi "github.com/olup/porcupine/internal/event"
	lakeapi "github.com/olup/porcupine/internal/lake"
)

func TestHostedOutputPlanUsesWebhookContract(t *testing.T) {
	t.Parallel()
	key := lakeapi.ProjectWriteKey{SourceID: eventapi.NewID(), ProjectID: "project"}
	fence := lakeapi.ProjectWriteFence{Key: key, StreamID: eventapi.NewID(), Epoch: 1, AcquisitionID: eventapi.NewID()}
	scope := lakeapi.HostedProjectScope{Key: key, CatalogID: lakeapi.SharedNativeCatalogID(key.SourceID, key.ProjectID), StreamID: fence.StreamID}
	for _, mode := range []string{"", "event", "batch"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			spec := WebhookPipelineSpec{DeliveryMode: mode, IncludeEvents: []string{"page_viewed", "excluded"}, ExcludeEvents: []string{"excluded"}}
			if mode == "batch" {
				spec.IntervalSeconds = 60
			}
			target := FrozenWebhookTarget{PipelineID: "pipeline", PipelineIncarnation: "incarnation", RuleRevision: "rule-v1", DestinationRevision: "destination-v1", Rule: delivery.WebhookRule{DeliveryMode: spec.DeliveryMode, IntervalSeconds: spec.IntervalSeconds, IncludeEvents: spec.IncludeEvents, ExcludeEvents: spec.ExcludeEvents}}
			plan := HostedWebhookPlan{Scope: scope, TenantID: "tenant", Targets: []FrozenWebhookTarget{target}, MaxEventReferences: 10}
			stager, err := NewHostedWebhookPreparation(&lakeapi.DuckLakeObjectStore{}, fence, plan)
			if err != nil {
				t.Fatal("public webhook contract was rejected", mode, err)
			}
			spec.IncludeEvents[0] = "mutated"
			spec.ExcludeEvents[0] = "mutated"
			counts, err := stager.CountReferences([]eventapi.Event{
				{Event: "page_viewed", ReceivedAt: "2026-11-17T12:00:01Z"},
				{Event: "mutated"},
				{Event: "excluded"},
			})
			if err != nil || counts["pipeline"] != 1 {
				t.Fatal("frozen plan retained mutable caller memory", counts, err)
			}
		})
	}
	invalid := HostedWebhookPlan{Scope: scope, TenantID: "tenant", Targets: []FrozenWebhookTarget{{PipelineID: "pipeline", PipelineIncarnation: "incarnation", RuleRevision: "rule", DestinationRevision: "destination", Rule: delivery.WebhookRule{DeliveryMode: "single"}}}, MaxEventReferences: 10}
	if _, err := NewHostedWebhookPreparation(&lakeapi.DuckLakeObjectStore{}, fence, invalid); err == nil {
		t.Fatal("non-contract delivery mode accepted")
	}
}

func TestHostedWebhookPreparationCountsAllRefusedReferences(t *testing.T) {
	t.Parallel()
	key := lakeapi.ProjectWriteKey{SourceID: eventapi.NewID(), ProjectID: "project"}
	fence := lakeapi.ProjectWriteFence{Key: key, StreamID: eventapi.NewID(), AcquisitionID: eventapi.NewID()}
	scope := lakeapi.HostedProjectScope{Key: key, CatalogID: lakeapi.SharedNativeCatalogID(key.SourceID, key.ProjectID), StreamID: fence.StreamID}
	first := FrozenWebhookTarget{
		PipelineID: "first", PipelineIncarnation: "first-v1", RuleRevision: "rule", DestinationRevision: "destination",
		Rule: delivery.WebhookRule{DeliveryMode: "batch", IntervalSeconds: 60, StartAt: "2026-11-17T12:00:00Z", IncludeEvents: []string{"page_viewed"}},
	}
	second := FrozenWebhookTarget{
		PipelineID: "second", PipelineIncarnation: "second-v1", RuleRevision: "rule", DestinationRevision: "destination",
		Rule: delivery.WebhookRule{ExcludeEvents: []string{"ignored"}},
	}
	plan := HostedWebhookPlan{Scope: scope, TenantID: "tenant", Targets: []FrozenWebhookTarget{first, second}, MaxEventReferences: 1}
	preparation, err := NewHostedWebhookPreparation(&lakeapi.DuckLakeObjectStore{}, fence, plan)
	if err != nil {
		t.Fatal(err)
	}
	selected := lakeapi.SelectedLiveEffects{Scope: scope, TenantID: "tenant", BatchID: "batch", Sequence: 1, Events: []eventapi.Event{
		{ProjectID: key.ProjectID, Event: "page_viewed", ReceivedAt: "2026-11-17T11:59:59Z"},
		{ProjectID: key.ProjectID, Event: "page_viewed", ReceivedAt: "2026-11-17T12:00:00Z"},
		{ProjectID: key.ProjectID, Event: "page_viewed", ReceivedAt: "2026-11-17T12:00:01Z"},
		{ProjectID: key.ProjectID, Event: "ignored"},
	}}
	outputs, objects, err := preparation.Prepare(selected)
	if !errors.Is(err, delivery.ErrWebhookCapacity) || outputs != nil || objects != nil {
		t.Fatal("preparation returned partial outputs at the reference limit", len(outputs), len(objects), err)
	}
	counts, err := preparation.CountReferences(selected.Events)
	if err != nil || !maps.Equal(counts, map[string]uint64{"first": 2, "second": 3}) {
		t.Fatal("refusal did not count all eligible references", counts, err)
	}
}
