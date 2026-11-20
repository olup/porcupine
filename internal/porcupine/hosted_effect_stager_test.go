package porcupine

import (
	"testing"

	"github.com/olup/porcupine/internal/output"

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
			spec := output.WebhookPipelineSpec{DeliveryMode: mode, IncludeEvents: []string{"page_viewed"}}
			if mode == "batch" {
				spec.IntervalSeconds = 60
			}
			target := frozenWebhookTarget{FrozenWebhookTarget: output.FrozenWebhookTarget{PipelineID: "pipeline", PipelineIncarnation: "incarnation", RuleRevision: "rule-v1", DestinationRevision: "destination-v1", Rule: delivery.WebhookRule{DeliveryMode: spec.DeliveryMode, IntervalSeconds: spec.IntervalSeconds, IncludeEvents: spec.IncludeEvents}}}
			plan := frozenLiveOutputPlan{Scope: scope, TenantID: "tenant", Targets: []frozenWebhookTarget{target}, MaxEventReferences: 10}
			stager, err := newHostedImmutableEffects(&lakeapi.DuckLakeObjectStore{}, fence, plan)
			if err != nil {
				t.Fatal("public webhook contract was rejected", mode, err)
			}
			plan.Targets[0].Rule.IncludeEvents[0] = "mutated"
			if stager.plan.Targets[0].Rule.IncludeEvents[0] != "page_viewed" {
				t.Fatal("frozen plan retained mutable caller memory")
			}
		})
	}
	invalid := frozenLiveOutputPlan{Scope: scope, TenantID: "tenant", Targets: []frozenWebhookTarget{{FrozenWebhookTarget: output.FrozenWebhookTarget{PipelineID: "pipeline", PipelineIncarnation: "incarnation", RuleRevision: "rule", DestinationRevision: "destination", Rule: delivery.WebhookRule{DeliveryMode: "single"}}}}, MaxEventReferences: 10}
	if _, err := newHostedImmutableEffects(&lakeapi.DuckLakeObjectStore{}, fence, invalid); err == nil {
		t.Fatal("non-contract delivery mode accepted")
	}
}
