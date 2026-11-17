package porcupine

import (
	"reflect"
	"testing"

	"github.com/olup/porcupine/internal/delivery"
	eventapi "github.com/olup/porcupine/internal/event"
	"github.com/olup/porcupine/internal/output"
)

func TestDuckLakeS3HostedWebhookRecomposition(t *testing.T) {
	t.Parallel()
	fixture := nativeS3Fixture(t)
	h := newAssemblyS3Harness(t, fixture.storage)
	events := assemblyEvents(3)
	claim := h.claim(t, h.stage(t, events))
	original, err := assembleHostedWebhook(t.Context(), h.objects, claim)
	if err != nil {
		t.Fatal(err)
	}
	previous := output.PipelineFragment{
		Claim:  output.PipelineClaim{ID: original.DeliveryID},
		Target: output.PipelineTarget{ID: h.target.PipelineID},
		Body:   pipelineRef(original.Body),
		Spec:   output.PipelineFragmentSpec{WindowStartMS: claim.WindowStartMS, WindowEndMS: claim.WindowEndMS, Rule: h.target.Rule},
		Count:  3, Recompose: true, Frozen: true,
	}
	for _, event := range events {
		raw, err := delivery.EncodeJSON(event)
		if err != nil {
			t.Fatal(err)
		}
		previous.Members = append(previous.Members, output.PipelineMember{UUID: event.UUID, Bytes: int64(len(raw))})
	}
	input := output.PipelineAssembly{Generation: eventapi.NewID(), Attempt: eventapi.NewID(), Inputs: []output.PipelineAssemblyInput{{Fragment: previous, Eligible: []int{0, 2}}}}
	w := hostedPipelineDelivery{flusher: &hostedLiveFlusher{publisher: &hostedEffectPublisher{objects: h.objects}}}
	got, err := w.recomposeAssembly(t.Context(), h.scope, input)
	if err != nil || got.ID == original.DeliveryID || got.Count != 2 {
		t.Fatal(got, err)
	}
	retry, err := w.recomposeAssembly(t.Context(), h.scope, input)
	if err != nil || !reflect.DeepEqual(got, retry) {
		t.Fatal(got, retry, err)
	}
	raw, err := h.objects.ReadImmutableEffectObject(t.Context(), h.scope, nativeRef(got.Body))
	if err != nil {
		t.Fatal(err)
	}
	var body delivery.WebhookPayload
	if err := decodeHostedJSON(raw, &body); err != nil {
		t.Fatal(err)
	}
	if body.DeliveryID != got.ID || !reflect.DeepEqual(body.Events, []eventapi.Event{events[0], events[2]}) {
		t.Fatal(body)
	}
	if !reflect.DeepEqual(h.readResult(t, original).Events, events) {
		t.Fatal("removed member changed the immutable original")
	}
	input.Generation, input.Attempt = eventapi.NewID(), eventapi.NewID()
	input.Inputs[0].Fragment.Frozen = false
	unfrozen, err := w.recomposeAssembly(t.Context(), h.scope, input)
	if err != nil || unfrozen.ID != original.DeliveryID || unfrozen.Body.SHA256 == original.Body.SHA256 || unfrozen.Count != 2 {
		t.Fatal(unfrozen, err)
	}
}
