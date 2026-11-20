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
	claim := h.claim(h.stage(t, events))
	original, err := output.StageWebhookAssembly(t.Context(), h.objects, h.scope, claim)
	if err != nil {
		t.Fatal(err)
	}
	previous := output.PipelineFragment{
		Claim:  output.PipelineClaim{ID: original.ID},
		Target: output.PipelineTarget{ID: h.target.PipelineID},
		Body:   original.Body,
		Spec:   claim.Inputs[0].Fragment.Spec,
		Count:  3, Recompose: true, Frozen: true,
	}
	for _, event := range events {
		raw, err := delivery.EncodeJSON(event)
		if err != nil {
			t.Fatal(err)
		}
		previous.Members = append(previous.Members, output.PipelineMember{UUID: event.UUID, Bytes: int64(len(raw))})
	}
	input := h.claim([]output.PipelineAssemblyInput{{Fragment: previous, Eligible: []int{0, 2}}})
	got, err := output.StageWebhookAssembly(t.Context(), h.objects, h.scope, input)
	if err != nil || got.ID == original.ID || got.Count != 2 {
		t.Fatal(got, err)
	}
	retry, err := output.StageWebhookAssembly(t.Context(), h.objects, h.scope, input)
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
	if !reflect.DeepEqual(h.readResult(t, original).payload.Events, events) {
		t.Fatal("removed member changed the immutable original")
	}
	input.Generation, input.Attempt = eventapi.NewID(), eventapi.NewID()
	input.Inputs[0].Fragment.Frozen = false
	unfrozen, err := output.StageWebhookAssembly(t.Context(), h.objects, h.scope, input)
	if err != nil || unfrozen.ID != original.ID || unfrozen.Body.SHA256 == original.Body.SHA256 || unfrozen.Count != 2 {
		t.Fatal(unfrozen, err)
	}
}
