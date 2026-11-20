package porcupine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/olup/porcupine/internal/delivery"
	eventapi "github.com/olup/porcupine/internal/event"
	identityapi "github.com/olup/porcupine/internal/identity"
	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/output"
)

type frozenWebhookTarget struct {
	output.FrozenWebhookTarget
	cutoff time.Time
}
type frozenLiveOutputPlan struct {
	Scope              lakeapi.HostedProjectScope `json:"scope"`
	TenantID           string                     `json:"tenant_id"`
	Targets            []frozenWebhookTarget      `json:"targets"`
	MaxEventReferences int64                      `json:"max_event_references"`
}

// The stager builds output metadata and bodies from the frozen routing plan.
type hostedImmutableEffects struct {
	objects *lakeapi.DuckLakeObjectStore
	fence   lakeapi.ProjectWriteFence
	plan    frozenLiveOutputPlan
	put     func(context.Context, []byte) (lakeapi.ImmutableObjectRef, error)
}

func newHostedImmutableEffects(objects *lakeapi.DuckLakeObjectStore, fence lakeapi.ProjectWriteFence, plan frozenLiveOutputPlan) (*hostedImmutableEffects, error) {
	if objects == nil || plan.Scope.Key != fence.Key || plan.Scope.StreamID != fence.StreamID || plan.TenantID == "" || plan.MaxEventReferences <= 0 {
		return nil, errors.New("immutable output plan scope or limits are unavailable")
	}
	encoded, err := json.Marshal(plan)
	if err != nil {
		return nil, err
	}
	var detached frozenLiveOutputPlan
	if err := json.Unmarshal(encoded, &detached); err != nil {
		return nil, err
	}
	for i := range detached.Targets {
		target := &detached.Targets[i]
		if target.PipelineID == "" || len(target.PipelineID) > 200 || target.PipelineIncarnation == "" || len(target.PipelineIncarnation) > 200 || target.RuleRevision == "" || len(target.RuleRevision) > 200 || target.DestinationRevision == "" || len(target.DestinationRevision) > 200 {
			return nil, errors.New("output plan requires immutable pipeline and destination revisions")
		}
		if target.Rule.DeliveryMode != "" && target.Rule.DeliveryMode != "event" && target.Rule.DeliveryMode != "batch" {
			return nil, errors.New("invalid frozen webhook delivery mode")
		}
		if target.Rule.DeliveryMode == "batch" && (target.Rule.IntervalSeconds < 60 || target.Rule.IntervalSeconds > 86400) {
			return nil, errors.New("invalid frozen webhook interval")
		}
		if target.Rule.DeliveryMode != "batch" && (target.Rule.IntervalSeconds != 0 || target.Rule.StartAt != "") {
			return nil, errors.New("event webhook rules cannot set a batch interval or cutoff")
		}
		if target.Rule.StartAt != "" {
			if target.cutoff, err = time.Parse(time.RFC3339Nano, target.Rule.StartAt); err != nil {
				return nil, errors.New("invalid frozen webhook cutoff")
			}
		}
	}
	return &hostedImmutableEffects{objects: objects, fence: fence, plan: detached}, nil
}
func (t frozenWebhookTarget) match(e eventapi.Event) (time.Time, bool, error) {
	if !t.Rule.Matches(e.Event) {
		return time.Time{}, false, nil
	}
	received, err := time.Parse(time.RFC3339Nano, e.ReceivedAt)
	return received, !received.Before(t.cutoff), err
}

func (s *hostedImmutableEffects) prepare(ctx context.Context, selected lakeapi.SelectedLiveEffects) ([]output.PipelineOutput, error) {
	if selected.Scope != s.plan.Scope || selected.TenantID != s.plan.TenantID || selected.Sequence <= 0 || selected.BatchID == "" {
		return nil, errors.New("selected output scope differs from frozen plan")
	}
	for _, e := range selected.Events {
		if e.ProjectID != s.plan.Scope.Key.ProjectID || e.IdentitySnapshot != "" || identityapi.IsControl(e.Event) {
			return nil, errors.New("private identity controls cannot enter output staging")
		}
	}
	prepared := []output.PipelineOutput{}
	references := int64(0)
	for _, target := range s.plan.Targets {
		group := []eventapi.Event{}
		var windowStart, windowEnd *int64
		flush := func() error {
			if len(group) == 0 {
				return nil
			}
			idHash := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%d\x00%s\x00%s\x00%s\x00%d", selected.Scope.StreamID, selected.Sequence, target.PipelineIncarnation, target.RuleRevision, target.DestinationRevision, len(prepared))))
			id := hex.EncodeToString(idHash[:])
			o := output.PipelineOutput{ID: "pc_hook_" + id, PipelineID: target.PipelineID, Incarnation: target.PipelineIncarnation, RuleRevision: target.RuleRevision, DestinationRevision: target.DestinationRevision, Generation: s.fence.AcquisitionID, Kind: "delivery"}
			var payload any = delivery.WebhookPayload{DeliveryID: o.ID, PipelineID: target.PipelineID, ProjectID: selected.Scope.Key.ProjectID, Event: &group[0]}
			if target.Rule.DeliveryMode == "batch" {
				o.ID, o.Kind = "pc_frag_"+id, "fragment"
				o.Fragment = &output.PipelineFragmentSpec{WindowStartMS: *windowStart, WindowEndMS: *windowEnd, Rule: target.Rule}
				payload = group
			}
			raw, err := delivery.EncodeJSON(payload)
			if err != nil {
				return err
			}
			body, err := s.stageObject(ctx, raw)
			if err != nil {
				return err
			}
			o.Body = pipelineRef(body)
			var queued int64
			for _, e := range group {
				encoded, err := delivery.EncodeJSON(e)
				if err != nil {
					return err
				}
				o.Members = append(o.Members, output.PipelineMember{UUID: e.UUID, Bytes: int64(len(encoded))})
				queued += int64(len(encoded)) + 1024
			}
			o.Charge = output.PipelineDemand{References: int64(len(group)), QueuedBytes: body.Bytes, MetadataRows: int64(1 + len(group)), ArtifactBytes: body.Bytes}
			if o.Fragment != nil {
				o.Charge.QueuedBytes = max(queued, body.Bytes)
			}
			prepared = append(prepared, o)
			group = nil
			return nil
		}
		bytes := 0
		for _, e := range selected.Events {
			received, matches, err := target.match(e)
			if err != nil {
				return nil, err
			}
			if !matches {
				continue
			}

			if references >= s.plan.MaxEventReferences {
				return nil, delivery.ErrWebhookCapacity
			}
			references++
			if target.Rule.DeliveryMode != "batch" {
				group = []eventapi.Event{e}
				if err := flush(); err != nil {
					return nil, err
				}
				continue
			}
			interval := target.Rule.IntervalSeconds * 1000
			start := received.UnixMilli() / interval * interval
			end := start + interval
			encoded, err := delivery.EncodeJSON(e)
			if err != nil {
				return nil, err
			}
			if len(group) > 0 && (*windowEnd != end || len(group) >= delivery.WebhookChunkEvents || bytes+len(encoded)+1024 > delivery.WebhookChunkBytes) {
				if err := flush(); err != nil {
					return nil, err
				}
				bytes = 0
			}
			if len(group) == 0 {
				windowStart, windowEnd = &start, &end
			}
			group = append(group, e)
			bytes += len(encoded) + 1
		}
		if err := flush(); err != nil {
			return nil, err
		}
	}
	return prepared, nil
}

func (s *hostedImmutableEffects) stageObject(ctx context.Context, raw []byte) (lakeapi.ImmutableObjectRef, error) {
	if s.put != nil {
		return s.put(ctx, raw)
	}
	return s.objects.PutImmutableEffectObject(ctx, s.plan.Scope, s.fence.AcquisitionID, raw)
}
