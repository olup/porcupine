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
	lakeapi "github.com/olup/porcupine/internal/lake"
)

type frozenWebhookTarget struct {
	PipelineID          string               `json:"pipeline_id"`
	PipelineIncarnation string               `json:"pipeline_incarnation"`
	RuleRevision        string               `json:"rule_revision"`
	DestinationRevision string               `json:"destination_revision"`
	Rule                delivery.WebhookRule `json:"rule"`
}
type frozenLiveOutputPlan struct {
	Scope              lakeapi.HostedProjectScope `json:"scope"`
	TenantID           string                     `json:"tenant_id"`
	Targets            []frozenWebhookTarget      `json:"targets"`
	MaxEventReferences int64                      `json:"max_event_references"`
}
type immutableWebhookOutput struct {
	DeliveryID string                     `json:"delivery_id"`
	Target     frozenWebhookTarget        `json:"target"`
	EventUUIDs []string                   `json:"event_uuids"`
	Body       lakeapi.ImmutableObjectRef `json:"body"`
}
type immutableWebhookFragment struct {
	FragmentID                 string                     `json:"fragment_id"`
	Target                     frozenWebhookTarget        `json:"target"`
	EventUUIDs                 []string                   `json:"event_uuids"`
	EventBytes                 []int64                    `json:"event_bytes"`
	Body                       lakeapi.ImmutableObjectRef `json:"body"`
	WindowStartMS, WindowEndMS int64
}
type immutableLiveEffectManifest struct {
	Version           int                        `json:"version"`
	Scope             lakeapi.HostedProjectScope `json:"scope"`
	TenantID, BatchID string
	Sequence          int64                      `json:"sequence"`
	PlanID            string                     `json:"plan_id"`
	Outputs           []immutableWebhookOutput   `json:"outputs"`
	BatchFragments    []immutableWebhookFragment `json:"batch_fragments"`
}

// Only the frozen routing plan and immutable object store are retained. This
// component cannot query mutable pipeline state, publish jobs or commit events.
type hostedImmutableEffects struct {
	objects *lakeapi.DuckLakeObjectStore
	fence   lakeapi.ProjectWriteFence
	plan    frozenLiveOutputPlan
	planID  string
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
	for _, target := range detached.Targets {
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
			if _, err := time.Parse(time.RFC3339Nano, target.Rule.StartAt); err != nil {
				return nil, errors.New("invalid frozen webhook cutoff")
			}
		}
	}
	digest := sha256.Sum256(encoded)
	return &hostedImmutableEffects{objects: objects, fence: fence, plan: detached, planID: hex.EncodeToString(digest[:])}, nil
}
func (s *hostedImmutableEffects) StageLiveEffects(ctx context.Context, selected lakeapi.SelectedLiveEffects) (lakeapi.StagedLiveEffects, error) {
	if selected.Scope != s.plan.Scope || selected.TenantID != s.plan.TenantID || selected.Sequence <= 0 || selected.BatchID == "" {
		return lakeapi.StagedLiveEffects{}, errors.New("selected output scope differs from frozen plan")
	}
	for _, e := range selected.Events {
		if e.ProjectID != s.plan.Scope.Key.ProjectID || e.IdentitySnapshot != "" || isIdentityControl(e.Event) {
			return lakeapi.StagedLiveEffects{}, errors.New("private identity controls cannot enter output staging")
		}
	}
	manifest := immutableLiveEffectManifest{Version: 1, Scope: selected.Scope, TenantID: selected.TenantID, BatchID: selected.BatchID, Sequence: selected.Sequence, PlanID: s.planID}
	references := int64(0)
	for _, target := range s.plan.Targets {
		group := []eventapi.Event{}
		var windowStart, windowEnd *int64
		flush := func() error {
			if len(group) == 0 {
				return nil
			}
			idHash := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%d\x00%s\x00%s\x00%s\x00%d", selected.Scope.StreamID, selected.Sequence, target.PipelineIncarnation, target.RuleRevision, target.DestinationRevision, len(manifest.Outputs)+len(manifest.BatchFragments))))
			id := hex.EncodeToString(idHash[:])
			var raw []byte
			var err error
			if target.Rule.DeliveryMode == "batch" {
				// Fragments are not deliveries. The window assembler combines them
				// across native commits before freezing the final body and ID.
				raw, err = delivery.EncodeJSON(group)
			} else {
				id = "pc_hook_" + id
				raw, err = delivery.EncodeJSON(delivery.WebhookPayload{DeliveryID: id, PipelineID: target.PipelineID, ProjectID: selected.Scope.Key.ProjectID, Event: &group[0]})
			}
			if err != nil {
				return err
			}
			body, err := s.stageObject(ctx, raw)
			if err != nil {
				return err
			}
			if target.Rule.DeliveryMode == "batch" {
				fragment := immutableWebhookFragment{FragmentID: "pc_frag_" + id, Target: target, Body: body, WindowStartMS: *windowStart, WindowEndMS: *windowEnd}
				for _, e := range group {
					encoded, err := delivery.EncodeJSON(e)
					if err != nil {
						return err
					}
					fragment.EventUUIDs = append(fragment.EventUUIDs, e.UUID)
					fragment.EventBytes = append(fragment.EventBytes, int64(len(encoded)))
				}
				manifest.BatchFragments = append(manifest.BatchFragments, fragment)
			} else {
				output := immutableWebhookOutput{DeliveryID: id, Target: target, Body: body}
				for _, e := range group {
					output.EventUUIDs = append(output.EventUUIDs, e.UUID)
				}
				manifest.Outputs = append(manifest.Outputs, output)
			}
			group = nil
			return nil
		}
		bytes := 0
		for _, e := range selected.Events {
			if !target.Rule.Matches(e.Event) {
				continue
			}
			received, err := time.Parse(time.RFC3339Nano, e.ReceivedAt)
			if err != nil {
				return lakeapi.StagedLiveEffects{}, err
			}
			if target.Rule.StartAt != "" {
				start, err := time.Parse(time.RFC3339Nano, target.Rule.StartAt)
				if err != nil {
					return lakeapi.StagedLiveEffects{}, err
				}
				if received.Before(start) {
					continue
				}
			}
			if references >= s.plan.MaxEventReferences {
				return lakeapi.StagedLiveEffects{}, delivery.ErrWebhookCapacity
			}
			references++
			if target.Rule.DeliveryMode != "batch" {
				group = []eventapi.Event{e}
				if err := flush(); err != nil {
					return lakeapi.StagedLiveEffects{}, err
				}
				continue
			}
			interval := target.Rule.IntervalSeconds * 1000
			start := received.UnixMilli() / interval * interval
			end := start + interval
			encoded, err := delivery.EncodeJSON(e)
			if err != nil {
				return lakeapi.StagedLiveEffects{}, err
			}
			if len(group) > 0 && (*windowEnd != end || len(group) >= delivery.WebhookChunkEvents || bytes+len(encoded)+1024 > delivery.WebhookChunkBytes) {
				if err := flush(); err != nil {
					return lakeapi.StagedLiveEffects{}, err
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
			return lakeapi.StagedLiveEffects{}, err
		}
	}
	staged := lakeapi.StagedLiveEffects{PlanID: s.planID, OutputCount: int64(len(manifest.Outputs) + len(manifest.BatchFragments))}
	if staged.OutputCount == 0 {
		return staged, nil
	}
	raw, err := json.Marshal(manifest)
	if err != nil {
		return lakeapi.StagedLiveEffects{}, err
	}
	staged.Manifest, err = s.stageObject(ctx, raw)
	return staged, err
}

func (s *hostedImmutableEffects) stageObject(ctx context.Context, raw []byte) (lakeapi.ImmutableObjectRef, error) {
	if s.put != nil {
		return s.put(ctx, raw)
	}
	return s.objects.PutImmutableEffectObject(ctx, s.plan.Scope, s.fence.AcquisitionID, raw)
}
