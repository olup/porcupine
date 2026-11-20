package output

import (
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/olup/porcupine/internal/delivery"
	eventapi "github.com/olup/porcupine/internal/event"
	identityapi "github.com/olup/porcupine/internal/identity"
	lakeapi "github.com/olup/porcupine/internal/lake"
)

// HostedWebhookPlan holds the rules and scope fixed for a native live commit.
type HostedWebhookPlan struct {
	Scope              lakeapi.HostedProjectScope
	TenantID           string
	Targets            []FrozenWebhookTarget
	MaxEventReferences int64
}

const maxHostedPreparedObjectBytes int64 = 64 << 20

// PreparedEffectObject is a body and its content address before upload.
type PreparedEffectObject struct {
	Ref  lakeapi.ImmutableObjectRef
	Body []byte
}

// HostedWebhookPreparation describes webhook outputs without uploading them.
type HostedWebhookPreparation struct {
	objects            *lakeapi.DuckLakeObjectStore
	scope              lakeapi.HostedProjectScope
	tenantID           string
	acquisitionID      string
	maxEventReferences int64
	targets            []frozenWebhookTarget
}

type frozenWebhookTarget struct {
	FrozenWebhookTarget
	cutoff time.Time
}

func NewHostedWebhookPreparation(objects *lakeapi.DuckLakeObjectStore, fence lakeapi.ProjectWriteFence, plan HostedWebhookPlan) (*HostedWebhookPreparation, error) {
	if objects == nil || plan.Scope.Key != fence.Key || plan.Scope.StreamID != fence.StreamID || plan.TenantID == "" || plan.MaxEventReferences <= 0 {
		return nil, errors.New("immutable output plan scope or limits are unavailable")
	}
	targets := make([]frozenWebhookTarget, len(plan.Targets))
	for i, ruleTarget := range plan.Targets {
		target := frozenWebhookTarget{FrozenWebhookTarget: ruleTarget}
		if !pipelineID(target.PipelineID) || !pipelineID(target.PipelineIncarnation) || !pipelineID(target.RuleRevision) || !pipelineID(target.DestinationRevision) {
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
			var err error
			if target.cutoff, err = time.Parse(time.RFC3339Nano, target.Rule.StartAt); err != nil {
				return nil, errors.New("invalid frozen webhook cutoff")
			}
		}
		target.Rule.IncludeEvents = slices.Clone(target.Rule.IncludeEvents)
		target.Rule.ExcludeEvents = slices.Clone(target.Rule.ExcludeEvents)
		targets[i] = target
	}
	return &HostedWebhookPreparation{
		objects: objects, scope: plan.Scope, tenantID: plan.TenantID, acquisitionID: fence.AcquisitionID,
		maxEventReferences: plan.MaxEventReferences, targets: targets,
	}, nil
}

func (t frozenWebhookTarget) match(e eventapi.Event) (time.Time, bool, error) {
	if !t.Rule.Matches(e.Event) {
		return time.Time{}, false, nil
	}
	received, err := time.Parse(time.RFC3339Nano, e.ReceivedAt)
	return received, !received.Before(t.cutoff), err
}

// Prepare returns metadata and body objects in matching order for reservation and later upload.
func (s *HostedWebhookPreparation) Prepare(selected lakeapi.SelectedLiveEffects) ([]PipelineOutput, []PreparedEffectObject, error) {
	if selected.Scope != s.scope || selected.TenantID != s.tenantID || selected.Sequence <= 0 || selected.BatchID == "" {
		return nil, nil, errors.New("selected output scope differs from frozen plan")
	}
	for _, e := range selected.Events {
		if e.ProjectID != s.scope.Key.ProjectID || e.IdentitySnapshot != "" || identityapi.IsControl(e.Event) {
			return nil, nil, errors.New("private identity controls cannot enter output staging")
		}
	}
	prepared := []PipelineOutput{}
	var objects []PreparedEffectObject
	var preparedBytes int64
	references := int64(0)
	for _, target := range s.targets {
		group := []eventapi.Event{}
		var windowStart, windowEnd *int64
		flush := func() error {
			if len(group) == 0 {
				return nil
			}
			id := sha256Hex([]byte(fmt.Sprintf("%s\x00%d\x00%s\x00%s\x00%s\x00%d", selected.Scope.StreamID, selected.Sequence, target.PipelineIncarnation, target.RuleRevision, target.DestinationRevision, len(prepared))))
			o := PipelineOutput{ID: "pc_hook_" + id, PipelineID: target.PipelineID, Incarnation: target.PipelineIncarnation, RuleRevision: target.RuleRevision, DestinationRevision: target.DestinationRevision, Generation: s.acquisitionID, Kind: "delivery"}
			var payload any = delivery.WebhookPayload{DeliveryID: o.ID, PipelineID: target.PipelineID, ProjectID: selected.Scope.Key.ProjectID, Event: &group[0]}
			if target.Rule.DeliveryMode == "batch" {
				o.ID, o.Kind = "pc_frag_"+id, "fragment"
				o.Fragment = &PipelineFragmentSpec{WindowStartMS: *windowStart, WindowEndMS: *windowEnd, Rule: target.Rule}
				payload = group
			}
			raw, err := delivery.EncodeJSON(payload)
			if err != nil {
				return err
			}
			if int64(len(raw)) > lakeapi.MaxImmutableEffectObjectBytes || int64(len(raw)) > maxHostedPreparedObjectBytes-preparedBytes {
				return ErrPipelineCapacity
			}
			body, _, err := s.objects.DescribeImmutableEffectObject(selected.Scope, s.acquisitionID, raw)
			if err != nil {
				return err
			}
			o.Body = PipelineRef(body)
			var queued int64
			for _, e := range group {
				encoded, err := delivery.EncodeJSON(e)
				if err != nil {
					return err
				}
				o.Members = append(o.Members, PipelineMember{UUID: e.UUID, Bytes: int64(len(encoded))})
				queued += int64(len(encoded)) + 1024
			}
			o.Charge = PipelineDemand{References: int64(len(group)), QueuedBytes: body.Bytes, MetadataRows: int64(1 + len(group)), ArtifactBytes: body.Bytes}
			if o.Fragment != nil {
				o.Charge.QueuedBytes = max(queued, body.Bytes)
			}
			prepared = append(prepared, o)
			objects = append(objects, PreparedEffectObject{Ref: body, Body: raw})
			preparedBytes += int64(len(raw))
			group = nil
			return nil
		}
		bytes := 0
		for _, e := range selected.Events {
			received, matches, err := target.match(e)
			if err != nil {
				return nil, nil, err
			}
			if !matches {
				continue
			}

			if references >= s.maxEventReferences {
				return nil, nil, delivery.ErrWebhookCapacity
			}
			references++
			if target.Rule.DeliveryMode != "batch" {
				group = []eventapi.Event{e}
				if err := flush(); err != nil {
					return nil, nil, err
				}
				continue
			}
			interval := target.Rule.IntervalSeconds * 1000
			start := received.UnixMilli() / interval * interval
			end := start + interval
			encoded, err := delivery.EncodeJSON(e)
			if err != nil {
				return nil, nil, err
			}
			if len(group) > 0 && (*windowEnd != end || len(group) >= delivery.WebhookChunkEvents || bytes+len(encoded)+1024 > delivery.WebhookChunkBytes) {
				if err := flush(); err != nil {
					return nil, nil, err
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
			return nil, nil, err
		}
	}
	return prepared, objects, nil
}

// CountReferences scans all selected events for each target when output is refused.
func (s *HostedWebhookPreparation) CountReferences(events []eventapi.Event) (map[string]uint64, error) {
	counts := make(map[string]uint64)
	for _, target := range s.targets {
		for _, e := range events {
			_, matches, err := target.match(e)
			if err != nil {
				return counts, err
			}
			if matches {
				counts[target.PipelineID]++
			}
		}
	}
	return counts, nil
}
