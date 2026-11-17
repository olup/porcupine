package porcupine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/olup/porcupine/internal/delivery"
	eventapi "github.com/olup/porcupine/internal/event"
	lakeapi "github.com/olup/porcupine/internal/lake"
)

const hostedAssemblyMaxFragments = delivery.WebhookChunkEvents + 1
const hostedAssemblyMaxReadBytes int64 = 32 << 20

// These are private adapter values, not execution authority or a PG claim API.
// The future job store freezes and CASes this complete input and exact result.
type hostedFragmentPosition struct {
	Scope          lakeapi.HostedProjectScope
	CommitSequence int64
	FragmentID     string
	NextMember     int
}

type hostedAssemblyFragment struct {
	Position hostedFragmentPosition
	Fragment immutableWebhookFragment
	// Members is the frozen, increasing list of eligible indexes at/after
	// NextMember. Omitted indexes represent already-resolved deletion decisions.
	Members []int
}

type frozenWebhookAssemblyClaim struct {
	Scope                      lakeapi.HostedProjectScope
	TenantID                   string
	Target                     frozenWebhookTarget
	WindowStartMS, WindowEndMS int64
	// Generation is stable across worker takeover; AttemptID only isolates S3
	// uploads and must be the same UUID when retrying this exact frozen claim.
	Generation, AttemptID      string
	Fragments                  []hostedAssemblyFragment
	MembershipFingerprint      string
	MaxFragments               int
	MaxReadBytes, MaxBodyBytes int64
}

type hostedAssemblyMember struct {
	Position     hostedFragmentPosition
	UUID         string
	Bytes        int64
	FragmentBody lakeapi.ImmutableObjectRef
}

type immutableWebhookAssemblyManifest struct {
	Version                           int
	Scope                             lakeapi.HostedProjectScope
	TenantID                          string
	Target                            frozenWebhookTarget
	WindowStartMS, WindowEndMS        int64
	Generation, MembershipFingerprint string
	DeliveryID                        string
	Members                           []hostedAssemblyMember
	Body                              lakeapi.ImmutableObjectRef
	Next                              []hostedFragmentPosition
}

type hostedWebhookAssemblyResult struct {
	DeliveryID     string
	Body, Manifest lakeapi.ImmutableObjectRef
	EventCount     int
	Next           []hostedFragmentPosition
}

func hostedAssemblyFingerprint(fragments []hostedAssemblyFragment) (string, error) {
	raw, err := json.Marshal(fragments)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}

func validateHostedAssemblyClaim(claim frozenWebhookAssemblyClaim) error {
	bad := errors.New("invalid frozen webhook assembly claim")
	bounded := func(s string) bool { return len(s) > 0 && len(s) <= 200 }
	scope := claim.Scope
	if !bounded(scope.Key.SourceID) || !bounded(scope.Key.ProjectID) || scope.CatalogID != lakeapi.SharedNativeCatalogID(scope.Key.SourceID, scope.Key.ProjectID) || len(scope.StreamID) != 36 || !lakeapi.LakePrefixSegmentPattern.MatchString(scope.StreamID) || !bounded(claim.TenantID) || !bounded(claim.Generation) || len(claim.AttemptID) != 36 || !lakeapi.LakePrefixSegmentPattern.MatchString(claim.AttemptID) {
		return bad
	}
	target := claim.Target
	if !bounded(target.PipelineID) || !bounded(target.PipelineIncarnation) || !bounded(target.RuleRevision) || !bounded(target.DestinationRevision) || target.Rule.DeliveryMode != "batch" || target.Rule.IntervalSeconds < 60 || target.Rule.IntervalSeconds > 86400 || len(target.Rule.IncludeEvents) > 100 || len(target.Rule.ExcludeEvents) > 100 || len(target.Rule.StartAt) > 50 {
		return bad
	}
	for _, names := range [][]string{target.Rule.IncludeEvents, target.Rule.ExcludeEvents} {
		for _, name := range names {
			if !bounded(name) {
				return bad
			}
		}
	}
	if target.Rule.StartAt != "" {
		if _, err := time.Parse(time.RFC3339Nano, target.Rule.StartAt); err != nil {
			return bad
		}
	}
	interval := target.Rule.IntervalSeconds * 1000
	if claim.WindowStartMS < 0 || claim.WindowStartMS%interval != 0 || claim.WindowEndMS <= claim.WindowStartMS || claim.WindowEndMS-claim.WindowStartMS != interval {
		return bad
	}
	if claim.MaxFragments <= 0 || claim.MaxFragments > hostedAssemblyMaxFragments || len(claim.Fragments) == 0 || len(claim.Fragments) > claim.MaxFragments || claim.MaxReadBytes <= 0 || claim.MaxReadBytes > hostedAssemblyMaxReadBytes || claim.MaxBodyBytes <= 0 || claim.MaxBodyBytes > lakeapi.MaxImmutableEffectObjectBytes {
		return bad
	}
	seen := map[string]bool{}
	uuids := map[string]bool{}
	readBytes := int64(0)
	sequence := int64(0)
	for _, input := range claim.Fragments {
		p, f := input.Position, input.Fragment
		if p.Scope != scope || p.CommitSequence <= 0 || p.CommitSequence < sequence || p.FragmentID != f.FragmentID || !bounded(f.FragmentID) || seen[f.FragmentID] || f.WindowStartMS != claim.WindowStartMS || f.WindowEndMS != claim.WindowEndMS || len(f.EventUUIDs) == 0 || len(f.EventUUIDs) > delivery.WebhookChunkEvents || len(f.EventBytes) != len(f.EventUUIDs) || p.NextMember < 0 || p.NextMember >= len(f.EventUUIDs) || len(input.Members) == 0 || len(input.Members) > len(f.EventUUIDs) {
			return bad
		}
		sequence = p.CommitSequence
		seen[f.FragmentID] = true
		ft := f.Target
		if ft.PipelineID != target.PipelineID || ft.PipelineIncarnation != target.PipelineIncarnation || ft.RuleRevision != target.RuleRevision || ft.DestinationRevision != target.DestinationRevision || ft.Rule.DeliveryMode != target.Rule.DeliveryMode || ft.Rule.IntervalSeconds != target.Rule.IntervalSeconds || ft.Rule.StartAt != target.Rule.StartAt || !slices.Equal(ft.Rule.IncludeEvents, target.Rule.IncludeEvents) || !slices.Equal(ft.Rule.ExcludeEvents, target.Rule.ExcludeEvents) {
			return bad
		}
		ref := f.Body
		if ref.Bytes <= 0 || ref.Bytes > lakeapi.MaxImmutableEffectObjectBytes || len(ref.Key) > 2048 || len(ref.VersionID) > 1024 || len(ref.SHA256) != 64 || ref.Bytes > claim.MaxReadBytes-readBytes {
			return bad
		}
		readBytes += ref.Bytes
		expected := int64(2 + len(f.EventUUIDs) - 1)
		for i, uuid := range f.EventUUIDs {
			if !bounded(uuid) || f.EventBytes[i] <= 0 || f.EventBytes[i] > ref.Bytes-expected {
				return bad
			}
			expected += f.EventBytes[i]
		}
		if expected != ref.Bytes {
			return bad
		}
		last := p.NextMember - 1
		for _, member := range input.Members {
			if member <= last || member >= len(f.EventUUIDs) || uuids[f.EventUUIDs[member]] {
				return bad
			}
			uuids[f.EventUUIDs[member]] = true
			last = member
		}
	}
	fingerprint, err := hostedAssemblyFingerprint(claim.Fragments)
	if err != nil || claim.MembershipFingerprint != fingerprint {
		return errors.New("webhook assembly membership fingerprint mismatch")
	}
	return nil
}

// assembleHostedWebhook writes one immutable chunk and its manifest. It never
// publishes a job or checkpoint. Failure can leave orphan objects, but returns
// no partial result. The caller must CAS the exact frozen input and next cursor.
func assembleHostedWebhook(ctx context.Context, objects *lakeapi.DuckLakeObjectStore, claim frozenWebhookAssemblyClaim) (hostedWebhookAssemblyResult, error) {
	var zero hostedWebhookAssemblyResult
	if err := validateHostedAssemblyClaim(claim); err != nil {
		return zero, err
	}
	if objects == nil {
		return zero, errors.New("webhook assembly object store is unavailable")
	}
	// Validate budgets and choose the chunk using metadata before reading S3.
	prefix, err := delivery.WebhookBatchPrefix("pc_hook_"+strings.Repeat("0", 64), claim.Target.PipelineID, claim.Scope.Key.ProjectID, claim.WindowStartMS, claim.WindowEndMS)
	if err != nil {
		return zero, err
	}
	var candidates []hostedAssemblyMember
	var lengths []int64
	for _, input := range claim.Fragments {
		for _, index := range input.Members {
			position := input.Position
			position.NextMember = index
			candidates = append(candidates, hostedAssemblyMember{position, input.Fragment.EventUUIDs[index], input.Fragment.EventBytes[index], input.Fragment.Body})
			lengths = append(lengths, input.Fragment.EventBytes[index])
		}
	}
	count, err := delivery.WebhookBatchCount(len(prefix), lengths, claim.MaxBodyBytes)
	if err != nil {
		return zero, err
	}
	members := candidates[:count]
	identity := struct {
		Version    int
		Scope      lakeapi.HostedProjectScope
		TenantID   string
		Target     frozenWebhookTarget
		Start, End int64
		Generation string
		Members    []hostedAssemblyMember
	}{1, claim.Scope, claim.TenantID, claim.Target, claim.WindowStartMS, claim.WindowEndMS, claim.Generation, members}
	raw, err := json.Marshal(identity)
	if err != nil {
		return zero, err
	}
	digest := sha256.Sum256(raw)
	id := "pc_hook_" + hex.EncodeToString(digest[:])
	prefix, err = delivery.WebhookBatchPrefix(id, claim.Target.PipelineID, claim.Scope.Key.ProjectID, claim.WindowStartMS, claim.WindowEndMS)
	if err != nil {
		return zero, err
	}
	encoded := make([][]byte, 0, count)
	next := make([]hostedFragmentPosition, len(claim.Fragments))
	for i, input := range claim.Fragments {
		next[i] = input.Position
		if len(encoded) == count {
			continue
		}
		events, err := readHostedAssemblyFragment(ctx, objects, claim, input.Fragment)
		if err != nil {
			return zero, err
		}
		for j, index := range input.Members {
			if len(encoded) == count {
				break
			}
			encoded = append(encoded, events[index])
			next[i].NextMember = len(input.Fragment.EventUUIDs)
			if j+1 < len(input.Members) {
				next[i].NextMember = input.Members[j+1]
			}
		}
	}
	body, err := delivery.EncodeWebhookBatch(prefix, encoded, claim.MaxBodyBytes)
	if err != nil {
		return zero, err
	}
	bodyRef, err := objects.PutImmutableEffectObject(ctx, claim.Scope, claim.AttemptID, body)
	if err != nil {
		return zero, err
	}
	manifest := immutableWebhookAssemblyManifest{1, claim.Scope, claim.TenantID, claim.Target, claim.WindowStartMS, claim.WindowEndMS, claim.Generation, claim.MembershipFingerprint, id, members, bodyRef, next}
	raw, err = json.Marshal(manifest)
	if err != nil {
		return zero, err
	}
	manifestRef, err := objects.PutImmutableEffectObject(ctx, claim.Scope, claim.AttemptID, raw)
	if err != nil {
		return zero, err
	}
	return hostedWebhookAssemblyResult{id, bodyRef, manifestRef, count, next}, nil
}

func readHostedAssemblyFragment(ctx context.Context, objects *lakeapi.DuckLakeObjectStore, claim frozenWebhookAssemblyClaim, fragment immutableWebhookFragment) ([][]byte, error) {
	raw, err := objects.ReadImmutableEffectObject(ctx, claim.Scope, fragment.Body)
	if err != nil {
		return nil, err
	}
	events, err := decodeHostedAssemblyMembers(bytes.NewReader(raw), len(fragment.EventUUIDs))
	if err != nil {
		return nil, err
	}
	encoded := make([][]byte, len(events))
	for i, raw := range events {
		if int64(len(raw)) != fragment.EventBytes[i] {
			return nil, errors.New("webhook fragment event length mismatch")
		}
		var event eventapi.Event
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&event); err != nil {
			return nil, errors.New("invalid webhook fragment event")
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			return nil, errors.New("extra webhook fragment event data")
		}
		if event.UUID != fragment.EventUUIDs[i] || event.ProjectID != claim.Scope.Key.ProjectID || event.IdentitySnapshot != "" || isIdentityControl(event.Event) || !claim.Target.Rule.Matches(event.Event) {
			return nil, errors.New("webhook fragment member differs from frozen claim")
		}
		received, err := time.Parse(time.RFC3339Nano, event.ReceivedAt)
		if err != nil || received.UTC().Format(time.RFC3339Nano) != event.ReceivedAt || received.UnixMilli() < claim.WindowStartMS || received.UnixMilli() >= claim.WindowEndMS {
			return nil, errors.New("webhook fragment ReceivedAt window mismatch")
		}
		if claim.Target.Rule.StartAt != "" {
			cutoff, _ := time.Parse(time.RFC3339Nano, claim.Target.Rule.StartAt)
			if received.Before(cutoff) {
				return nil, errors.New("webhook fragment precedes rule cutoff")
			}
		}
		canonical, err := delivery.EncodeJSON(event)
		if err != nil || !bytes.Equal(canonical, raw) {
			return nil, fmt.Errorf("webhook fragment member %d is not canonical", i)
		}
		encoded[i] = raw
	}
	return encoded, nil
}

func decodeHostedAssemblyMembers(input io.Reader, expected int) ([]json.RawMessage, error) {
	if expected < 1 || expected > delivery.WebhookChunkEvents {
		return nil, errors.New("webhook fragment event count mismatch")
	}
	decoder := json.NewDecoder(input)
	start, err := decoder.Token()
	if err != nil || start != json.Delim('[') {
		return nil, errors.New("invalid webhook fragment array")
	}
	events := make([]json.RawMessage, 0, expected)
	for decoder.More() {
		if len(events) == expected {
			return nil, errors.New("webhook fragment event count mismatch")
		}
		var event json.RawMessage
		if err := decoder.Decode(&event); err != nil {
			return nil, errors.New("invalid webhook fragment event")
		}
		events = append(events, event)
	}
	end, err := decoder.Token()
	if err != nil || end != json.Delim(']') || len(events) != expected {
		return nil, errors.New("webhook fragment event count mismatch")
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, errors.New("extra webhook fragment array data")
	}
	return events, nil
}
