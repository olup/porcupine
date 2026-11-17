package lake

import (
	"context"
	"encoding/json"
	"time"

	"github.com/olup/porcupine/internal/event"
	identityapi "github.com/olup/porcupine/internal/identity"
)

type LiveCommitInput struct {
	Event      event.Event
	Identity   *identityapi.Command
	AdmittedAt time.Time
}
type LiveCommitBatch struct {
	ID, TenantID string
	Scope        HostedProjectScope
	Inputs       []LiveCommitInput
}
type ImmutableObjectRef struct {
	Key       string `json:"key"`
	VersionID string `json:"version_id,omitempty"`
	SHA256    string `json:"sha256"`
	Bytes     int64  `json:"bytes"`
}
type StagedLiveEffects struct {
	PlanID   string             `json:"plan_id"`
	Manifest ImmutableObjectRef `json:"manifest"`
	// OutputCount counts direct outputs plus batch fragments, not final deliveries.
	OutputCount int64 `json:"output_count"`
}

// SelectedLiveEffects contains only ordinary, output-eligible canonical events.
// Identity controls and private snapshots never cross this stager boundary.
type SelectedLiveEffects struct {
	Scope             HostedProjectScope
	TenantID, BatchID string
	Sequence          int64
	Events            []event.Event
}

// LiveEffectStager freezes exact pipeline incarnations, rule and destination
// versions, ordered membership and immutable body references before native
// commit. It cannot publish operational jobs or acknowledge admission.
type LiveEffectStager interface {
	StageLiveEffects(context.Context, SelectedLiveEffects) (StagedLiveEffects, error)
}
type NativeSourceTotals struct {
	Day          string `json:"day"`
	Revision     int64  `json:"revision"`
	Live, Import int64
}
type LiveCommitEvidence struct {
	Scope                                       HostedProjectScope
	TenantID                                    string
	Sequence                                    int64
	BatchID, BatchFingerprint, ProofFingerprint string
	FenceEpoch                                  int64
	AccountedAt                                 time.Time
	AcceptedLive                                int64
	Effects                                     StagedLiveEffects
	Totals                                      []NativeSourceTotals
}
type LiveCommitResult interface{ liveCommitResult() }
type CommittedLiveBatch struct{ Evidence LiveCommitEvidence }

func (CommittedLiveBatch) liveCommitResult() {}

type DuplicateLiveBatch struct{}

func (DuplicateLiveBatch) liveCommitResult() {}

type EventIDReservation struct {
	fence   ProjectWriteFence
	batchID string
	ids     []int64
}

func (r EventIDReservation) matches(fence ProjectWriteFence, batchID string, count int) bool {
	return r.fence == fence && r.batchID == batchID && len(r.ids) >= count
}

type nativeLiveIdentitySnapshot struct {
	IdentitySnapshot
	Identified bool `json:"is_identified"`
}

func emptyLiveIdentitySnapshot(id string) nativeLiveIdentitySnapshot {
	return nativeLiveIdentitySnapshot{IdentitySnapshot: IdentitySnapshot{Version: id, Traits: map[string]any{}, Links: []IdentityLinkSnapshot{}}}
}
func encodeNativeLiveSnapshot(snapshot nativeLiveIdentitySnapshot) (string, error) {
	raw, err := json.Marshal(snapshot)
	return string(raw), err
}
