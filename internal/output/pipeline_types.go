package output

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
)

var ErrPipelineCapacity = errors.New("pipeline capacity exhausted")
var ErrPipelineConflict = errors.New("pipeline binding or attempt conflict")
var ErrPipelineAuthority = errors.New("pipeline execution authority unavailable")

type PipelineScope struct{ SourceID, ProjectID, CatalogID, StreamID string }
type PipelineRef struct {
	Key, VersionID, SHA256 string
	Bytes                  int64
}
type PipelineDemand struct{ References, QueuedBytes, MetadataRows, ArtifactBytes, ProofSlots int64 }

func (d PipelineDemand) values() []int64 {
	return []int64{d.References, d.QueuedBytes, d.MetadataRows, d.ArtifactBytes, d.ProofSlots}
}
func (d PipelineDemand) valid() bool {
	for _, v := range d.values() {
		if v < 0 {
			return false
		}
	}
	return true
}
func (d PipelineDemand) fits(limit PipelineDemand) bool {
	a, b := d.values(), limit.values()
	for i := range a {
		if a[i] > b[i] {
			return false
		}
	}
	return true
}
func (d PipelineDemand) positive() bool {
	for _, v := range d.values() {
		if v <= 0 {
			return false
		}
	}
	return true
}
func (s PipelineScope) valid() bool {
	return pipelineID(s.SourceID) && pipelineID(s.ProjectID) && pipelineID(s.CatalogID) && pipelineID(s.StreamID)
}
func pipelineID(s string) bool   { return len(s) > 0 && len(s) <= 200 }
func pipelineHash(s string) bool { b, e := hex.DecodeString(s); return e == nil && len(b) == 32 }
func (r PipelineRef) valid() bool {
	return r.Key != "" && len(r.Key) <= 2048 && len(r.VersionID) <= 1024 && pipelineHash(r.SHA256) && r.Bytes > 0
}
func pipelineDigest(v any) string {
	b, _ := json.Marshal(v)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

type PipelineTarget struct {
	ID, Incarnation, RuleRevision, DestinationRevision, CreatorID string
	Rule                                                          PipelineRef
}
type PipelinePlan struct {
	ID, TenantID string
	Scope        PipelineScope
	Targets      []PipelineTarget
}
type PipelineBinding struct {
	Scope                                                                    PipelineScope
	TenantID, BatchID, PlanID, PreparedSHA256, AcquisitionID, ArtifactPrefix string
	Epoch, ManifestBytesLimit                                                int64
	PlanRef                                                                  PipelineRef
	Demand                                                                   PipelineDemand
}

func (b PipelineBinding) valid() bool {
	return b.Scope.valid() && pipelineID(b.TenantID) && pipelineID(b.BatchID) && pipelineHash(b.PlanID) && pipelineHash(b.PreparedSHA256) && pipelineID(b.AcquisitionID) && len(b.ArtifactPrefix) > 0 && len(b.ArtifactPrefix) <= 2048 && b.Epoch > 0 && b.PlanRef.valid() && b.Demand.valid() && b.Demand.ProofSlots == 1 && b.Demand.MetadataRows >= 1 && b.ManifestBytesLimit >= 0 && b.Demand.ArtifactBytes >= b.PlanRef.Bytes && b.ManifestBytesLimit <= b.Demand.ArtifactBytes-b.PlanRef.Bytes
}

// ResolveCreatorTx must use the existing auth adapter and current credential/consent
// state in this transaction. The caller holds the project session and target row
// locks. No producer JWT is passed. False is a terminal retirement; errors abort.
type PipelineAuthority interface {
	ResolveCreatorTx(context.Context, *sql.Tx, string, string, string) (bool, error)
}

// Settlement evidence is supplied by the native integration adapter. This store
// cannot establish native absence. Expiry alone never supplies a decision.
type PipelineSettlement struct {
	SuccessorEpoch int64
	Outcome        PipelineSettlementOutcome
	ProofSHA256    string
}
type PipelineSettlementOutcome string

const (
	PipelineKeepReservation    PipelineSettlementOutcome = "unresolved"
	PipelinePublishReservation PipelineSettlementOutcome = "publish"
	PipelineReleaseReservation PipelineSettlementOutcome = "proven_absent"
)

func PipelineSettlementDecision(b PipelineBinding, e PipelineSettlement) PipelineSettlementOutcome {
	if e.SuccessorEpoch <= b.Epoch {
		return PipelineKeepReservation
	}
	if e.Outcome == PipelinePublishReservation && pipelineHash(e.ProofSHA256) {
		return PipelinePublishReservation
	}
	if e.Outcome == PipelineReleaseReservation && e.ProofSHA256 == "" {
		return PipelineReleaseReservation
	}
	return PipelineKeepReservation
}

type PGPipelineEffects struct{ authority PipelineAuthority }

func NewPGPipelineEffects(authority PipelineAuthority) (*PGPipelineEffects, error) {
	if authority == nil {
		return nil, ErrPipelineAuthority
	}
	return &PGPipelineEffects{authority: authority}, nil
}
