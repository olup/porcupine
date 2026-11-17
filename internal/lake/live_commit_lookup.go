package lake

import (
	"context"
	"errors"
)

// FindLiveCommitBatch reads a complete original stream view. A miss alone does
// not authorize settlement; callers must first commit their successor fence.
// A found proof must match the same validated input, excluding candidate IDs.
func (s *DuckLakeStore) FindLiveCommitBatch(ctx context.Context, fence ProjectWriteFence, batch LiveCommitBatch) (NativeCommitEvidence, bool, error) {
	fingerprint, err := validateLiveCommitBatch(fence, batch, EventIDReservation{fence: fence, batchID: batch.ID, ids: make([]int64, len(batch.Inputs))})
	if err != nil {
		return NativeCommitEvidence{}, false, err
	}
	p, release, err := s.leaseHostedProjectExisting(ctx, batch.Scope.Key.ProjectID)
	if err != nil {
		return NativeCommitEvidence{}, false, err
	}
	defer release()
	tx, err := p.DB.BeginTx(ctx, nil)
	if err != nil {
		return NativeCommitEvidence{}, false, err
	}
	defer tx.Rollback()
	scope, err := readHostedProjectScopeState(ctx, tx, batch.Scope.Key, true)
	if err != nil {
		return NativeCommitEvidence{}, false, err
	}
	if scope != batch.Scope {
		return NativeCommitEvidence{}, false, ErrProjectWriteFenceStale
	}
	var high int64
	if err = tx.QueryRowContext(ctx, `SELECT CAST(next_effect_seq AS BIGINT) FROM lake.hosted_write_guard WHERE id=1`).Scan(&high); err != nil {
		return NativeCommitEvidence{}, false, err
	}
	if err = validateNativeEffectHistory(ctx, tx, high); err != nil {
		return NativeCommitEvidence{}, false, err
	}
	proof, found, err := readNativeCommitProof(ctx, tx, scope, batch.ID)
	if err == nil && found && (proof.Kind != NativeCommitLive || proof.InputFingerprint != fingerprint || proof.TenantID != batch.TenantID) {
		err = errors.New("native batch identity reused with different input")
	}
	return proof, found, err
}
