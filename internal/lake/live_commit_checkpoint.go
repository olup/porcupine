package lake

import (
	"context"
	"database/sql"
	"errors"
	"math"
)

func verifyLivePredecessor(ctx context.Context, tx *sql.Tx, scope HostedProjectScope, checkpoint NativeEffectCheckpoint) error {
	var high, ack int64
	err := tx.QueryRowContext(ctx, `SELECT CAST(next_effect_seq AS BIGINT),CAST(ack_effect_seq AS BIGINT)
FROM lake.hosted_write_guard WHERE id=1 AND source_id=? AND project_id=? AND catalog_id=? AND stream_id=?`,
		scope.Key.SourceID, scope.Key.ProjectID, scope.CatalogID, scope.StreamID).Scan(&high, &ack)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrProjectWriteFenceStale
	}
	if err != nil {
		return err
	}
	if checkpoint.Sequence < 0 || checkpoint.Sequence > high || ack < 0 || ack > high {
		return ErrNativeEvidenceGap
	}
	if checkpoint.Sequence < high {
		return ErrNativeEffectsPending
	}
	if checkpoint.Sequence == 0 {
		if checkpoint.Fingerprint != "" {
			return ErrNativeEvidenceGap
		}
		return nil
	}
	rows, err := tx.QueryContext(ctx, "SELECT "+nativeProofColumns+" FROM lake.native_commit_proofs WHERE effect_seq=? LIMIT 2", checkpoint.Sequence)
	if err != nil {
		return err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return err
		}
		return ErrNativeEvidenceGap
	}
	proof, err := scanNativeCommitProof(rows, scope)
	if err != nil {
		return err
	}
	if proof.ProofFingerprint != checkpoint.Fingerprint || rows.Next() {
		return ErrNativeEvidenceGap
	}
	return rows.Err()
}

func beginCheckpointedLiveWrite(ctx context.Context, tx *sql.Tx, fence ProjectWriteFence, batch LiveCommitBatch, checkpoint NativeEffectCheckpoint) (int64, error) {
	if checkpoint.Sequence == math.MaxInt64 {
		return 0, errors.New("native effect sequence exhausted")
	}
	// Every hosted writer rewrites the same guard row. If the previous
	// acquisition commits after our read, DuckLake rejects this transaction.
	err := nativeUpdateExactlyOne(ctx, tx, `UPDATE lake.hosted_write_guard
SET fence_epoch=?,fence_nonce=?,generation=generation+1,next_effect_seq=next_effect_seq+1,ack_effect_seq=?,tenant_id=coalesce(tenant_id,?)
WHERE id=1 AND source_id=? AND project_id=? AND catalog_id=? AND stream_id=? AND identity_ready AND state='open'
AND (fence_epoch<? OR (fence_epoch=? AND fence_nonce=?)) AND next_effect_seq=? AND (tenant_id IS NULL OR tenant_id=?)`,
		fence.Epoch, fence.AcquisitionID, checkpoint.Sequence, batch.TenantID, fence.Key.SourceID, fence.Key.ProjectID,
		batch.Scope.CatalogID, fence.StreamID, fence.Epoch, fence.Epoch, fence.AcquisitionID, checkpoint.Sequence, batch.TenantID)
	if err != nil {
		return 0, err
	}
	if err := checkNativeProjectNotDeleted(ctx, tx); err != nil {
		return 0, err
	}
	return checkpoint.Sequence + 1, nil
}
