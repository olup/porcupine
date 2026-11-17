package lake

import (
	"context"
	"database/sql"
	"errors"
)

func validProjectWriteFence(fence ProjectWriteFence) bool {
	return fence.Key.SourceID != "" && fence.Key.ProjectID != "" && fence.StreamID != "" && fence.Epoch > 0 && fence.AcquisitionID != ""
}

// AdvanceWriteFence commits ownership before long native work begins. A delayed
// owner cannot lower an epoch. Equality is accepted only for the same acquisition
// nonce, allowing a retry after a lost response without reusing another owner.
func (s *DuckLakeStore) AdvanceWriteFence(ctx context.Context, fence ProjectWriteFence) error {
	if !validProjectWriteFence(fence) || s.SharedCatalog == nil || fence.Key.SourceID != s.Config.SharedSourceID {
		return ErrProjectWriteFenceStale
	}
	p, release, err := s.LeaseProject(ctx, fence.Key.ProjectID)
	if err != nil {
		return err
	}
	defer release()
	if err := p.write.Lock(ctx); err != nil {
		return err
	}
	defer p.write.Unlock()
	return retryNativeTransaction(ctx, func() error {
		tx, err := p.DB.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if err := nativeUpdateExactlyOne(ctx, tx, `UPDATE lake.hosted_write_guard SET fence_epoch=?,fence_nonce=?,generation=generation+1
 WHERE id=1 AND source_id=? AND project_id=? AND stream_id=? AND state='open' AND identity_ready
 AND (fence_epoch<? OR (fence_epoch=? AND fence_nonce=?))`, fence.Epoch, fence.AcquisitionID, fence.Key.SourceID, fence.Key.ProjectID, fence.StreamID, fence.Epoch, fence.Epoch, fence.AcquisitionID); err != nil {
			return err
		}
		if err := p.acquireNativeWrite(ctx, tx); err != nil {
			return err
		}
		return tx.Commit()
	})
}

// acquireHostedNativeWrite is called in the SAME transaction as event/proof
// changes. A retry keeps this token; it never refreshes an expired owner's epoch.
func (p *ProjectLake) acquireHostedNativeWrite(ctx context.Context, tx *sql.Tx, fence ProjectWriteFence) error {
	if !validProjectWriteFence(fence) || !p.shared || fence.Key.SourceID != p.sharedStore.Config.SharedSourceID || fence.Key.ProjectID != p.projectID {
		return ErrProjectWriteFenceStale
	}
	if err := nativeUpdateExactlyOne(ctx, tx, `UPDATE lake.hosted_write_guard SET generation=generation+1
 WHERE id=1 AND source_id=? AND project_id=? AND stream_id=? AND state='open' AND identity_ready AND fence_epoch=? AND fence_nonce=?`, fence.Key.SourceID, fence.Key.ProjectID, fence.StreamID, fence.Epoch, fence.AcquisitionID); err != nil {
		return err
	}
	return p.acquireNativeWrite(ctx, tx)
}
func nativeUpdateExactlyOne(ctx context.Context, tx *sql.Tx, statement string, args ...any) error {
	result, err := tx.ExecContext(ctx, statement, args...)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return errors.New("native write guard did not report affected rows")
	}
	if count != 1 {
		return ErrProjectWriteFenceStale
	}
	return nil
}
