package lake

import (
	"context"
	"database/sql"
	"errors"
)

var ErrHostedProjectNotReady = errors.New("existing hosted project is not ready for recovery")

func (s *DuckLakeStore) existingHostedRecord(ctx context.Context, project string) (nativeProjectRecord, error) {
	if s.SharedCatalog == nil {
		return nativeProjectRecord{}, ErrHostedProjectNotReady
	}
	record, err := s.registry.lookup(ctx, project)
	if err != nil {
		return nativeProjectRecord{}, err
	}
	if record.Deleted {
		return nativeProjectRecord{}, errSharedNativeDeleted
	}
	if !record.Ready {
		return nativeProjectRecord{}, ErrHostedProjectNotReady
	}
	return record, nil
}

func (s *DuckLakeStore) leaseHostedProjectExisting(ctx context.Context, project string) (*ProjectLake, func(), error) {
	if project == "" || len(project) > 1000 {
		return nil, nil, ErrHostedProjectNotReady
	}
	if err := s.Mu.Lock(ctx); err != nil {
		return nil, nil, err
	}
	defer s.Mu.Unlock()
	return s.leaseProjectModeLocked(ctx, project, true)
}

// LookupHostedProjectExisting never bootstraps a registry record, schema, or
// stream. Sealed streams remain discoverable until their catalog is removed.
func (s *DuckLakeStore) LookupHostedProjectExisting(ctx context.Context, key ProjectWriteKey) (HostedProjectScope, error) {
	if key.SourceID != s.Config.SharedSourceID || s.SharedCatalog == nil {
		return HostedProjectScope{}, ErrProjectWriteFenceStale
	}
	p, release, err := s.leaseHostedProjectExisting(ctx, key.ProjectID)
	if err != nil {
		return HostedProjectScope{}, err
	}
	defer release()
	return readHostedProjectScopeState(ctx, p.DB, key, true)
}

// HostedProjectCatalogAbsent verifies that neither a registry row nor the
// deterministic metadata schema exists. Call with the project coordinator held
// and initialization drained. A failed lookup inside a catalog is not absence.
func (s *DuckLakeStore) HostedProjectCatalogAbsent(ctx context.Context, key ProjectWriteKey) (bool, error) {
	if s.SharedCatalog == nil || key.SourceID != s.Config.SharedSourceID || key.ProjectID == "" {
		return false, ErrProjectWriteFenceStale
	}
	if _, err := s.SourceID(ctx); err != nil {
		return false, err
	}
	var absent bool
	err := s.SharedCatalog.QueryRowContext(ctx, `SELECT NOT EXISTS (
SELECT 1 FROM pc_native_projects WHERE source_id=$1 AND project_id=$2)
AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname=$3)`,
		key.SourceID, key.ProjectID, SharedNativeSchema(SharedNativeCatalogID(key.SourceID, key.ProjectID))).Scan(&absent)
	return absent, err
}

// AdvanceRecoveryFence also accepts a sealed stream. It cannot reopen one or
// authorize event mutations. Call under the already acquired project session.
func (s *DuckLakeStore) AdvanceRecoveryFence(ctx context.Context, fence ProjectWriteFence) error {
	return s.advanceWriteFence(ctx, fence, true)
}

type HostedProjectSeal struct {
	Scope                              HostedProjectScope
	FinalSequence, AcknowledgedThrough int64
}

// SealHostedProject serializes with same-engine writers and commits against the
// same native guard as other engines. Its cut is immutable across retries. This
// proves a native write cut, not that another worker's blocked call has returned.
func (s *DuckLakeStore) SealHostedProject(ctx context.Context, fence ProjectWriteFence) (HostedProjectSeal, error) {
	var result HostedProjectSeal
	if !validProjectWriteFence(fence) || fence.Key.SourceID != s.Config.SharedSourceID {
		return result, ErrProjectWriteFenceStale
	}
	p, release, err := s.leaseHostedProjectExisting(ctx, fence.Key.ProjectID)
	if err != nil {
		return result, err
	}
	defer release()
	if err := p.write.Lock(ctx); err != nil {
		return result, err
	}
	defer p.write.Unlock()
	err = retryNativeTransaction(ctx, func() error {
		tx, err := p.DB.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if err := nativeUpdateExactlyOne(ctx, tx, `UPDATE lake.hosted_write_guard SET generation=generation+1,state='sealed',sealed_effect_seq=coalesce(sealed_effect_seq,next_effect_seq)
WHERE id=1 AND source_id=? AND project_id=? AND stream_id=? AND state IN ('open','sealed') AND identity_ready AND fence_epoch=? AND fence_nonce=?`, fence.Key.SourceID, fence.Key.ProjectID, fence.StreamID, fence.Epoch, fence.AcquisitionID); err != nil {
			return err
		}
		scope, err := readHostedProjectScopeState(ctx, tx, fence.Key, true)
		if err != nil {
			return err
		}
		result.Scope = scope
		var high int64
		if err := tx.QueryRowContext(ctx, `SELECT CAST(sealed_effect_seq AS BIGINT),CAST(next_effect_seq AS BIGINT),CAST(ack_effect_seq AS BIGINT) FROM lake.hosted_write_guard WHERE id=1`).Scan(&result.FinalSequence, &high, &result.AcknowledgedThrough); err != nil {
			return err
		}
		if result.FinalSequence != high || result.AcknowledgedThrough < 0 || result.AcknowledgedThrough > high {
			return ErrNativeEvidenceGap
		}
		if err := auditNativeEffectHistory(ctx, tx, high); err != nil {
			return err
		}
		if err := checkNativeProjectNotDeleted(ctx, tx); err != nil {
			return err
		}
		return tx.Commit()
	})
	if err != nil {
		return HostedProjectSeal{}, err
	}
	return result, nil
}

// AcknowledgeNativeEffects records confirmed external publication coverage only.
// It retains all proofs and tombstones. The caller must first commit its output,
// usage and checkpoint transaction; ACK is not permission to prune or drop.
func (s *DuckLakeStore) AcknowledgeNativeEffects(ctx context.Context, fence ProjectWriteFence, scope HostedProjectScope, through int64) error {
	if !validProjectWriteFence(fence) || scope.Key != fence.Key || scope.StreamID != fence.StreamID || scope.Key.SourceID != s.Config.SharedSourceID || through < 0 {
		return ErrProjectWriteFenceStale
	}
	p, release, err := s.leaseHostedProjectExisting(ctx, scope.Key.ProjectID)
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
		var high, ack int64
		if err := tx.QueryRowContext(ctx, `SELECT CAST(next_effect_seq AS BIGINT),CAST(ack_effect_seq AS BIGINT) FROM lake.hosted_write_guard WHERE id=1 AND source_id=? AND project_id=? AND catalog_id=? AND stream_id=? AND state IN ('open','sealed') AND identity_ready AND fence_epoch=? AND fence_nonce=?`, scope.Key.SourceID, scope.Key.ProjectID, scope.CatalogID, scope.StreamID, fence.Epoch, fence.AcquisitionID).Scan(&high, &ack); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrProjectWriteFenceStale
			}
			return err
		}
		if through > high || ack < 0 || ack > high {
			return ErrNativeEvidenceGap
		}
		if err := p.validateNativeEffectHistory(ctx, tx, high); err != nil {
			return err
		}
		if err := nativeUpdateExactlyOne(ctx, tx, `UPDATE lake.hosted_write_guard SET generation=generation+1,ack_effect_seq=greatest(ack_effect_seq,?) WHERE id=1 AND fence_epoch=? AND fence_nonce=?`, through, fence.Epoch, fence.AcquisitionID); err != nil {
			return err
		}
		if err := checkNativeProjectNotDeleted(ctx, tx); err != nil {
			return err
		}
		return tx.Commit()
	})
}

var ErrHostedNativeGuardRequired = errors.New("hosted native operation requires the common fenced lifecycle and durable outcome protocol")

func rejectHostedLegacyOperation(ctx context.Context, query interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}) error {
	var hosted bool
	if err := query.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM duckdb_tables() WHERE database_name='lake' AND schema_name='main' AND table_name='hosted_write_guard')`).Scan(&hosted); err != nil {
		return err
	}
	if hosted {
		return ErrHostedNativeGuardRequired
	}
	return nil
}
