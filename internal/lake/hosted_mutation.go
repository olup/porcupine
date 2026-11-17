package lake

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
)

var ErrHostedMutationConflict = errors.New("native mutation input conflicts with the original operation")

type HostedMutationInput struct {
	Scope            HostedProjectScope
	TenantID         string
	MutationID       string
	ExpectedRevision uint64
	Projection       EventProjection
}

type HostedMutationOutcome struct {
	Revision  uint64
	Deleted   bool
	ErrorCode string
}

func (o HostedMutationOutcome) valid() bool {
	return o.Revision > 0 && o.Revision <= math.MaxInt64 && (o.ErrorCode == "" || o.ErrorCode == "head_conflict")
}

// HostedMutationFingerprint identifies the exact immutable intent bytes.
func HostedMutationFingerprint(input HostedMutationInput) (string, error) {
	raw, err := json.Marshal(input)
	if err != nil {
		return "", err
	}
	return nativeDigest(raw), nil
}

// LoadHostedEvent reads the existing catalog without creating it on a cold worker.
func (s *DuckLakeStore) LoadHostedEvent(ctx context.Context, scope HostedProjectScope, uuid string) (EventProjection, error) {
	p, release, err := s.leaseHostedProjectExisting(ctx, scope.Key.ProjectID)
	if err != nil {
		return EventProjection{}, err
	}
	defer release()
	tx, err := p.DB.BeginTx(ctx, nil)
	if err != nil {
		return EventProjection{}, err
	}
	defer tx.Rollback()
	actual, err := readHostedProjectScope(ctx, tx, scope.Key)
	if err != nil {
		return EventProjection{}, err
	}
	if actual != scope {
		return EventProjection{}, ErrProjectWriteFenceStale
	}
	return loadEventProjection(ctx, tx, scope.Key.ProjectID, uuid)
}

// CommitHostedMutation records the original outcome and logical projection in
// the common native transaction. The caller must own a durably Ready intent.
func (s *DuckLakeStore) CommitHostedMutation(ctx context.Context, fence ProjectWriteFence, input HostedMutationInput) (NativeCommitEvidence, error) {
	scope, projection := input.Scope, input.Projection
	if s.SharedCatalog == nil || !validProjectWriteFence(fence) || fence.Key != scope.Key || fence.StreamID != scope.StreamID || scope.CatalogID != SharedNativeCatalogID(scope.Key.SourceID, scope.Key.ProjectID) || fence.Key.SourceID != s.Config.SharedSourceID {
		return NativeCommitEvidence{}, ErrProjectWriteFenceStale
	}
	if input.MutationID == "" || len(input.MutationID) > 200 || input.TenantID == "" || projection.ProjectID != scope.Key.ProjectID || !ValidStoredEventUUID(projection.UUID) || projection.Revision <= input.ExpectedRevision || projection.Revision > math.MaxInt64 || projection.IdentitySnapshot != "" {
		return NativeCommitEvidence{}, errors.New("invalid native mutation input")
	}
	prepared, err := PrepareLakeProjection(projection)
	if err != nil {
		return NativeCommitEvidence{}, err
	}
	fingerprint, err := HostedMutationFingerprint(input)
	if err != nil {
		return NativeCommitEvidence{}, err
	}
	p, release, err := s.leaseHostedProjectExisting(ctx, scope.Key.ProjectID)
	if err != nil {
		return NativeCommitEvidence{}, err
	}
	defer release()
	if err := p.write.Lock(ctx); err != nil {
		return NativeCommitEvidence{}, err
	}
	defer p.write.Unlock()

	var result NativeCommitEvidence
	err = retryNativeTransaction(ctx, func() error {
		tx, err := p.DB.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		var version int
		if err := tx.QueryRowContext(ctx, "SELECT version FROM lake.schema_version").Scan(&version); err != nil {
			return err
		}
		if version != hostedLakeSchemaVersion {
			return ErrHostedProjectNotReady
		}
		prior, found, err := readNativeCommitProof(ctx, tx, scope, input.MutationID)
		if err != nil {
			return err
		}
		if found {
			if prior.Kind != NativeCommitMutation || prior.InputFingerprint != fingerprint || prior.TenantID != input.TenantID {
				return ErrHostedMutationConflict
			}
			result = prior
			return nil
		}
		sequence, err := p.beginHostedIngestionWrite(ctx, tx, fence, input.TenantID)
		if err != nil {
			return err
		}
		old, err := loadEventProjection(ctx, tx, scope.Key.ProjectID, projection.UUID)
		if err != nil && !errors.Is(err, ErrEventNotFound) {
			return err
		}
		outcome := HostedMutationOutcome{Revision: projection.Revision, Deleted: projection.IsDeleted}
		if errors.Is(err, ErrEventNotFound) || old.IsDeleted || old.IdentitySnapshot != "" || old.ID != projection.ID || old.Revision != input.ExpectedRevision || old.ReceivedAt != prepared.projection.ReceivedAt {
			outcome.ErrorCode = "head_conflict"
		} else if err := applyHostedMutationProjection(ctx, tx, old, prepared); err != nil {
			return err
		}
		at := s.liveCommitTime()
		totals, err := updateNativeSourceTotals(ctx, tx, sequence, at, 0, 0)
		if err != nil {
			return err
		}
		result = NativeCommitEvidence{Kind: NativeCommitMutation, Scope: scope, TenantID: input.TenantID, Sequence: sequence, OperationID: input.MutationID, InputFingerprint: fingerprint, FenceEpoch: fence.Epoch, AccountedAt: at, Mutation: &outcome, Totals: totals}
		result.ProofFingerprint, err = nativeEvidenceFingerprint(result)
		if err != nil {
			return err
		}
		if err := insertNativeCommitProof(ctx, tx, result); err != nil {
			return err
		}
		return tx.Commit()
	})
	if err != nil {
		return NativeCommitEvidence{}, err
	}
	return result, nil
}

func applyHostedMutationProjection(ctx context.Context, tx *sql.Tx, old EventProjection, edited PreparedLakeProjection) error {
	row := edited.row()
	if row.Deleted {
		row.EventTime, row.TimestampMS = "1970-01-01T00:00:00Z", 0
	} else {
		original := old.OriginalSnapshot
		if original == "" {
			var err error
			original, err = OriginalSnapshot(old.Event)
			if err != nil {
				return err
			}
		}
		if row.OriginalSnapshot != original || row.SourceID != old.SourceID || row.MessageID != old.MessageID || row.EventType != old.EventType || row.Imported != boolByte(old.Imported) || !bytes.Equal(edited.projection.Context, old.Context) {
			return errors.New("native mutation changed immutable event metadata")
		}
	}
	raw, err := json.Marshal([]lakeProjectionRow{row})
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, lakeStageProjections, string(raw)); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `DELETE FROM lake.current_events WHERE uuid IN (SELECT uuid FROM _pc_incoming);
DELETE FROM lake.heads WHERE uuid IN (SELECT uuid FROM _pc_incoming);
INSERT INTO lake.heads (uuid,event_id,revision,is_deleted,received_at) SELECT uuid,id,revision,is_deleted,received_at FROM _pc_incoming;
INSERT INTO lake.current_events (id,project_id,uuid,event,distinct_id,properties,timestamp,timestamp_ms,event_time,received_at,received_at_ms,revision,source_id,message_id,event_type,context,is_imported,identity_snapshot,original_snapshot)
 SELECT id,project_id,uuid,event,distinct_id,properties,timestamp,timestamp_ms,event_time,received_at,received_at_ms,revision,source_id,message_id,event_type,context,is_imported,identity_snapshot,original_snapshot FROM _pc_incoming WHERE NOT is_deleted;
DROP TABLE _pc_incoming`)
	return err
}
