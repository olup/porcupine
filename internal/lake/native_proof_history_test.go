package lake

import (
	"errors"
	"testing"
	"time"
)

func TestDuckLakeS3SharedPostgresNativeProofHistorySnapshots(t *testing.T) {
	t.Parallel()
	ctx, a, b, config := sharedNativeStores(t)
	_, coordinator := nativeFenceControl(t)

	for _, corruption := range []string{"replace old sequence with a duplicate", "append valid tail and duplicate old sequence"} {
		t.Run(corruption, func(t *testing.T) {
			const high = int64(6)
			const tenant = "history-tenant"
			at := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
			key := ProjectWriteKey{SourceID: config.SharedSourceID, ProjectID: corruption}
			scope, err := a.InitializeHostedProject(ctx, key)
			if err != nil {
				t.Fatal(err)
			}
			session := acquireFenceSession(t, coordinator, key)
			fence := session.Fence(scope.StreamID)
			if err := a.AdvanceWriteFence(ctx, fence); err != nil {
				t.Fatal(err)
			}
			seedNativeProofHistory(t, a.Projects[key.ProjectID], scope, fence, tenant, at, high, 3)

			page, err := b.PendingNativeEffects(ctx, scope, high, 1)
			if err != nil || page.NativeHighWater != high || page.AcknowledgedThrough != high || len(page.Evidence) != 0 {
				t.Fatal(page, err)
			}
			p := b.Projects[key.ProjectID]
			prior := p.nativeProofHistory.Load()
			if prior == nil || prior.high != high {
				t.Fatal("complete audit did not record its snapshot", prior)
			}

			reader, err := p.DB.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Rollback()
			var pinned int64
			if err := reader.QueryRowContext(ctx, "SELECT id FROM ducklake_current_snapshot('lake')").Scan(&pinned); err != nil || pinned != prior.snapshot {
				t.Fatal(pinned, err)
			}

			writer := a.Projects[key.ProjectID]
			tx, err := writer.DB.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			next := high + 1
			original, err := scanNativeCommitProof(tx.QueryRowContext(ctx, "SELECT "+nativeProofColumns+" FROM lake.native_commit_proofs WHERE effect_seq=?", high), scope)
			if err != nil {
				t.Fatal(err)
			}
			original.Sequence, original.Totals[0].Revision, original.Totals[0].Live = next, next, next
			original.OperationID = "appended"
			original.ProofFingerprint, err = nativeEvidenceFingerprint(original)
			if err != nil {
				t.Fatal(err)
			}
			if err := insertNativeCommitProof(ctx, tx, original); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.ExecContext(ctx, `UPDATE lake.hosted_write_guard SET generation=generation+1,next_effect_seq=? WHERE id=1`, next); err != nil {
				t.Fatal(err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			if err := p.validateNativeEffectHistory(ctx, reader, high); err != nil {
				t.Fatal("an older reader advanced past its pinned snapshot", err)
			}
			if err := reader.Rollback(); err != nil {
				t.Fatal(err)
			}

			page, err = b.PendingNativeEffects(ctx, scope, high, 1)
			if err != nil || page.NativeHighWater != next || len(page.Evidence) != 1 || page.Evidence[0].ProofFingerprint != original.ProofFingerprint {
				t.Fatal(page, err)
			}
			appended := p.nativeProofHistory.Load()
			if appended.snapshot <= prior.snapshot || appended.high != next {
				t.Fatal("append did not advance the checked snapshot", appended)
			}
			if _, valid, inserted := p.nativeProofHistoryChanges(ctx, prior.snapshot, appended.snapshot, prior); !valid || !inserted {
				t.Fatal("normal guard and proof commit did not qualify for append validation")
			}

			if err := b.AcknowledgeNativeEffects(ctx, fence, scope, next); err != nil {
				t.Fatal(err)
			}
			page, err = b.PendingNativeEffects(ctx, scope, next, 1)
			if err != nil || page.AcknowledgedThrough != next || len(page.Evidence) != 0 {
				t.Fatal(page, err)
			}
			acknowledged := p.nativeProofHistory.Load()
			if _, valid, inserted := p.nativeProofHistoryChanges(ctx, appended.snapshot, acknowledged.snapshot, appended); !valid || inserted {
				t.Fatal("a guard-only ACK did not qualify to reuse the checked history")
			}

			corrupt, err := writer.DB.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer corrupt.Rollback()
			// Preserve both end points and, in the replacement case, the total
			// row count. A tail lookup cannot see either invalid prior sequence.
			if _, err := corrupt.ExecContext(ctx, `INSERT INTO lake.native_commit_proofs SELECT * FROM lake.native_commit_proofs WHERE effect_seq=2`); err != nil {
				t.Fatal(err)
			}
			if corruption == "replace old sequence with a duplicate" {
				if _, err := corrupt.ExecContext(ctx, `DELETE FROM lake.native_commit_proofs WHERE effect_seq=3`); err != nil {
					t.Fatal(err)
				}
			} else {
				next++
				original.Sequence, original.Totals[0].Revision, original.Totals[0].Live = next, next, next
				original.OperationID = "another-append"
				original.ProofFingerprint, err = nativeEvidenceFingerprint(original)
				if err != nil {
					t.Fatal(err)
				}
				if err := insertNativeCommitProof(ctx, corrupt, original); err != nil {
					t.Fatal(err)
				}
				if _, err := corrupt.ExecContext(ctx, `UPDATE lake.hosted_write_guard SET generation=generation+1,next_effect_seq=? WHERE id=1`, next); err != nil {
					t.Fatal(err)
				}
			}
			if err := corrupt.Commit(); err != nil {
				t.Fatal(err)
			}
			if _, err := b.PendingNativeEffects(ctx, scope, next, 1); !errors.Is(err, ErrNativeEvidenceGap) {
				t.Fatal("covered cursor hid stale retained history", err)
			}
			if err := b.AcknowledgeNativeEffects(ctx, fence, scope, next); !errors.Is(err, ErrNativeEvidenceGap) {
				t.Fatal("ACK accepted a duplicate sequence", err)
			}
			if _, err := b.SealHostedProject(ctx, fence); !errors.Is(err, ErrNativeEvidenceGap) {
				t.Fatal("seal accepted corrupt history", err)
			}
			if got, err := readHostedProjectScope(ctx, p.DB, key); err != nil || got != scope {
				t.Fatal("failed seal changed the stream", got, err)
			}
			if mark := p.nativeProofHistory.Load(); mark != acknowledged {
				t.Fatal("corrupt history advanced the trusted snapshot", mark)
			}
		})
	}
}
