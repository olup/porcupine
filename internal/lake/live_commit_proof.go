package lake

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"
)

const liveProofVersion = 1
const liveProofColumns = `proof_version,effect_seq,batch_id,batch_fingerprint,proof_fingerprint,tenant_id,fence_epoch,accounted_at,accepted_live,plan_id,manifest_key,manifest_version,manifest_sha256,manifest_bytes,output_count,totals_json`

func liveEvidenceFingerprint(e LiveCommitEvidence) (string, error) {
	// This v1 codec is independent of future additions to the result type.
	raw, err := json.Marshal(struct {
		Version                   int
		Scope                     HostedProjectScope
		TenantID                  string
		Sequence                  int64
		BatchID, BatchFingerprint string
		FenceEpoch                int64
		AccountedAt               string
		AcceptedLive              int64
		Effects                   StagedLiveEffects
		Totals                    []NativeSourceTotals
	}{liveProofVersion, e.Scope, e.TenantID, e.Sequence, e.BatchID, e.BatchFingerprint, e.FenceEpoch, e.AccountedAt.UTC().Format(time.RFC3339Nano), e.AcceptedLive, e.Effects, e.Totals})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}
func insertLiveCommitProof(ctx context.Context, tx *sql.Tx, e LiveCommitEvidence) error {
	totals, err := json.Marshal(e.Totals)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO lake.live_commit_proofs (`+liveProofColumns+`) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, liveProofVersion, e.Sequence, e.BatchID, e.BatchFingerprint, e.ProofFingerprint, e.TenantID, e.FenceEpoch, e.AccountedAt.UTC().Format(time.RFC3339Nano), e.AcceptedLive, e.Effects.PlanID, e.Effects.Manifest.Key, e.Effects.Manifest.VersionID, e.Effects.Manifest.SHA256, e.Effects.Manifest.Bytes, e.Effects.OutputCount, string(totals))
	return err
}

type liveProofScanner interface{ Scan(...any) error }

func scanLiveCommitProof(row liveProofScanner, scope HostedProjectScope) (LiveCommitEvidence, error) {
	e := LiveCommitEvidence{Scope: scope}
	var version int
	var accounted, totals string
	err := row.Scan(&version, &e.Sequence, &e.BatchID, &e.BatchFingerprint, &e.ProofFingerprint, &e.TenantID, &e.FenceEpoch, &accounted, &e.AcceptedLive, &e.Effects.PlanID, &e.Effects.Manifest.Key, &e.Effects.Manifest.VersionID, &e.Effects.Manifest.SHA256, &e.Effects.Manifest.Bytes, &e.Effects.OutputCount, &totals)
	if err != nil {
		return LiveCommitEvidence{}, err
	}
	if version != liveProofVersion || e.Sequence <= 0 || e.AcceptedLive <= 0 || e.FenceEpoch <= 0 || e.BatchID == "" || e.TenantID == "" {
		return LiveCommitEvidence{}, errors.New("invalid or unsupported native commit proof")
	}
	e.AccountedAt, err = time.Parse(time.RFC3339Nano, accounted)
	if err != nil {
		return LiveCommitEvidence{}, errors.New("invalid native commit accounting time")
	}
	if err := json.Unmarshal([]byte(totals), &e.Totals); err != nil {
		return LiveCommitEvidence{}, errors.New("invalid native cumulative usage proof")
	}
	fingerprint, err := liveEvidenceFingerprint(e)
	if err != nil {
		return LiveCommitEvidence{}, err
	}
	if fingerprint != e.ProofFingerprint {
		return LiveCommitEvidence{}, errors.New("native commit proof fingerprint mismatch")
	}
	return e, nil
}
func readLiveCommitProof(ctx context.Context, tx *sql.Tx, scope HostedProjectScope, batchID string) (LiveCommitEvidence, bool, error) {
	var count int
	if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM lake.live_commit_proofs WHERE batch_id=?", batchID).Scan(&count); err != nil {
		return LiveCommitEvidence{}, false, err
	}
	if count == 0 {
		return LiveCommitEvidence{}, false, nil
	}
	if count != 1 {
		return LiveCommitEvidence{}, false, errors.New("native batch proof is not unique")
	}
	e, err := scanLiveCommitProof(tx.QueryRowContext(ctx, "SELECT "+liveProofColumns+" FROM lake.live_commit_proofs WHERE batch_id=?", batchID), scope)
	return e, err == nil, err
}

type LiveEvidencePage struct {
	Evidence            []LiveCommitEvidence
	NativeHighWater     int64
	AcknowledgedThrough int64
}

var ErrNativeEvidenceGap = errors.New("native effect history does not cover the requested publication checkpoint")

// PendingLiveCommitEffects is available to recovery workers after admission is
// fenced. Physical catalog removal must wait for its final sequence to publish.
func (s *DuckLakeStore) PendingLiveCommitEffects(ctx context.Context, scope HostedProjectScope, after int64, limit int) (LiveEvidencePage, error) {
	if scope.Key.SourceID != s.Config.SharedSourceID || scope.StreamID == "" || after < 0 || limit <= 0 || limit > 1000 {
		return LiveEvidencePage{}, errors.New("invalid native effect recovery scope or page")
	}
	p, release, err := s.leaseHostedProjectExisting(ctx, scope.Key.ProjectID)
	if err != nil {
		return LiveEvidencePage{}, err
	}
	defer release()
	tx, err := p.DB.BeginTx(ctx, nil)
	if err != nil {
		return LiveEvidencePage{}, err
	}
	defer tx.Rollback()
	var page LiveEvidencePage
	if err := tx.QueryRowContext(ctx, `SELECT CAST(next_effect_seq AS BIGINT),CAST(ack_effect_seq AS BIGINT) FROM lake.hosted_write_guard WHERE id=1 AND source_id=? AND project_id=? AND catalog_id=? AND stream_id=?`, scope.Key.SourceID, scope.Key.ProjectID, scope.CatalogID, scope.StreamID).Scan(&page.NativeHighWater, &page.AcknowledgedThrough); err != nil {
		return LiveEvidencePage{}, err
	}
	if after > page.NativeHighWater || page.AcknowledgedThrough < 0 || page.AcknowledgedThrough > page.NativeHighWater {
		return LiveEvidencePage{}, ErrNativeEvidenceGap
	}
	if err := validateNativeEffectHistory(ctx, tx, page.NativeHighWater); err != nil {
		return LiveEvidencePage{}, err
	}
	rows, err := tx.QueryContext(ctx, "SELECT "+liveProofColumns+" FROM lake.live_commit_proofs WHERE effect_seq>? ORDER BY effect_seq LIMIT ?", after, limit)
	if err != nil {
		return LiveEvidencePage{}, err
	}
	last := after
	for rows.Next() {
		e, err := scanLiveCommitProof(rows, scope)
		if err != nil {
			rows.Close()
			return LiveEvidencePage{}, err
		}
		if e.Sequence <= last || e.Sequence-last != 1 || e.Sequence > page.NativeHighWater {
			rows.Close()
			return LiveEvidencePage{}, ErrNativeEvidenceGap
		}
		page.Evidence = append(page.Evidence, e)
		last = e.Sequence
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return LiveEvidencePage{}, err
	}
	if len(page.Evidence) < limit && last != page.NativeHighWater {
		return LiveEvidencePage{}, ErrNativeEvidenceGap
	}
	return page, nil
}
