package lake

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
)

const nativeProofVersion = 1
const nativeProofColumns = `proof_version,effect_seq,operation_id,input_fingerprint,proof_fingerprint,kind,evidence_json`
const maxNativeProofBytes = 2 << 20

func nativeEvidenceFingerprint(e NativeCommitEvidence) (string, error) {
	e.ProofFingerprint = ""
	raw, err := json.Marshal(struct {
		Version  int
		Evidence NativeCommitEvidence
	}{nativeProofVersion, e})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}

// ValidateNativeCommitEvidence checks the common envelope and its implemented
// kind. Recovery uses this original native outcome without producer authority.
func ValidateNativeCommitEvidence(e NativeCommitEvidence) error {
	if e.Sequence <= 0 || e.FenceEpoch <= 0 || e.OperationID == "" || len(e.OperationID) > 200 || e.TenantID == "" || e.AccountedAt.IsZero() || e.Scope.Key.SourceID == "" || e.Scope.Key.ProjectID == "" || e.Scope.CatalogID != SharedNativeCatalogID(e.Scope.Key.SourceID, e.Scope.Key.ProjectID) || e.Scope.StreamID == "" || !validNativeHash(e.InputFingerprint) || len(e.Totals) != 1 {
		return errors.New("invalid native commit envelope")
	}
	total := e.Totals[0]
	if total.Day != e.AccountedAt.UTC().Format("2006-01-02T00:00:00Z") || total.Revision != e.Sequence || e.AcceptedLive < 0 || e.AcceptedImport < 0 || total.Live < e.AcceptedLive || total.Import < e.AcceptedImport {
		return errors.New("invalid native cumulative usage proof")
	}
	switch e.Kind {
	case NativeCommitLive:
		if e.AcceptedLive <= 0 || e.AcceptedImport != 0 || e.Import != nil || e.Effects.PlanID == "" || e.Effects.OutputCount < 0 {
			return errors.New("invalid native live outcome")
		}
	case NativeCommitImport:
		if e.AcceptedLive != 0 || e.Import == nil || e.Effects != (StagedLiveEffects{}) || e.Import.Delta.Accepted != e.AcceptedImport || !e.Import.AccountedAt.Equal(e.AccountedAt) || e.Import.Key.Scope != e.Scope || e.Import.Key.ID() != e.OperationID {
			return errors.New("invalid native import outcome")
		}
		if err := validateImportOutcome(*e.Import); err != nil {
			return err
		}
	default:
		return errors.New("unsupported native commit kind")
	}
	fingerprint, err := nativeEvidenceFingerprint(e)
	if err != nil {
		return err
	}
	if fingerprint != e.ProofFingerprint {
		return errors.New("native commit proof fingerprint mismatch")
	}
	return nil
}
func validNativeHash(s string) bool {
	decoded, err := hex.DecodeString(s)
	return err == nil && len(decoded) == sha256.Size && s == hex.EncodeToString(decoded)
}
func insertNativeCommitProof(ctx context.Context, tx *sql.Tx, e NativeCommitEvidence) error {
	if err := ValidateNativeCommitEvidence(e); err != nil {
		return err
	}
	raw, err := json.Marshal(e)
	if err != nil {
		return err
	}
	if len(raw) > maxNativeProofBytes {
		return errors.New("native proof exceeds bound")
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO lake.native_commit_proofs (`+nativeProofColumns+`) VALUES(?,?,?,?,?,?,?)`, nativeProofVersion, e.Sequence, e.OperationID, e.InputFingerprint, e.ProofFingerprint, string(e.Kind), string(raw))
	return err
}

type nativeProofScanner interface{ Scan(...any) error }

func scanNativeCommitProof(row nativeProofScanner, scope HostedProjectScope) (NativeCommitEvidence, error) {
	var version int
	var sequence int64
	var id, input, fingerprint, kind, raw string
	if err := row.Scan(&version, &sequence, &id, &input, &fingerprint, &kind, &raw); err != nil {
		return NativeCommitEvidence{}, err
	}
	if version != nativeProofVersion || len(raw) > maxNativeProofBytes {
		return NativeCommitEvidence{}, errors.New("invalid or unsupported native commit proof")
	}
	var e NativeCommitEvidence
	if err := json.Unmarshal([]byte(raw), &e); err != nil {
		return e, err
	}
	if e.Scope != scope || e.Sequence != sequence || e.OperationID != id || e.InputFingerprint != input || e.ProofFingerprint != fingerprint || string(e.Kind) != kind {
		return NativeCommitEvidence{}, errors.New("native proof envelope mismatch")
	}
	return e, ValidateNativeCommitEvidence(e)
}
func readNativeCommitProof(ctx context.Context, tx *sql.Tx, scope HostedProjectScope, operationID string) (NativeCommitEvidence, bool, error) {
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM lake.native_commit_proofs WHERE operation_id=?`, operationID).Scan(&count); err != nil {
		return NativeCommitEvidence{}, false, err
	}
	if count == 0 {
		return NativeCommitEvidence{}, false, nil
	}
	if count != 1 {
		return NativeCommitEvidence{}, false, errors.New("native operation proof is not unique")
	}
	e, err := scanNativeCommitProof(tx.QueryRowContext(ctx, `SELECT `+nativeProofColumns+` FROM lake.native_commit_proofs WHERE operation_id=?`, operationID), scope)
	return e, err == nil, err
}

type NativeEvidencePage struct {
	Evidence            []NativeCommitEvidence
	NativeHighWater     int64
	AcknowledgedThrough int64
}

var ErrNativeEvidenceGap = errors.New("native effect history does not cover the requested publication checkpoint")

// PendingNativeEffects is available to recovery workers after admission is
// fenced. Physical catalog removal must wait for its final sequence to publish.
func (s *DuckLakeStore) PendingNativeEffects(ctx context.Context, scope HostedProjectScope, after int64, limit int) (NativeEvidencePage, error) {
	if scope.Key.SourceID != s.Config.SharedSourceID || scope.StreamID == "" || after < 0 || limit <= 0 || limit > 1000 {
		return NativeEvidencePage{}, errors.New("invalid native effect recovery scope or page")
	}
	p, release, err := s.leaseHostedProjectExisting(ctx, scope.Key.ProjectID)
	if err != nil {
		return NativeEvidencePage{}, err
	}
	defer release()
	tx, err := p.DB.BeginTx(ctx, nil)
	if err != nil {
		return NativeEvidencePage{}, err
	}
	defer tx.Rollback()
	var page NativeEvidencePage
	if err := tx.QueryRowContext(ctx, `SELECT CAST(next_effect_seq AS BIGINT),CAST(ack_effect_seq AS BIGINT) FROM lake.hosted_write_guard WHERE id=1 AND source_id=? AND project_id=? AND catalog_id=? AND stream_id=?`, scope.Key.SourceID, scope.Key.ProjectID, scope.CatalogID, scope.StreamID).Scan(&page.NativeHighWater, &page.AcknowledgedThrough); err != nil {
		return NativeEvidencePage{}, err
	}
	if after > page.NativeHighWater || page.AcknowledgedThrough < 0 || page.AcknowledgedThrough > page.NativeHighWater {
		return NativeEvidencePage{}, ErrNativeEvidenceGap
	}
	if err := validateNativeEffectHistory(ctx, tx, page.NativeHighWater); err != nil {
		return NativeEvidencePage{}, err
	}
	rows, err := tx.QueryContext(ctx, "SELECT "+nativeProofColumns+" FROM lake.native_commit_proofs WHERE effect_seq>? ORDER BY effect_seq LIMIT ?", after, limit)
	if err != nil {
		return NativeEvidencePage{}, err
	}
	last := after
	for rows.Next() {
		e, err := scanNativeCommitProof(rows, scope)
		if err != nil {
			rows.Close()
			return NativeEvidencePage{}, err
		}
		if e.Sequence <= last || e.Sequence-last != 1 || e.Sequence > page.NativeHighWater {
			rows.Close()
			return NativeEvidencePage{}, ErrNativeEvidenceGap
		}
		page.Evidence = append(page.Evidence, e)
		last = e.Sequence
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return NativeEvidencePage{}, err
	}
	if len(page.Evidence) < limit && last != page.NativeHighWater {
		return NativeEvidencePage{}, ErrNativeEvidenceGap
	}
	return page, nil
}
