package lake

import (
	"context"
	"database/sql"
	"strconv"
	"strings"
	"time"
)

const maxNativeProofHistorySnapshotDelta = 64

type nativeProofHistoryMark struct {
	snapshot, high, schemaVersion, tableID int64
	snapshotTime                           time.Time
	tableUUID                              string
}

// validateNativeEffectHistory reuses a complete continuity audit only for the
// same attached catalog and a known committed snapshot. Call before changing
// native_commit_proofs in tx. A restarted or restored engine starts with no mark.
func (p *ProjectLake) validateNativeEffectHistory(ctx context.Context, tx *sql.Tx, high int64) error {
	prior := p.nativeProofHistory.Load()
	var snapshot int64
	query := "SELECT id FROM ducklake_current_snapshot('lake')"
	if prior == nil {
		// The fast path uses DuckLake's PostgreSQL metadata and change-feed
		// semantics as verified on this extension. Other versions can audit.
		query += " WHERE version()='v2.0.0' AND EXISTS (SELECT 1 FROM ducklake_settings('lake') WHERE extension_version='417a9117f5')"
	}
	if err := tx.QueryRowContext(ctx, query).Scan(&snapshot); err != nil {
		return auditNativeEffectHistory(ctx, tx, high)
	}
	if prior != nil && snapshot == prior.snapshot && high == prior.high {
		return nil
	}
	from := snapshot
	if prior != nil && snapshot > prior.snapshot && snapshot-prior.snapshot <= maxNativeProofHistorySnapshotDelta {
		from = prior.snapshot
	}
	current, canExtend, appended := p.nativeProofHistoryChanges(ctx, from, snapshot, prior)
	if canExtend && high >= prior.high && appended == (high > prior.high) {
		if !appended {
			p.rememberNativeProofHistory(current, high)
			return nil
		}
		// A sequence filter alone could miss an extra insertion into the
		// validated prefix. Count every insertion in these snapshots.
		var count, unique int64
		var first, last sql.NullInt64
		err := tx.QueryRowContext(ctx, `SELECT count(*),count(DISTINCT effect_seq),min(effect_seq),max(effect_seq)
FROM ducklake_table_insertions('lake','main','native_commit_proofs',?,?)`, from+1, snapshot).Scan(&count, &unique, &first, &last)
		if err == nil && count == high-prior.high && unique == count && first.Valid && first.Int64 == prior.high+1 && last.Valid && last.Int64 == high {
			p.rememberNativeProofHistory(current, high)
			return nil
		}
	}
	if err := auditNativeEffectHistory(ctx, tx, high); err != nil {
		return err
	}
	p.rememberNativeProofHistory(current, high)
	return nil
}

// DuckLake selects its snapshot inside tx. PostgreSQL can already see a newer
// commit, so read its immutable metadata by the exact snapshot ID range from tx.
// Expired or unrecognized changes cannot certify the prefix. The table UUID
// and snapshot time bind the previous audit to the same metadata history.
func (p *ProjectLake) nativeProofHistoryChanges(ctx context.Context, from, snapshot int64, prior *nativeProofHistoryMark) (current *nativeProofHistoryMark, canExtend, appended bool) {
	if p.sharedStore == nil {
		return nil, false, false
	}
	registry, ok := p.sharedStore.registry.(*postgresNativeRegistry)
	if !ok {
		return nil, false, false
	}
	ctx, cancel := registry.metadataContext(ctx)
	defer cancel()
	schema := SharedNativeSchema(SharedNativeCatalogID(p.sharedStore.Config.SharedSourceID, p.projectID))
	query := strings.ReplaceAll(`WITH proof_table AS (
 SELECT t.table_id,t.table_uuid::text
 FROM meta.ducklake_table t JOIN meta.ducklake_schema sc USING(schema_id)
 WHERE t.table_name='native_commit_proofs' AND sc.schema_name='main'
 AND t.begin_snapshot<=$2 AND (t.end_snapshot IS NULL OR t.end_snapshot>$2)
 AND sc.begin_snapshot<=$2 AND (sc.end_snapshot IS NULL OR sc.end_snapshot>$2)
)
SELECT s.snapshot_id,s.snapshot_time,s.schema_version,p.table_id,p.table_uuid,c.changes_made
FROM meta.ducklake_snapshot s CROSS JOIN proof_table p
LEFT JOIN meta.ducklake_snapshot_changes c ON c.snapshot_id=s.snapshot_id
WHERE s.snapshot_id BETWEEN $1 AND $2 ORDER BY s.snapshot_id`, "meta.", `"`+schema+`".`)
	rows, err := registry.db.QueryContext(ctx, query, from, snapshot)
	if err != nil {
		return nil, false, false
	}
	defer rows.Close()
	canExtend = prior != nil && from == prior.snapshot && from < snapshot
	expected := from
	for rows.Next() {
		var entry nativeProofHistoryMark
		var changes sql.NullString
		if err := rows.Scan(&entry.snapshot, &entry.snapshotTime, &entry.schemaVersion, &entry.tableID, &entry.tableUUID, &changes); err != nil {
			return nil, false, false
		}
		if entry.snapshot != expected || entry.tableUUID == "" || entry.snapshotTime.IsZero() {
			canExtend = false
		}
		expected = entry.snapshot + 1
		if entry.snapshot == snapshot {
			if current != nil {
				return nil, false, false
			}
			current = &entry
		}
		if !canExtend {
			continue
		}
		if entry.schemaVersion != prior.schemaVersion || entry.tableID != prior.tableID || entry.tableUUID != prior.tableUUID {
			canExtend = false
			continue
		}
		if entry.snapshot == from {
			canExtend = entry.snapshotTime.Equal(prior.snapshotTime)
			continue
		}
		inserted, ok := nativeProofAppendChanges(changes, entry.tableID)
		appended = appended || inserted
		canExtend = ok
	}
	if rows.Err() != nil || current == nil {
		return nil, false, false
	}
	return current, canExtend, appended
}

// Snapshot changes are comma-delimited "kind:table_id" records for data
// changes. DDL, changes to the proof table other than an append, and unfamiliar
// records all send the caller to the full audit.
func nativeProofAppendChanges(changes sql.NullString, proofTableID int64) (appended, ok bool) {
	if !changes.Valid || changes.String == "" {
		return false, false
	}
	for change := range strings.SplitSeq(changes.String, ",") {
		kind, rawID, found := strings.Cut(change, ":")
		id, err := strconv.ParseInt(rawID, 10, 64)
		if !found || err != nil || id < 0 {
			return false, false
		}
		switch kind {
		case "inserted_into_table":
			appended = appended || id == proofTableID
		case "deleted_from_table", "inlined_insert", "inlined_delete", "inline_flush", "flushed_inlined",
			"merge_adjacent", "rewrite_delete", "compacted_table":
			if id == proofTableID {
				return false, false
			}
		default:
			return false, false
		}
	}
	return appended, true
}

func (p *ProjectLake) rememberNativeProofHistory(current *nativeProofHistoryMark, high int64) {
	if current == nil {
		return
	}
	current.high = high
	for {
		prior := p.nativeProofHistory.Load()
		if prior != nil && prior.snapshot >= current.snapshot {
			return
		}
		if p.nativeProofHistory.CompareAndSwap(prior, current) {
			return
		}
	}
}

// There is deliberately no pruning floor yet. Until a retained floor and restore
// horizon are proven, every native sequence must still have exactly one proof.
func auditNativeEffectHistory(ctx context.Context, tx *sql.Tx, high int64) error {
	var count, unique, first, last int64
	if err := tx.QueryRowContext(ctx, `SELECT count(*),count(DISTINCT effect_seq),coalesce(min(effect_seq),0),coalesce(max(effect_seq),0) FROM lake.native_commit_proofs`).Scan(&count, &unique, &first, &last); err != nil {
		return err
	}
	if high < 0 || count != high || unique != high || last != high || (high > 0 && first != 1) || (high == 0 && first != 0) {
		return ErrNativeEvidenceGap
	}
	return nil
}
