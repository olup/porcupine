package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

func localReceipt(ctx context.Context, q oauthQueryer, project, uuid string) (bool, error) {
	var found bool
	err := q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM events WHERE project_id=$1 AND uuid=$2) OR EXISTS(SELECT 1 FROM event_receipts WHERE project_id=$3 AND uuid=$4) OR EXISTS(SELECT 1 FROM event_heads WHERE project_id=$5 AND uuid=$6) OR EXISTS(SELECT 1 FROM mutation_outbox WHERE project_id=$7 AND uuid=$8)`, project, uuid, project, uuid, project, uuid, project, uuid).Scan(&found)
	return found, err
}
func localReceiptEventID(ctx context.Context, q oauthQueryer, project, uuid string) (int64, error) {
	var id int64
	err := q.QueryRowContext(ctx, `SELECT id FROM (SELECT id FROM events WHERE project_id=$1 AND uuid=$2 UNION ALL SELECT event_id AS id FROM event_receipts WHERE project_id=$3 AND uuid=$4 UNION ALL SELECT event_id AS id FROM event_heads WHERE project_id=$5 AND uuid=$6) AS receipt_ids WHERE id>0 LIMIT 1`, project, uuid, project, uuid, project, uuid).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return id, err
}
func (s *Service) recordReceipt(ctx context.Context, tx *sql.Tx, project, uuid string, id int64) error {
	if id > 0 {
		if _, err := tx.ExecContext(ctx, `UPDATE event_heads SET event_id=CASE WHEN event_id=0 THEN $1 ELSE event_id END WHERE project_id=$2 AND uuid=$3`, id, project, uuid); err != nil {
			return err
		}
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO event_receipts(project_id,uuid,event_id,recorded_at_ms) VALUES($1,$2,$3,$4) ON CONFLICT(project_id,uuid) DO UPDATE SET event_id=CASE WHEN event_receipts.event_id=0 THEN excluded.event_id ELSE event_receipts.event_id END`, project, uuid, id, s.clock().UnixMilli())
	return err
}

// receiptColdMargin widens the occurrence-time window checked against DuckLake,
// so client clocks running ahead cannot outlive a pruned receipt.
const receiptColdMargin = 7 * 24 * time.Hour

// coldKnown returns projected event IDs for events old enough that their local
// receipt may have been pruned. It runs before the SQLite acceptance
// transaction; an event projected meanwhile already has a fresh receipt.
func (rules *eventRules) coldKnown(ctx context.Context, project string, events []Event) (map[string]int64, error) {
	if rules.receiptRetention <= 0 || rules.knownEvents == nil {
		return nil, nil
	}
	cutoff := rules.clock().Add(-rules.receiptRetention + receiptColdMargin)
	var uuids []string
	for _, e := range events {
		at, err := time.Parse(time.RFC3339Nano, e.Timestamp)
		if err != nil || at.Before(cutoff) {
			uuids = append(uuids, e.UUID)
		}
	}
	if len(uuids) == 0 {
		return nil, nil
	}
	return rules.knownEvents(ctx, project, uuids)
}

// pruneReceipts removes receipts older than the retention window in small
// batches. Their events are already in DuckLake, where coldKnown finds them.
func (s *Service) pruneReceipts(ctx context.Context) error {
	if s.receiptRetention <= 0 || s.lake == nil {
		return nil
	}
	cutoff := s.clock().Add(-s.receiptRetention).UnixMilli()
	for {
		result, err := s.db.ExecContext(ctx, `DELETE FROM event_receipts WHERE (project_id,uuid) IN (SELECT project_id,uuid FROM event_receipts WHERE recorded_at_ms<$1 LIMIT 2000)`, cutoff)
		if err != nil {
			return err
		}
		if n, _ := result.RowsAffected(); n < 2000 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}
