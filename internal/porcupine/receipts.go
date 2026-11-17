package porcupine

import (
	"context"
	"database/sql"
	"errors"
)

// Native receipts are a permanent local ledger. A database which previously
// discarded them into another store cannot be opened without an explicit import.
func (s *Service) initializeReceipts(ctx context.Context) error {
	var foreign bool
	if err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM metadata WHERE (key IN ('receipt_source_uuid','receipt_source_config') AND value!='') OR (key IN ('receipt_fallback_required','receipt_protocol_required') AND value='1'))`).Scan(&foreign); err != nil {
		return err
	}
	if foreign {
		return errors.New("this acceptance database requires unsupported archived receipt history")
	}
	_, err := s.db.ExecContext(ctx, `DROP TRIGGER IF EXISTS receipt_protocol_guard; DROP TRIGGER IF EXISTS receipt_head_protocol_guard; DROP TABLE IF EXISTS receipt_lookup_leases; DROP TABLE IF EXISTS receipt_provenance; DELETE FROM metadata WHERE key IN ('receipt_generation','receipt_reconcile_cursor');`)
	return err
}

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
	_, err := tx.ExecContext(ctx, `INSERT INTO event_receipts(project_id,uuid,event_id) VALUES($1,$2,$3) ON CONFLICT(project_id,uuid) DO UPDATE SET event_id=CASE WHEN event_receipts.event_id=0 THEN excluded.event_id ELSE event_receipts.event_id END`, project, uuid, id)
	return err
}
