package porcupine

import (
	"context"
	"fmt"
)

const DefaultQueueMaxBytes int64 = 256 << 20
const queueEventOverheadBytes int64 = 64

// queueEventBytes charges stored UTF-8 payload bytes plus a fixed row allowance.
// SQLite page/index overhead, WAL, receipts, and mutation snapshots are separate.
func queueEventBytes(event Event) int64 {
	return queueEventOverheadBytes + int64(len(event.ProjectID)) + int64(len(event.UUID)) +
		int64(len(event.Event)) + int64(len(event.DistinctID)) + int64(len(event.Properties)) +
		int64(len(event.Timestamp)) + int64(len(event.ReceivedAt))
}

func queueByteExpression(prefix string) string {
	expression := fmt.Sprint(queueEventOverheadBytes)
	for _, column := range []string{"project_id", "uuid", "event", "distinct_id", "properties", "timestamp", "received_at"} {
		expression += "+length(CAST(" + prefix + column + " AS BLOB))"
	}
	return "(" + expression + ")"
}

func (s *Service) initializeQueueUsage(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS queue_usage (
id INTEGER PRIMARY KEY CHECK(id=1),
pending_count INTEGER NOT NULL CHECK(pending_count>=0),
pending_bytes INTEGER NOT NULL CHECK(pending_bytes>=0)
)`); err != nil {
		return err
	}
	var initialized bool
	if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM queue_usage WHERE id=1)`).Scan(&initialized); err != nil {
		return err
	}
	if !initialized {
		if _, err = tx.ExecContext(ctx, `INSERT INTO queue_usage(id,pending_count,pending_bytes)
SELECT 1,count(*),COALESCE(sum(`+queueByteExpression("")+`),0) FROM events WHERE delivered=0`); err != nil {
			return err
		}
	}
	newBytes, oldBytes := queueByteExpression("NEW."), queueByteExpression("OLD.")
	_, err = tx.ExecContext(ctx, `CREATE TRIGGER IF NOT EXISTS queue_usage_insert AFTER INSERT ON events WHEN NEW.delivered=0 BEGIN
UPDATE queue_usage SET pending_count=pending_count+1,pending_bytes=pending_bytes+`+newBytes+` WHERE id=1;
END;
CREATE TRIGGER IF NOT EXISTS queue_usage_delete AFTER DELETE ON events WHEN OLD.delivered=0 BEGIN
UPDATE queue_usage SET pending_count=pending_count-1,pending_bytes=pending_bytes-`+oldBytes+` WHERE id=1;
END;
CREATE TRIGGER IF NOT EXISTS queue_usage_update AFTER UPDATE ON events WHEN OLD.delivered=0 OR NEW.delivered=0 BEGIN
UPDATE queue_usage SET pending_count=pending_count+(NEW.delivered=0)-(OLD.delivered=0),
pending_bytes=pending_bytes+CASE WHEN NEW.delivered=0 THEN `+newBytes+` ELSE 0 END-CASE WHEN OLD.delivered=0 THEN `+oldBytes+` ELSE 0 END WHERE id=1;
END;`)
	if err != nil {
		return err
	}
	return tx.Commit()
}
