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
		int64(len(event.Timestamp)) + int64(len(event.ReceivedAt)) + int64(len(event.IdentitySnapshot)) + int64(len(encodeIngestionMetadata(event)))
}

func queueByteExpression(prefix string) string {
	expression := fmt.Sprint(queueEventOverheadBytes)
	for _, column := range []string{"project_id", "uuid", "event", "distinct_id", "properties", "timestamp", "received_at", "identity_snapshot", "ingestion_metadata"} {
		expression += "+length(CAST(" + prefix + column + " AS BLOB))"
	}
	return "(" + expression + ")"
}

// initializeQueueUsage seeds the accounting row; schema triggers maintain it.
func (s *Service) initializeQueueUsage(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO queue_usage(id,pending_count,pending_bytes)
SELECT 1,count(*),COALESCE(sum(`+queueByteExpression("")+`),0) FROM events WHERE delivered=0`)
	return err
}
