package porcupine

import (
	"context"
	"database/sql"
	"fmt"
	"math"

	"github.com/olup/porcupine/internal/acceptance"
	eventapi "github.com/olup/porcupine/internal/event"
)

const DefaultQueueMaxBytes int64 = 256 << 20
const queueEventOverheadBytes int64 = 64

// queueEventBytes charges stored UTF-8 payload bytes plus a fixed row allowance.
// SQLite page/index overhead, WAL, receipts, and mutation snapshots are separate.
func queueEventBytes(event eventapi.Event) int64 {
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

// Read capacity once under the SQLite write lock. The caller accounts for
// events added in the current batch.
func (rules *eventRules) loadLiveQueueRoom(ctx context.Context, tx *sql.Tx, project string) (liveQueueRoom, error) {
	var pending, pendingBytes, projectPending, projectPendingBytes int64
	var err error
	if rules.queueProjectMaxEvents > 0 || rules.queueProjectMaxBytes > 0 {
		err = tx.QueryRowContext(ctx, `SELECT q.pending_count,q.pending_bytes,
COALESCE(p.pending_count,0),COALESCE(p.pending_bytes,0)
FROM queue_usage q LEFT JOIN project_queue_usage p ON p.project_id=? WHERE q.id=1`, project).
			Scan(&pending, &pendingBytes, &projectPending, &projectPendingBytes)
	} else {
		err = tx.QueryRowContext(ctx, `SELECT pending_count,pending_bytes FROM queue_usage WHERE id=1`).
			Scan(&pending, &pendingBytes)
	}
	room := liveQueueRoom{
		events:        int64(rules.queueMax) - pending,
		bytes:         rules.queueMaxBytes - pendingBytes,
		projectEvents: math.MaxInt64,
		projectBytes:  math.MaxInt64,
	}
	if rules.queueProjectMaxEvents > 0 {
		room.projectEvents = int64(rules.queueProjectMaxEvents) - projectPending
	}
	if rules.queueProjectMaxBytes > 0 {
		room.projectBytes = rules.queueProjectMaxBytes - projectPendingBytes
	}
	return room, err
}

type liveQueueRoom struct {
	events, bytes, projectEvents, projectBytes int64
}

var (
	errQueueFull        = &acceptance.Failure{Message: "queue is full", Retry: true, Pressure: true}
	errProjectQueueFull = &acceptance.Failure{Message: "project queue is full", Retry: true, Pressure: true}
)

func (room liveQueueRoom) check(events, bytes int64) error {
	if events > room.events || bytes > room.bytes {
		return errQueueFull
	}
	if events > room.projectEvents || bytes > room.projectBytes {
		return errProjectQueueFull
	}
	return nil
}
