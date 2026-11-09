package porcupine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

type DeliveryStatus struct {
	Enabled                 bool   `json:"enabled"`
	Pending                 int64  `json:"pending"`
	OldestPendingReceivedAt string `json:"oldest_pending_received_at,omitempty" doc:"Receipt time of the earliest queued original by acceptance order, including quarantined events, in this project"`
	Quarantined             int64  `json:"quarantined" doc:"Accepted originals retained in SQLite after confirmed row-specific rejection. Included in pending and queue capacity."`
	PendingMutations        int64  `json:"pending_mutations"`
	LastSuccess             string `json:"last_success,omitempty"`
	Error                   string `json:"error,omitempty"`
}

func (s *Service) startDelivery() {
	ctx, cancel := context.WithCancel(context.Background())
	s.deliveryCancel = cancel
	s.deliveryDone = make(chan struct{})
	go func() {
		defer close(s.deliveryDone)
		initialized := false
		backoff := time.Second
		for ctx.Err() == nil {
			var err error
			if !initialized {
				err = s.initializeDelivery(ctx)
				initialized = err == nil
			}
			delivered := false
			originalProgress := false
			if err == nil {
				mutations, mutationErr := s.deliverMutations(ctx)
				var originalErr error
				originalProgress, originalErr = s.deliverBatch(ctx)
				delivered = mutations || originalProgress
				err = errors.Join(mutationErr, originalErr)
			}
			if ctx.Err() != nil {
				return
			}
			s.recordDeliveryResult(ctx, err)
			delay := time.Second
			if err != nil {
				if originalProgress {
					// A blocked mutation must not stop healthy originals, but retry
					// the failed lane at most once per second while they progress.
					delay, backoff = time.Second, time.Second
				} else {
					initialized = false
					delay = backoff
					backoff = min(backoff*2, 30*time.Second)
				}
			} else {
				backoff = time.Second
				if delivered {
					continue
				}
			}
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}()
}

func (s *Service) flushDelivery(ctx context.Context) error {
	if s.ch == nil {
		return errors.New("ClickHouse delivery is disabled")
	}
	err := s.initializeDelivery(ctx)
	if err == nil {
		_, mutationErr := s.deliverMutations(ctx)
		_, originalErr := s.deliverBatch(ctx)
		err = errors.Join(mutationErr, originalErr)
	}
	if ctx.Err() == nil {
		s.recordDeliveryResult(ctx, err)
	}
	return err
}

func (s *Service) deliveryBatch(ctx context.Context) ([]Event, error) {
	cursor, err := s.deliveryCursor(ctx)
	if err != nil {
		return nil, err
	}
	batch, err := s.deliveryBatchAfter(ctx, cursor)
	if err != nil || len(batch) > 0 || cursor == 0 {
		return batch, err
	}
	return s.deliveryBatchAfter(ctx, 0)
}

func (s *Service) deliveryBatchAfter(ctx context.Context, cursor int64) ([]Event, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT e.id,e.project_id,e.uuid,e.event,e.distinct_id,e.properties,e.timestamp,e.received_at,e.identity_snapshot,e.ingestion_metadata,e.is_imported FROM delivery_ready r JOIN events e ON e.id=r.event_id WHERE r.event_id>? ORDER BY r.event_id LIMIT 100`, cursor)
	if err != nil {
		return nil, errors.New("cannot read delivery queue")
	}
	defer rows.Close()
	batch := make([]Event, 0, 100)
	bytes := 0
	for rows.Next() {
		var event Event
		var properties, ingestion string
		if err := rows.Scan(&event.ID, &event.ProjectID, &event.UUID, &event.Event, &event.DistinctID, &properties, &event.Timestamp, &event.ReceivedAt, &event.IdentitySnapshot, &ingestion, &event.Imported); err != nil {
			return nil, errors.New("cannot read queued event")
		}
		if err := decodeIngestionMetadata(&event, ingestion); err != nil {
			return nil, err
		}
		event.Properties = json.RawMessage(properties)
		// Properties are already JSON. Encoding that JSON as a String at most doubles
		// its size; other strings can expand sixfold when JSON escapes control bytes.
		size := 4*len(properties) + 2*len(event.IdentitySnapshot) + 2*len(ingestion) + 12*(len(event.ProjectID)+len(event.UUID)+len(event.Event)+len(event.DistinctID)+len(event.Timestamp)+len(event.ReceivedAt)) + 2048
		// The byte budget is soft for one event: previously accepted JSON can grow
		// during encoding, and a large event must not permanently block the queue.
		if bytes+size > 4<<20 && len(batch) > 0 {
			break
		}
		batch = append(batch, event)
		bytes += size
	}
	if rows.Err() != nil {
		return nil, errors.New("cannot read delivery queue")
	}
	return batch, nil
}

func (s *Service) deliverBatch(ctx context.Context) (bool, error) {
	ctx, release, err := s.projectionContext(ctx)
	if err != nil {
		return false, err
	}
	defer release()
	if progress, err := s.reconcileReceipts(ctx); progress || err != nil {
		return progress, err
	}
	batch, err := s.deliveryBatch(ctx)
	if err != nil || len(batch) == 0 {
		return false, err
	}
	progress, deliveryErr := s.deliverIsolatedBatch(ctx, batch)
	// Advance even after an unclassified failure. The next attempt can reach
	// healthy later events without discarding this failed batch.
	if err := s.advanceDeliveryCursor(ctx, batch[len(batch)-1].ID); err != nil {
		return progress, err
	}
	return progress, deliveryErr
}

func (s *Service) acknowledgeOriginals(ctx context.Context, batch []Event) error {
	source, err := s.receiptAcknowledgmentSource(ctx)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.New("cannot acknowledge delivered events")
	}
	defer tx.Rollback()
	projects := map[string]bool{}
	for _, event := range batch {
		if err := s.recordReceipt(ctx, tx, event.ProjectID, event.UUID, source, event.ID); err != nil {
			return errors.New("cannot save delivery receipt")
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM events WHERE id=?`, event.ID); err != nil {
			return errors.New("cannot remove delivered event")
		}
		projects[event.ProjectID] = true
	}
	for projectID := range projects {
		if _, err := tx.ExecContext(ctx, `INSERT INTO metadata(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, "last_delivery_success:"+projectID, now()); err != nil {
			return errors.New("cannot record project delivery")
		}
	}
	if err := s.enforceReceiptLimit(ctx, tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return errors.New("cannot commit delivery receipts")
	}
	return nil
}

func (s *Service) recordDeliveryResult(ctx context.Context, deliveryErr error) {
	if deliveryErr != nil {
		s.db.ExecContext(ctx, `INSERT INTO metadata(key,value) VALUES('last_delivery_error','ClickHouse delivery failed') ON CONFLICT(key) DO UPDATE SET value=excluded.value`)
		return
	}
	s.db.ExecContext(ctx, `DELETE FROM metadata WHERE key='last_delivery_error'`)
}

func (s *Service) deliveryStatus(ctx context.Context, projectID string) (DeliveryStatus, error) {
	status := DeliveryStatus{Enabled: s.ch != nil}
	if err := s.db.QueryRowContext(ctx, `SELECT count(*), COALESCE((SELECT received_at FROM events WHERE project_id=? AND delivered=0 ORDER BY id LIMIT 1),'') FROM events WHERE project_id=? AND delivered=0`, projectID, projectID).Scan(&status.Pending, &status.OldestPendingReceivedAt); err != nil {
		return status, err
	}
	if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM delivery_quarantine q JOIN events e ON e.id=q.event_id WHERE e.project_id=?`, projectID).Scan(&status.Quarantined); err != nil {
		return status, err
	}
	if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM mutation_outbox WHERE project_id=?`, projectID).Scan(&status.PendingMutations); err != nil {
		return status, err
	}
	for _, field := range []struct {
		key   string
		value *string
	}{{"last_delivery_success:" + projectID, &status.LastSuccess}, {"last_delivery_error", &status.Error}} {
		if err := s.db.QueryRowContext(ctx, `SELECT value FROM metadata WHERE key=?`, field.key).Scan(field.value); err != nil && !errors.Is(err, sql.ErrNoRows) {
			return status, err
		}
	}
	if status.Pending == 0 && status.PendingMutations == 0 {
		status.Error = ""
	} else if status.Quarantined > 0 && status.Error == "" {
		status.Error = "Some accepted events require delivery repair"
	}
	return status, nil
}
