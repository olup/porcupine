package porcupine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	eventapi "github.com/olup/porcupine/internal/event"
	lakeapi "github.com/olup/porcupine/internal/lake"
)

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
	if s.lake == nil {
		return errors.New("analytical delivery is disabled")
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

func (s *Service) deliveryBatch(ctx context.Context) ([]eventapi.Event, error) {
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

func (s *Service) deliveryBatchAfter(ctx context.Context, cursor int64) ([]eventapi.Event, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT e.id,e.project_id,e.uuid,e.event,e.distinct_id,e.properties,e.timestamp,e.received_at,e.identity_snapshot,e.ingestion_metadata,e.is_imported FROM delivery_ready r JOIN events e ON e.id=r.event_id WHERE r.event_id>$1 ORDER BY r.event_id LIMIT $2`, cursor, s.deliveryMaxEvents)
	if err != nil {
		return nil, errors.New("cannot read delivery queue")
	}
	defer rows.Close()
	batch := make([]eventapi.Event, 0, s.deliveryMaxEvents)
	bytes := 0
	for rows.Next() {
		var event eventapi.Event
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
		if bytes+size > s.deliveryMaxBytes && len(batch) > 0 {
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
	s.catalogMu.RLock()
	defer s.catalogMu.RUnlock()
	ctx, release, err := s.projectionContext(ctx)
	if err != nil {
		return false, err
	}
	defer release()
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

func (s *Service) acknowledgeOriginals(ctx context.Context, batch []eventapi.Event) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.New("cannot acknowledge delivered events")
	}
	defer tx.Rollback()
	projects := map[string]bool{}
	for _, event := range batch {
		if err := s.recordReceipt(ctx, tx, event.ProjectID, event.UUID, event.ID); err != nil {
			return errors.New("cannot save delivery receipt")
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM events WHERE id=$1`, event.ID); err != nil {
			return errors.New("cannot remove delivered event")
		}
		projects[event.ProjectID] = true
	}
	for projectID := range projects {
		if _, err := tx.ExecContext(ctx, `INSERT INTO metadata(key,value) VALUES($1,$2) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, "last_delivery_success:"+projectID, now()); err != nil {
			return errors.New("cannot record project delivery")
		}
	}
	if err := tx.Commit(); err != nil {
		return errors.New("cannot commit delivery receipts")
	}
	return nil
}

func (s *Service) recordDeliveryResult(ctx context.Context, deliveryErr error) {
	if deliveryErr != nil {
		message := "DuckLake delivery failed"
		s.db.ExecContext(ctx, `INSERT INTO metadata(key,value) VALUES('last_delivery_error',$1) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, message)
		return
	}
	s.db.ExecContext(ctx, `DELETE FROM metadata WHERE key='last_delivery_error'`)
}

func (s *Service) deliveryStatus(ctx context.Context, projectID string) (lakeapi.DeliveryStatus, error) {
	status := lakeapi.DeliveryStatus{Enabled: s.lake != nil}
	if s.hostedQueries != nil {
		// Buffered work is distributed and volatile. This worker cannot report
		// a complete count of events waiting for analytical visibility.
		status.PendingUnknown = true
		return status, nil
	}
	if err := s.db.QueryRowContext(ctx, `SELECT count(*), COALESCE((SELECT received_at FROM events WHERE project_id=$1 AND delivered=0 ORDER BY id LIMIT 1),'') FROM events WHERE project_id=$2 AND delivered=0`, projectID, projectID).Scan(&status.Pending, &status.OldestPendingReceivedAt); err != nil {
		return status, err
	}
	if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM delivery_quarantine q JOIN events e ON e.id=q.event_id WHERE e.project_id=$1`, projectID).Scan(&status.Quarantined); err != nil {
		return status, err
	}
	if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM mutation_outbox WHERE project_id=$1`, projectID).Scan(&status.PendingMutations); err != nil {
		return status, err
	}
	for _, field := range []struct {
		key   string
		value *string
	}{{"last_delivery_success:" + projectID, &status.LastSuccess}, {"last_delivery_error", &status.Error}} {
		if err := s.db.QueryRowContext(ctx, `SELECT value FROM metadata WHERE key=$1`, field.key).Scan(field.value); err != nil && !errors.Is(err, sql.ErrNoRows) {
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
