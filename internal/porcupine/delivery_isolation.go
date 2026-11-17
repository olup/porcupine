package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"time"
)

func (s *Service) deliveryCursor(ctx context.Context) (int64, error) {
	var value string
	err := s.db.QueryRowContext(ctx, `SELECT value FROM metadata WHERE key='delivery_scan_cursor'`).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, errors.New("cannot read delivery cursor")
	}
	cursor, err := strconv.ParseInt(value, 10, 64)
	if err != nil || cursor < 0 {
		return 0, errors.New("invalid delivery cursor")
	}
	return cursor, nil
}

func (s *Service) advanceDeliveryCursor(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO metadata(key,value) VALUES('delivery_scan_cursor',$1) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, strconv.FormatInt(id, 10))
	if err != nil {
		return errors.New("cannot save delivery cursor")
	}
	return nil
}

func (s *Service) deliverIsolatedBatch(ctx context.Context, batch []Event) (bool, error) {
	projections := make([]eventProjection, 0, len(batch))
	for _, event := range batch {
		// Unedited events need no stored original: the first correction
		// snapshots the current values, which are still the original.
		projections = append(projections, eventProjection{Event: event})
	}
	if err := s.applyAnalyticalProjections(ctx, projections); err != nil {
		return false, err
	}
	if err := s.acknowledgeOriginals(ctx, batch); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Service) quarantineOriginal(ctx context.Context, event Event, code int) (bool, error) {
	result, err := s.db.ExecContext(ctx, `INSERT INTO delivery_quarantine(event_id,error_code,quarantined_at) SELECT id,$1,$2 FROM events WHERE id=$3 AND project_id=$4 AND uuid=$5 ON CONFLICT(event_id) DO NOTHING`, code, s.clock().UTC().Format(time.RFC3339Nano), event.ID, event.ProjectID, event.UUID)
	if err != nil {
		return false, errors.New("cannot quarantine rejected event")
	}
	n, err := result.RowsAffected()
	if err != nil {
		return false, errors.New("cannot record quarantined event")
	}
	return n > 0, nil
}

// retryQuarantinedDelivery only makes an accepted original eligible again. It
// never changes its identity, payload, revision or receipt partition.
func (s *Service) retryQuarantinedDelivery(ctx context.Context, project string, eventID int64) (bool, error) {
	result, err := s.db.ExecContext(ctx, `DELETE FROM delivery_quarantine WHERE event_id IN (SELECT id FROM events WHERE id=$1 AND project_id=$2)`, eventID, project)
	if err != nil {
		return false, errors.New("cannot retry quarantined event")
	}
	n, err := result.RowsAffected()
	return n > 0, err
}

// The positive replacement is already acknowledged by the analytical store. Its private
// original snapshot preserves evidence unless this is an intentional deletion.
func (s *Service) acknowledgeSupersededQuarantine(ctx context.Context, tx *sql.Tx, projection eventProjection) error {
	if projection.Revision == 0 {
		return nil
	}
	var queued bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM events e JOIN delivery_quarantine q ON q.event_id=e.id WHERE e.project_id=$1 AND e.uuid=$2)`, projection.ProjectID, projection.UUID).Scan(&queued); err != nil {
		return err
	}
	if queued {
		if err := s.recordReceipt(ctx, tx, projection.ProjectID, projection.UUID, projection.ID); err != nil {
			return err
		}
	}
	_, err := tx.ExecContext(ctx, `DELETE FROM events WHERE project_id=$1 AND uuid=$2 AND id IN (SELECT event_id FROM delivery_quarantine)`, projection.ProjectID, projection.UUID)
	if err != nil {
		return errors.New("cannot remove superseded quarantined event")
	}
	return nil
}
