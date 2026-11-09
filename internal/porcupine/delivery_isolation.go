package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"time"
)

const deliveryIsolationProbes = 32
const deliveryIsolationTimeout = 10 * time.Second

// Only these errors specifically describe a rejected value/row constraint.
// General parser, schema, memory, permission, network and storage errors are not
// sufficient evidence to quarantine accepted records.
func deliveryRowError(err error) (int, bool) {
	var failure *clickHouseError
	if !errors.As(err, &failure) || (failure.Status != 400 && failure.Status != 500 && failure.Status != 200) {
		return 0, false
	}
	switch failure.Code {
	case 321, 469:
		return failure.Code, true
	}
	return 0, false
}

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
	_, err := s.db.ExecContext(ctx, `INSERT INTO metadata(key,value) VALUES('delivery_scan_cursor',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, strconv.FormatInt(id, 10))
	if err != nil {
		return errors.New("cannot save delivery cursor")
	}
	return nil
}

type deliveryIsolation struct {
	s         *Service
	ctx       context.Context
	remaining int
	progress  bool
}

func (s *Service) deliverIsolatedBatch(ctx context.Context, batch []Event) (bool, error) {
	err := s.ch.insert(ctx, batch)
	if err == nil {
		if err = s.acknowledgeOriginals(ctx, batch); err != nil {
			return false, err
		}
		return true, nil
	}
	if _, specific := deliveryRowError(err); !specific {
		return false, err
	}
	probeCtx, cancel := context.WithTimeout(ctx, deliveryIsolationTimeout)
	defer cancel()
	isolation := deliveryIsolation{s: s, ctx: probeCtx, remaining: deliveryIsolationProbes - 1}
	err = isolation.reject(batch, err)
	return isolation.progress, err
}

func (p *deliveryIsolation) insert(batch []Event) error {
	if p.remaining == 0 {
		return errDeliveryProbeBudget
	}
	p.remaining--
	return p.s.ch.insert(p.ctx, batch)
}

var errDeliveryProbeBudget = errors.New("delivery isolation probe budget exhausted")

func (p *deliveryIsolation) reject(batch []Event, failure error) error {
	code, specific := deliveryRowError(failure)
	if !specific {
		return failure
	}
	if len(batch) == 1 {
		// A second independent rejection must name the same row-specific code.
		confirmed := p.insert(batch)
		if confirmed == nil {
			if err := p.s.acknowledgeOriginals(p.ctx, batch); err != nil {
				return err
			}
			p.progress = true
			return nil
		}
		if errors.Is(confirmed, errDeliveryProbeBudget) {
			return nil
		}
		second, ok := deliveryRowError(confirmed)
		if !ok || second != code {
			return confirmed
		}
		changed, err := p.s.quarantineOriginal(p.ctx, batch[0], code)
		p.progress = p.progress || changed
		return err
	}
	middle := len(batch) / 2
	for _, part := range [][]Event{batch[:middle], batch[middle:]} {
		err := p.insert(part)
		if errors.Is(err, errDeliveryProbeBudget) {
			return nil
		}
		if err == nil {
			if err = p.s.acknowledgeOriginals(p.ctx, part); err != nil {
				return err
			}
			p.progress = true
			continue
		}
		if err = p.reject(part, err); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) quarantineOriginal(ctx context.Context, event Event, code int) (bool, error) {
	result, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO delivery_quarantine(event_id,error_code,quarantined_at) SELECT id,?,? FROM events WHERE id=? AND project_id=? AND uuid=?`, code, s.clock().UTC().Format(time.RFC3339Nano), event.ID, event.ProjectID, event.UUID)
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
	result, err := s.db.ExecContext(ctx, `DELETE FROM delivery_quarantine WHERE event_id IN (SELECT id FROM events WHERE id=? AND project_id=?)`, eventID, project)
	if err != nil {
		return false, errors.New("cannot retry quarantined event")
	}
	n, err := result.RowsAffected()
	return n > 0, err
}

// The positive replacement is already acknowledged by ClickHouse. Its private
// original snapshot preserves evidence unless this is an intentional deletion.
func (s *Service) acknowledgeSupersededQuarantine(ctx context.Context, tx *sql.Tx, projection eventProjection) error {
	if projection.Revision == 0 {
		return nil
	}
	var queued bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM events e JOIN delivery_quarantine q ON q.event_id=e.id WHERE e.project_id=? AND e.uuid=?)`, projection.ProjectID, projection.UUID).Scan(&queued); err != nil {
		return err
	}
	if queued {
		if err := s.recordReceipt(ctx, tx, projection.ProjectID, projection.UUID, "", projection.ID); err != nil {
			return err
		}
	}
	_, err := tx.ExecContext(ctx, `DELETE FROM events WHERE project_id=? AND uuid=? AND id IN (SELECT event_id FROM delivery_quarantine)`, projection.ProjectID, projection.UUID)
	if err != nil {
		return errors.New("cannot remove superseded quarantined event")
	}
	return nil
}
