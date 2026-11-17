package lake

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"sort"
)

const SharedEventIDBlockSize int64 = 1024

// ReserveEventIDs reads whole sequence blocks on the held control session. The
// resulting candidates belong to this acquisition only; a new tenure reserves
// new candidates. Committed heads/proof, never this reservation, own stable IDs.
func (s *ProjectWriteSession) ReserveEventIDs(ctx context.Context, streamID, batchID string, count int) (EventIDReservation, error) {
	if streamID == "" || batchID == "" || len(batchID) > 200 || count <= 0 || count > 100000 {
		return EventIDReservation{}, errors.New("invalid native event ID reservation size or stream")
	}
	select {
	case <-s.stopProbe:
		return EventIDReservation{}, ErrProjectWriteSessionLost
	default:
	}
	s.operations.Lock()
	defer s.operations.Unlock()
	select {
	case <-s.stopProbe:
		return EventIDReservation{}, ErrProjectWriteSessionLost
	default:
	}
	if s.ctx.Err() != nil {
		return EventIDReservation{}, ErrProjectWriteSessionLost
	}
	bounded, cancel := context.WithTimeout(ctx, s.owner.config.ProbeTimeout)
	defer cancel()
	stop := context.AfterFunc(s.ctx, cancel)
	defer stop()
	if err := s.checkPhysicalSession(bounded); err != nil {
		s.cancel(err)
		return EventIDReservation{}, err
	}
	var increment, cache int64
	var cycle bool
	if err := s.conn.QueryRowContext(bounded, `SELECT increment_by,cache_size,cycle FROM pg_sequences WHERE schemaname=current_schema() AND sequencename='shared_event_id_sequence'`).Scan(&increment, &cache, &cycle); err != nil || increment != SharedEventIDBlockSize || cache != 1 || cycle {
		return EventIDReservation{}, errors.New("native event ID sequence has unsupported allocation settings")
	}
	blocks := (int64(count) + SharedEventIDBlockSize - 1) / SharedEventIDBlockSize
	rows, err := s.conn.QueryContext(bounded, `SELECT CASE WHEN pg_backend_pid()=$1 THEN nextval('shared_event_id_sequence') END FROM generate_series(1,$2)`, s.pid, blocks)
	if err != nil {
		return EventIDReservation{}, err
	}
	starts := make([]int64, 0, blocks)
	for rows.Next() {
		var start sql.NullInt64
		if err := rows.Scan(&start); err != nil {
			rows.Close()
			return EventIDReservation{}, err
		}
		if !start.Valid || start.Int64 <= 0 || start.Int64 > math.MaxInt64-SharedEventIDBlockSize+1 {
			rows.Close()
			return EventIDReservation{}, errors.New("native event ID range unavailable or exhausted")
		}
		starts = append(starts, start.Int64)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return EventIDReservation{}, err
	}
	if int64(len(starts)) != blocks {
		return EventIDReservation{}, errors.New("native event ID reservation is incomplete")
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i] < starts[j] })
	reservation := EventIDReservation{fence: s.Fence(streamID), batchID: batchID, ids: make([]int64, 0, count)}
	for _, start := range starts {
		for offset := int64(0); offset < SharedEventIDBlockSize && len(reservation.ids) < count; offset++ {
			reservation.ids = append(reservation.ids, start+offset)
		}
	}
	return reservation, nil
}
