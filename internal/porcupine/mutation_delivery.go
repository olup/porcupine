package porcupine

import (
	"context"
	"encoding/json"
	"errors"
)

// deliverMutations retains exactly the same immutable projection across retries.
// The mutex serializes projection lookups, CAS acceptance, and acknowledgment,
// while network calls run without holding a SQLite transaction.
func (s *Service) deliverMutations(ctx context.Context) (bool, error) {
	s.catalogMu.RLock()
	defer s.catalogMu.RUnlock()
	if s.ch == nil {
		return false, errors.New("ClickHouse delivery is disabled")
	}
	ctx, release, err := s.projectionContext(ctx)
	if err != nil {
		return false, err
	}
	defer release()
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	rows, err := s.db.QueryContext(ctx, `SELECT seq,payload FROM mutation_outbox ORDER BY seq LIMIT 100`)
	if err != nil {
		return false, errors.New("cannot read mutation queue")
	}
	var revisions []int64
	var projections []eventProjection
	bytes := 0
	for rows.Next() {
		var seq int64
		var payload string
		if err := rows.Scan(&seq, &payload); err != nil {
			rows.Close()
			return false, errors.New("cannot read queued mutation")
		}
		if bytes+len(payload) > 4<<20 && len(projections) > 0 {
			break
		}
		var projection eventProjection
		if json.Unmarshal([]byte(payload), &projection) != nil || projection.Revision != uint64(seq) {
			rows.Close()
			return false, errors.New("invalid queued mutation")
		}
		revisions = append(revisions, seq)
		projections = append(projections, projection)
		bytes += len(payload)
	}
	readErr := rows.Err()
	rows.Close()
	if readErr != nil {
		return false, errors.New("cannot read mutation queue")
	}
	if len(projections) == 0 {
		return false, nil
	}
	if err := s.ch.initialize(ctx); err != nil {
		return false, err
	}
	if err := s.ch.insertProjections(ctx, projections); err != nil {
		return false, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, errors.New("cannot acknowledge delivered mutations")
	}
	defer tx.Rollback()
	applied := now()
	projects := map[string]bool{}
	for i, seq := range revisions {
		projection := projections[i]
		if _, err := tx.ExecContext(ctx, `DELETE FROM mutation_outbox WHERE seq=?`, seq); err != nil {
			return false, errors.New("cannot remove delivered mutation payload")
		}
		if _, err := tx.ExecContext(ctx, `UPDATE mutation_audit SET state='applied',applied_at=? WHERE seq=?`, applied, seq); err != nil {
			return false, errors.New("cannot acknowledge delivered mutation")
		}
		if _, err := tx.ExecContext(ctx, `UPDATE event_heads SET pending=0 WHERE project_id=? AND uuid=? AND revision=?`, projection.ProjectID, projection.UUID, seq); err != nil {
			return false, errors.New("cannot acknowledge current event revision")
		}
		if err := s.acknowledgeSupersededQuarantine(ctx, tx, projection); err != nil {
			return false, err
		}
		projects[projection.ProjectID] = true
	}
	for projectID := range projects {
		if _, err := tx.ExecContext(ctx, `INSERT INTO metadata(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, "last_delivery_success:"+projectID, applied); err != nil {
			return false, errors.New("cannot record project mutation delivery")
		}
	}
	if err := tx.Commit(); err != nil {
		return false, errors.New("cannot commit mutation acknowledgments")
	}
	return true, nil
}
