package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"time"

	eventapi "github.com/olup/porcupine/internal/event"
)

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
	_, err := tx.ExecContext(ctx, `INSERT INTO event_receipts(project_id,uuid,event_id,recorded_at_ms) VALUES($1,$2,$3,$4) ON CONFLICT(project_id,uuid) DO UPDATE SET event_id=CASE WHEN event_receipts.event_id=0 THEN excluded.event_id ELSE event_receipts.event_id END`, project, uuid, id, s.clock().UnixMilli())
	return err
}

// Receipt lookups and pruning serialize within one project before taking the
// short instance-wide policy/commit lock. A blocked object store never holds
// that global lock. Idle gates are removed; the map holds only current callers.
type receiptProjectGate struct {
	mutex contextMutex
	users int
}

type receiptProjectGates struct {
	mutex    sync.Mutex
	projects map[string]*receiptProjectGate
}

func (g *receiptProjectGates) lock(ctx context.Context, project string) (func(), error) {
	g.mutex.Lock()
	if g.projects == nil {
		g.projects = map[string]*receiptProjectGate{}
	}
	gate := g.projects[project]
	if gate == nil {
		gate = &receiptProjectGate{}
		g.projects[project] = gate
	}
	gate.users++
	g.mutex.Unlock()
	drop := func() {
		g.mutex.Lock()
		gate.users--
		if gate.users == 0 {
			delete(g.projects, project)
		}
		g.mutex.Unlock()
	}
	if err := gate.mutex.Lock(ctx); err != nil {
		drop()
		return nil, err
	}
	return func() { gate.mutex.Unlock(); drop() }, nil
}

const receiptLookupTimeout = 5 * time.Second

// coldKnown checks UUIDs without trusting producer timestamps. A retry may omit
// its timestamp or change any payload field while keeping the same message ID.
func (rules *eventRules) coldKnown(ctx context.Context, project string, events []eventapi.Event) (map[string]int64, error) {
	if rules.knownEvents == nil || len(events) == 0 {
		return nil, nil
	}
	uuids := make([]string, 0, len(events))
	seen := make(map[string]bool, len(events))
	for _, event := range events {
		if !seen[event.UUID] {
			uuids = append(uuids, event.UUID)
			seen[event.UUID] = true
		}
	}
	return rules.knownEvents(ctx, project, uuids)
}

// knownEventsForAcceptance keeps ingestion SQLite-only until this project has
// pruned a receipt. The marker survives restarts and disabling retention: once
// local history is incomplete, DuckLake must arbitrate every producer ID.
func (s *Service) knownEventsForAcceptance(ctx context.Context, project string, uuids []string) (map[string]int64, error) {
	var pruned bool
	if err := s.db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM metadata WHERE key='receipts_pruned:' || ?)", project).Scan(&pruned); err != nil {
		return nil, err
	}
	if !pruned {
		return nil, nil
	}
	if s.lake == nil {
		return nil, errors.New("analytical receipt history unavailable")
	}
	return s.lake.KnownEvents(ctx, project, uuids)
}

// pruneReceipts verifies each batch against analytical heads before deleting
// receipts. Remote reads use only the project gate; the rechecked deletion and
// durable marker commit together under the short instance admission lock.
func (s *Service) pruneReceipts(ctx context.Context) error {
	if s.receiptRetention <= 0 || s.lake == nil {
		return nil
	}
	for {
		count, err := s.pruneReceiptBatch(ctx)
		if err != nil || count < 2000 {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

type receiptCandidate struct {
	project, uuid string
	id            int64
}

func (s *Service) pruneReceiptBatch(ctx context.Context) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, receiptLookupTimeout)
	defer cancel()
	cutoff := s.clock().Add(-s.receiptRetention).UnixMilli()
	rows, err := s.db.QueryContext(ctx, `SELECT r.project_id,r.uuid,r.event_id FROM event_receipts r
 WHERE r.recorded_at_ms<? AND NOT EXISTS(SELECT 1 FROM events e WHERE e.project_id=r.project_id AND e.uuid=r.uuid)
 AND NOT EXISTS(SELECT 1 FROM mutation_outbox m WHERE m.project_id=r.project_id AND m.uuid=r.uuid)
 ORDER BY r.recorded_at_ms,r.project_id,r.uuid LIMIT 2000`, cutoff)
	if err != nil {
		return 0, err
	}
	projects := map[string][]receiptCandidate{}
	count := 0
	for rows.Next() {
		var r receiptCandidate
		if err := rows.Scan(&r.project, &r.uuid, &r.id); err != nil {
			rows.Close()
			return 0, err
		}
		projects[r.project] = append(projects[r.project], r)
		count++
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return 0, err
	}
	for project, candidates := range projects {
		if err := s.pruneProjectReceipts(ctx, project, candidates, cutoff); err != nil {
			return 0, err
		}
	}
	return count, nil
}

func (s *Service) pruneProjectReceipts(ctx context.Context, project string, candidates []receiptCandidate, cutoff int64) error {
	release, err := s.eventRules.receiptGates.lock(ctx, project)
	if err != nil {
		return err
	}
	defer release()
	uuids := make([]string, 0, len(candidates))
	for _, r := range candidates {
		uuids = append(uuids, r.uuid)
	}
	known, err := s.lake.KnownEvents(ctx, project, uuids)
	if err != nil {
		return err
	}
	for _, r := range candidates {
		if r.id <= 0 || known[r.uuid] != r.id {
			return errors.New("receipt does not match its analytical event")
		}
	}
	if s.eventRules.admission != nil {
		if err := s.eventRules.admission.Lock(ctx); err != nil {
			return err
		}
		defer s.eventRules.admission.Unlock()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	removed := false
	for _, r := range candidates {
		// The remote check ran outside the global lock. A concurrent lifecycle
		// operation or pending mutation may have changed the candidate since.
		result, err := tx.ExecContext(ctx, `DELETE FROM event_receipts AS r WHERE project_id=? AND uuid=? AND event_id=? AND recorded_at_ms<?
 AND NOT EXISTS(SELECT 1 FROM events e WHERE e.project_id=r.project_id AND e.uuid=r.uuid)
 AND NOT EXISTS(SELECT 1 FROM mutation_outbox m WHERE m.project_id=r.project_id AND m.uuid=r.uuid)`, project, r.uuid, r.id, cutoff)
		if err != nil {
			return err
		}
		count, err := result.RowsAffected()
		if err != nil {
			return err
		}
		removed = removed || count > 0
	}
	if removed {
		if _, err := tx.ExecContext(ctx, "INSERT INTO metadata(key,value) VALUES('receipts_pruned:' || ?,'1') ON CONFLICT(key) DO NOTHING", project); err != nil {
			return err
		}
	}
	return tx.Commit()
}
