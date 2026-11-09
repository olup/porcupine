package porcupine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

const receiptWorkLimit = 256
const receiptLookupLifetime = 30 * time.Second

var errReceiptHistory = errors.New("receipt history unavailable or unverified")
var errReceiptBudget = errors.New("receipt budget reconciliation required")

type receiptKey struct {
	ProjectID string `json:"project_id"`
	UUID      string `json:"uuid"`
}
type receiptLookup struct {
	known           map[string]bool
	ids             map[string]int64
	unknown         int
	leaseID, source string
	active          bool
}

func (s *Service) initializeReceipts(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS receipt_provenance (
seq INTEGER PRIMARY KEY AUTOINCREMENT,project_id TEXT NOT NULL,uuid TEXT NOT NULL,source_uuid TEXT NOT NULL,
UNIQUE(project_id,uuid),FOREIGN KEY(project_id,uuid) REFERENCES event_receipts(project_id,uuid) ON DELETE CASCADE);
CREATE INDEX IF NOT EXISTS receipt_provenance_source_seq ON receipt_provenance(source_uuid,seq);
CREATE TABLE IF NOT EXISTS receipt_lookup_leases (
lease_id TEXT NOT NULL,project_id TEXT NOT NULL,uuid TEXT NOT NULL,expires_at_ms INTEGER NOT NULL,
PRIMARY KEY(lease_id,project_id,uuid)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS receipt_leases_key ON receipt_lookup_leases(project_id,uuid,expires_at_ms);
CREATE INDEX IF NOT EXISTS receipt_leases_expiry ON receipt_lookup_leases(expires_at_ms);
INSERT OR IGNORE INTO metadata(key,value) VALUES('receipt_generation','0');`)
	if err != nil {
		return err
	}
	var protocolColumns int
	if err = s.db.QueryRowContext(ctx, `SELECT count(*) FROM pragma_table_info('events') WHERE name='capture_protocol'`).Scan(&protocolColumns); err != nil {
		return err
	}
	if protocolColumns == 0 {
		if _, err = s.db.ExecContext(ctx, `ALTER TABLE events ADD COLUMN capture_protocol INTEGER NOT NULL DEFAULT 0`); err != nil {
			return err
		}
	}
	if _, err = s.db.ExecContext(ctx, `CREATE TRIGGER IF NOT EXISTS receipt_protocol_guard BEFORE INSERT ON events WHEN NEW.capture_protocol<>1 AND EXISTS(SELECT 1 FROM metadata WHERE key='receipt_fallback_required' AND value='1') BEGIN SELECT RAISE(ABORT,'receipt archive requires protocol-aware writer'); END;`); err != nil {
		return err
	}
	if _, err = s.db.ExecContext(ctx, `CREATE TRIGGER IF NOT EXISTS receipt_head_protocol_guard BEFORE INSERT ON events WHEN NEW.capture_protocol<>1 AND EXISTS(SELECT 1 FROM metadata WHERE key='receipt_protocol_required' AND value='1') BEGIN SELECT RAISE(ABORT,'receipt archive requires protocol-aware writer'); END;`); err != nil {
		return err
	}
	var config string
	err = s.db.QueryRowContext(ctx, `SELECT value FROM metadata WHERE key='receipt_source_config'`).Scan(&config)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if config != "" && (s.ch == nil || config != s.receiptConfigFingerprint()) {
		return errReceiptHistory
	}
	if s.hotReceiptLimit > 0 && s.ch == nil {
		return errors.New("finite receipts require configured ClickHouse history")
	}
	if config != "" {
		probe, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if _, err = s.receiptSourceIdentity(probe); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) receiptConfigFingerprint() string {
	if s.ch == nil {
		return ""
	}
	return hash(strings.TrimSuffix(s.ch.url.String(), "/") + "\x00" + s.ch.config.Database + "\x00" + s.ch.config.User)
}

func receiptMetadata(ctx context.Context, q oauthQueryer, key string) (string, error) {
	var value string
	err := q.QueryRowContext(ctx, `SELECT value FROM metadata WHERE key=?`, key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return value, err
}

func (s *Service) pinReceiptSource(ctx context.Context, uuid string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `UPDATE metadata SET value=value WHERE key='initialized'`); err != nil {
		return err
	}
	pinned, err := receiptMetadata(ctx, tx, "receipt_source_uuid")
	if err != nil {
		return err
	}
	config, err := receiptMetadata(ctx, tx, "receipt_source_config")
	if err != nil {
		return err
	}
	if pinned != "" && (pinned != uuid || config != s.receiptConfigFingerprint()) {
		return errReceiptHistory
	}
	if pinned == "" {
		for key, value := range map[string]string{"receipt_source_uuid": uuid, "receipt_source_config": s.receiptConfigFingerprint()} {
			if _, err = tx.ExecContext(ctx, `INSERT INTO metadata(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, key, value); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

func (s *Service) receiptSourceIdentity(ctx context.Context) (string, error) {
	if s.ch == nil {
		return "", errReceiptHistory
	}
	uuid, _, err := s.ch.receiptFacts(ctx, nil)
	if err != nil {
		return "", err
	}
	if err = s.pinReceiptSource(ctx, uuid); err != nil {
		return "", err
	}
	return uuid, nil
}

func localReceipt(ctx context.Context, q oauthQueryer, project, uuid string) (bool, error) {
	var found bool
	err := q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM events WHERE project_id=? AND uuid=?) OR EXISTS(SELECT 1 FROM event_receipts WHERE project_id=? AND uuid=?) OR EXISTS(SELECT 1 FROM event_heads WHERE project_id=? AND uuid=?) OR EXISTS(SELECT 1 FROM mutation_outbox WHERE project_id=? AND uuid=?)`, project, uuid, project, uuid, project, uuid, project, uuid).Scan(&found)
	return found, err
}

func (s *Service) prepareReceiptLookup(ctx context.Context, project string, events []Event) (*receiptLookup, error) {
	lookup := &receiptLookup{known: map[string]bool{}, ids: map[string]int64{}}
	// Until the first eviction the complete ledger is local. The final write
	// transaction rechecks this bit, closing the first-eviction race.
	fallback, err := receiptMetadata(ctx, s.db, "receipt_fallback_required")
	if err != nil {
		return nil, err
	}
	if fallback != "1" {
		return lookup, nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `UPDATE metadata SET value=value WHERE key='initialized'`); err != nil {
		return nil, err
	}
	fallback, err = receiptMetadata(ctx, tx, "receipt_fallback_required")
	if err != nil {
		return nil, err
	}
	lookup.active = fallback == "1"
	if !lookup.active {
		return lookup, tx.Commit()
	}
	keys := []receiptKey{}
	seen := map[string]bool{}
	for _, event := range events {
		if seen[event.UUID] {
			continue
		}
		seen[event.UUID] = true
		found, err := localReceipt(ctx, tx, project, event.UUID)
		if err != nil {
			return nil, err
		}
		if found {
			lookup.known[event.UUID] = true
		} else {
			keys = append(keys, receiptKey{project, event.UUID})
		}
	}
	lookup.unknown = len(keys)
	if len(keys) == 0 {
		return lookup, tx.Commit()
	}
	now := s.clock().UnixMilli()
	if _, err = tx.ExecContext(ctx, `DELETE FROM receipt_lookup_leases WHERE expires_at_ms<=?`, now); err != nil {
		return nil, err
	}
	var leases int
	if err = tx.QueryRowContext(ctx, `SELECT count(DISTINCT lease_id) FROM receipt_lookup_leases`).Scan(&leases); err != nil {
		return nil, err
	}
	if leases >= 64 {
		return nil, errReceiptHistory
	}
	lookup.leaseID = newID()
	for _, key := range keys {
		if _, err = tx.ExecContext(ctx, `INSERT INTO receipt_lookup_leases(lease_id,project_id,uuid,expires_at_ms) VALUES(?,?,?,?)`, lookup.leaseID, key.ProjectID, key.UUID, now+receiptLookupLifetime.Milliseconds()); err != nil {
			return nil, err
		}
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	select {
	case s.receiptSlots <- struct{}{}:
		defer func() { <-s.receiptSlots }()
	default:
		s.releaseReceiptLookup(lookup)
		return nil, errReceiptHistory
	}
	probe, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	source, seenFacts, err := s.ch.receiptFactsWithIDs(probe, keys)
	lookup.source = source
	if err == nil {
		var pinned string
		pinned, err = receiptMetadata(probe, s.db, "receipt_source_uuid")
		if pinned != lookup.source {
			err = errReceiptHistory
		}
	}
	if err != nil {
		s.releaseReceiptLookup(lookup)
		return nil, errReceiptHistory
	}
	for key, id := range seenFacts {
		lookup.known[key.UUID] = true
		lookup.ids[key.UUID] = id
	}
	return lookup, nil
}

func (s *Service) releaseReceiptLookup(lookup *receiptLookup) {
	if lookup == nil || lookup.leaseID == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s.db.ExecContext(ctx, `DELETE FROM receipt_lookup_leases WHERE lease_id=?`, lookup.leaseID)
}

func (s *Service) validateReceiptLookup(ctx context.Context, tx *sql.Tx, lookup *receiptLookup) error {
	fallback, err := receiptMetadata(ctx, tx, "receipt_fallback_required")
	if err != nil {
		return err
	}
	if !lookup.active {
		if fallback == "1" {
			return errReceiptHistory
		}
		return nil
	}
	if lookup.unknown == 0 {
		return nil
	}
	var count int
	if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM receipt_lookup_leases WHERE lease_id=? AND expires_at_ms>?`, lookup.leaseID, s.clock().UnixMilli()).Scan(&count); err != nil {
		return err
	}
	pinned, err := receiptMetadata(ctx, tx, "receipt_source_uuid")
	if err != nil {
		return err
	}
	if count != lookup.unknown || pinned != lookup.source {
		return errReceiptHistory
	}
	return nil
}

func (s *Service) receiptAcknowledgmentSource(ctx context.Context) (string, error) {
	pinned, err := receiptMetadata(ctx, s.db, "receipt_source_uuid")
	if err != nil {
		return "", err
	}
	if s.hotReceiptLimit == 0 && pinned == "" {
		return "", nil
	}
	probe, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return s.receiptSourceIdentity(probe)
}

func (s *Service) recordReceipt(ctx context.Context, tx *sql.Tx, project, uuid, source string, eventIDs ...int64) error {
	var eventID int64
	if len(eventIDs) > 0 {
		eventID = eventIDs[0]
	}
	if eventID > 0 {
		if _, err := tx.ExecContext(ctx, `UPDATE event_heads SET event_id=CASE WHEN event_id=0 THEN ? ELSE event_id END WHERE project_id=? AND uuid=?`, eventID, project, uuid); err != nil {
			return err
		}
	}
	// Mutation heads permanently remember accepted UUIDs, including a deletion
	// whose tombstone has not reached ClickHouse yet.
	if s.hotReceiptLimit > 0 {
		var head bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM event_heads WHERE project_id=? AND uuid=?)`, project, uuid).Scan(&head); err != nil {
			return err
		}
		if head {
			// A permanent head is enough for this binary. Mark the omission in
			// the same transaction so an older writer cannot forget that UUID.
			_, err := tx.ExecContext(ctx, `INSERT INTO metadata(key,value) VALUES('receipt_protocol_required','1') ON CONFLICT(key) DO UPDATE SET value='1'`)
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO event_receipts(project_id,uuid,event_id) VALUES(?,?,?) ON CONFLICT(project_id,uuid) DO UPDATE SET event_id=CASE WHEN event_receipts.event_id=0 THEN excluded.event_id ELSE event_receipts.event_id END`, project, uuid, eventID); err != nil {
		return err
	}
	if source != "" {
		_, err := tx.ExecContext(ctx, `INSERT INTO receipt_provenance(project_id,uuid,source_uuid) VALUES(?,?,?) ON CONFLICT(project_id,uuid) DO UPDATE SET source_uuid=excluded.source_uuid`, project, uuid, source)
		return err
	}
	return nil
}

func (s *Service) pruneReceipts(ctx context.Context, tx *sql.Tx) (int64, error) {
	if s.hotReceiptLimit == 0 {
		return 0, nil
	}
	var count int64
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM event_receipts`).Scan(&count); err != nil {
		return 0, err
	}
	over := count - s.hotReceiptLimit
	if over <= 0 {
		return 0, nil
	}
	pinned, err := receiptMetadata(ctx, tx, "receipt_source_uuid")
	if err != nil {
		return 0, err
	}
	if pinned == "" {
		return 0, errReceiptHistory
	}
	// The indexed ACK-proven queue handles steady-state eviction without
	// repeatedly sorting or scanning the entire hot cache.
	rows, err := tx.QueryContext(ctx, `SELECT p.project_id,p.uuid FROM receipt_provenance p WHERE p.source_uuid=? AND NOT EXISTS(SELECT 1 FROM receipt_lookup_leases l WHERE l.project_id=p.project_id AND l.uuid=p.uuid AND l.expires_at_ms>?) ORDER BY p.seq LIMIT ?`, pinned, s.clock().UnixMilli(), min(over, receiptWorkLimit))
	if err != nil {
		return 0, err
	}
	keys := []receiptKey{}
	for rows.Next() {
		var key receiptKey
		if err = rows.Scan(&key.ProjectID, &key.UUID); err != nil {
			rows.Close()
			return 0, err
		}
		keys = append(keys, key)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return 0, err
	}
	if len(keys) < int(min(over, receiptWorkLimit)) {
		// Legacy receipts with permanent mutation heads need no remote proof.
		// New finite-mode heads do not create redundant hot receipts at all.
		rows, err = tx.QueryContext(ctx, `SELECT r.project_id,r.uuid FROM event_receipts r JOIN event_heads h ON h.project_id=r.project_id AND h.uuid=r.uuid WHERE NOT EXISTS(SELECT 1 FROM receipt_provenance p WHERE p.project_id=r.project_id AND p.uuid=r.uuid AND p.source_uuid=?) AND NOT EXISTS(SELECT 1 FROM receipt_lookup_leases l WHERE l.project_id=r.project_id AND l.uuid=r.uuid AND l.expires_at_ms>?) LIMIT ?`, pinned, s.clock().UnixMilli(), min(over, receiptWorkLimit)-int64(len(keys)))
		if err != nil {
			return 0, err
		}
		for rows.Next() {
			var key receiptKey
			if err = rows.Scan(&key.ProjectID, &key.UUID); err != nil {
				rows.Close()
				return 0, err
			}
			keys = append(keys, key)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return 0, err
		}
	}
	for _, key := range keys {
		if _, err = tx.ExecContext(ctx, `DELETE FROM event_receipts WHERE project_id=? AND uuid=?`, key.ProjectID, key.UUID); err != nil {
			return 0, err
		}
	}
	if len(keys) > 0 {
		if _, err = tx.ExecContext(ctx, `INSERT INTO metadata(key,value) VALUES('receipt_fallback_required','1') ON CONFLICT(key) DO UPDATE SET value='1'`); err != nil {
			return 0, err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE metadata SET value=CAST(value AS INTEGER)+1 WHERE key='receipt_generation'`); err != nil {
			return 0, err
		}
	}
	return int64(len(keys)), nil
}

func (s *Service) enforceReceiptLimit(ctx context.Context, tx *sql.Tx) error {
	if s.hotReceiptLimit == 0 {
		return nil
	}
	if _, err := s.pruneReceipts(ctx, tx); err != nil {
		return err
	}
	var count int64
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM event_receipts`).Scan(&count); err != nil {
		return err
	}
	if count > s.hotReceiptLimit {
		return errReceiptBudget
	}
	return nil
}

// Reconciliation only certifies receipts actually found in authoritative history.
// An unproven legacy receipt is never removed merely because it is old.
func (s *Service) reconcileReceipts(ctx context.Context) (bool, error) {
	if s.hotReceiptLimit == 0 {
		return false, nil
	}
	var count int64
	if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM event_receipts`).Scan(&count); err != nil {
		return false, err
	}
	if count <= s.hotReceiptLimit {
		return false, nil
	}
	encoded, err := receiptMetadata(ctx, s.db, "receipt_reconcile_cursor")
	if err != nil {
		return false, err
	}
	var cursor receiptKey
	if encoded != "" && json.Unmarshal([]byte(encoded), &cursor) != nil {
		return false, errReceiptHistory
	}
	keys, err := s.unprovenReceipts(ctx, cursor)
	if err != nil {
		return false, err
	}
	if len(keys) == 0 {
		keys, err = s.unprovenReceipts(ctx, receiptKey{})
		if err != nil {
			return false, err
		}
	}
	probe, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	source, seen, err := s.ch.receiptFacts(probe, keys)
	if err != nil {
		return false, errReceiptHistory
	}
	if err = s.pinReceiptSource(probe, source); err != nil {
		return false, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	for key := range seen {
		if _, err = tx.ExecContext(ctx, `INSERT INTO receipt_provenance(project_id,uuid,source_uuid) SELECT project_id,uuid,? FROM event_receipts WHERE project_id=? AND uuid=? ON CONFLICT(project_id,uuid) DO UPDATE SET source_uuid=excluded.source_uuid`, source, key.ProjectID, key.UUID); err != nil {
			return false, err
		}
	}
	if len(keys) > 0 {
		last, _ := json.Marshal(keys[len(keys)-1])
		if _, err = tx.ExecContext(ctx, `INSERT INTO metadata(key,value) VALUES('receipt_reconcile_cursor',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, string(last)); err != nil {
			return false, err
		}
	}
	removed, err := s.pruneReceipts(ctx, tx)
	if err != nil {
		return false, err
	}
	if err = tx.Commit(); err != nil {
		return false, err
	}
	if removed == 0 {
		return false, errReceiptBudget
	}
	return true, nil
}

func (s *Service) unprovenReceipts(ctx context.Context, cursor receiptKey) ([]receiptKey, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT r.project_id,r.uuid FROM event_receipts r LEFT JOIN receipt_provenance p ON p.project_id=r.project_id AND p.uuid=r.uuid WHERE p.seq IS NULL AND (r.project_id,r.uuid)>(?,?) ORDER BY r.project_id,r.uuid LIMIT ?`, cursor.ProjectID, cursor.UUID, receiptWorkLimit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	keys := []receiptKey{}
	for rows.Next() {
		var key receiptKey
		if err = rows.Scan(&key.ProjectID, &key.UUID); err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, rows.Err()
}

func localReceiptEventID(ctx context.Context, q oauthQueryer, project, uuid string) (int64, error) {
	var id int64
	err := q.QueryRowContext(ctx, `SELECT id FROM (SELECT id FROM events WHERE project_id=? AND uuid=? UNION ALL SELECT event_id AS id FROM event_receipts WHERE project_id=? AND uuid=? UNION ALL SELECT event_id AS id FROM event_heads WHERE project_id=? AND uuid=?) WHERE id>0 LIMIT 1`, project, uuid, project, uuid, project, uuid).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return id, err
}
