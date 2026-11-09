package porcupine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"strings"
	"time"
)

const mutationMaxBytes = 64 << 20

type EventSnapshot struct {
	Event      string         `json:"event" maxLength:"500"`
	DistinctID string         `json:"distinct_id" maxLength:"1000"`
	Properties map[string]any `json:"properties"`
	Timestamp  string         `json:"timestamp"`
}

type EventGetInput struct {
	ID   string `path:"id"`
	UUID string `path:"uuid"`
}

type EventCurrent struct {
	Event    Event  `json:"event"`
	Revision string `json:"revision" doc:"Current revision as a decimal string. Pass this as expected_revision to edit or delete."`
	Pending  bool   `json:"pending" doc:"True until the current mutation is acknowledged by ClickHouse."`
}

type EventGetOutput struct{ Body EventCurrent }

type EventEditBody struct {
	IdempotencyKey   string        `json:"idempotency_key" minLength:"1" maxLength:"200"`
	ExpectedRevision string        `json:"expected_revision" doc:"Required current decimal revision from get-event."`
	Snapshot         EventSnapshot `json:"snapshot" doc:"Full replacement of the editable event fields."`
}

type EventEditInput struct {
	ID   string `path:"id"`
	UUID string `path:"uuid"`
	Body EventEditBody
}

type EventDeleteBody struct {
	IdempotencyKey   string `json:"idempotency_key" minLength:"1" maxLength:"200"`
	ExpectedRevision string `json:"expected_revision" doc:"Required current decimal revision from get-event."`
}

type EventDeleteInput struct {
	ID   string `path:"id"`
	UUID string `path:"uuid"`
	Body EventDeleteBody
}

type EventMutationInput struct {
	ID         string `path:"id"`
	MutationID string `path:"mutation_id"`
}

type MutationResponse struct {
	MutationID string `json:"mutation_id"`
	ProjectID  string `json:"project_id"`
	UUID       string `json:"uuid"`
	Revision   string `json:"revision"`
	Operation  string `json:"operation"`
	State      string `json:"state" doc:"pending means durably queued; applied means ClickHouse acknowledged the projection."`
	CreatedAt  string `json:"created_at"`
	AppliedAt  string `json:"applied_at,omitempty"`
}

type MutationOutput struct{ Body MutationResponse }
type EventMutationOutput = MutationOutput

func (s *Service) initializeMutations(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS event_heads (
project_id TEXT NOT NULL REFERENCES projects(id), uuid TEXT NOT NULL,
revision INTEGER NOT NULL, is_deleted INTEGER NOT NULL, pending INTEGER NOT NULL,
PRIMARY KEY(project_id,uuid)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS mutation_audit (
seq INTEGER PRIMARY KEY AUTOINCREMENT, mutation_id TEXT NOT NULL UNIQUE,
project_id TEXT NOT NULL REFERENCES projects(id), uuid TEXT NOT NULL,
actor_id TEXT NOT NULL, operation TEXT NOT NULL, idempotency_key_hash TEXT NOT NULL,
request_hash TEXT NOT NULL, expected_revision INTEGER NOT NULL,
state TEXT NOT NULL, created_at TEXT NOT NULL, applied_at TEXT NOT NULL DEFAULT '',
UNIQUE(project_id,idempotency_key_hash));
CREATE INDEX IF NOT EXISTS mutation_audit_project_sequence ON mutation_audit(project_id,seq);
CREATE TABLE IF NOT EXISTS mutation_outbox (
seq INTEGER PRIMARY KEY REFERENCES mutation_audit(seq),
project_id TEXT NOT NULL REFERENCES projects(id), uuid TEXT NOT NULL,
payload TEXT NOT NULL, UNIQUE(project_id,uuid));`)
	return err
}

func parseExpectedRevision(raw string) (uint64, error) {
	if raw == "" {
		return 0, apiError(400, "expected_revision must be a decimal revision string")
	}
	for _, digit := range raw {
		if digit < '0' || digit > '9' {
			return 0, apiError(400, "expected_revision must be a decimal revision string")
		}
	}
	revision, err := strconv.ParseUint(raw, 10, 63)
	if err != nil || revision > math.MaxInt64 {
		return 0, apiError(400, "expected_revision exceeds the supported range")
	}
	return revision, nil
}

func validateEventSnapshot(snapshot EventSnapshot) (EventSnapshot, error) {
	if strings.TrimSpace(snapshot.Event) == "" || len(snapshot.Event) > 500 || strings.TrimSpace(snapshot.DistinctID) == "" || len(snapshot.DistinctID) > 1000 {
		return snapshot, apiError(400, "event and distinct_id are required and must fit limits")
	}
	if snapshot.DistinctID == cookielessSentinel {
		return snapshot, apiError(400, "event edits require a resolved distinct_id")
	}
	if strings.HasPrefix(snapshot.Event, "$") && snapshot.Event != "$pageview" && snapshot.Event != "$pageleave" && snapshot.Event != "$autocapture" && snapshot.Event != "$screen" && snapshot.Event != "$exception" {
		return snapshot, apiError(400, "unsupported reserved control event")
	}
	stamp, err := time.Parse(time.RFC3339Nano, snapshot.Timestamp)
	if err != nil {
		return snapshot, apiError(400, "timestamp must be RFC3339")
	}
	snapshot.Timestamp = stamp.UTC().Format(time.RFC3339Nano)
	if snapshot.Properties == nil {
		return snapshot, apiError(400, "properties must be an object")
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		return snapshot, apiError(400, "invalid event snapshot")
	}
	if len(encoded) > maxBodyBytes {
		return snapshot, apiError(413, "event snapshot exceeds 1 MiB")
	}
	// Detach request maps before removing identity properties or server-owned markers.
	var detached EventSnapshot
	if err := json.Unmarshal(encoded, &detached); err != nil {
		return snapshot, apiError(400, "invalid event snapshot")
	}
	return detached, nil
}

func (s *Service) getEvent(ctx context.Context, input *EventGetInput) (*EventGetOutput, error) {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	projection, pending, err := s.currentEvent(ctx, input.ID, input.UUID)
	if err != nil {
		return nil, err
	}
	if projection.IdentitySnapshot != "" {
		return nil, apiError(404, "event not found")
	}
	if projection.IsDeleted {
		return nil, apiError(410, "event was deleted")
	}
	return &EventGetOutput{Body: EventCurrent{Event: projection.Event, Revision: strconv.FormatUint(projection.Revision, 10), Pending: pending}}, nil
}

func (s *Service) currentEvent(ctx context.Context, project, uuid string) (eventProjection, bool, error) {
	var revision int64
	var deleted, pending bool
	err := s.db.QueryRowContext(ctx, `SELECT revision,is_deleted,pending FROM event_heads WHERE project_id=? AND uuid=?`, project, uuid).Scan(&revision, &deleted, &pending)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return eventProjection{}, false, apiError(503, "mutation storage unavailable")
	}
	if err == nil && pending {
		var payload string
		if err := s.db.QueryRowContext(ctx, `SELECT payload FROM mutation_outbox WHERE seq=? AND project_id=? AND uuid=?`, revision, project, uuid).Scan(&payload); err != nil {
			return eventProjection{}, false, apiError(503, "mutation storage unavailable")
		}
		var projection eventProjection
		if json.Unmarshal([]byte(payload), &projection) != nil {
			return eventProjection{}, false, apiError(503, "mutation storage unavailable")
		}
		return projection, true, nil
	}
	if deleted {
		return eventProjection{Event: Event{ProjectID: project, UUID: uuid}, Revision: uint64(revision), IsDeleted: true}, false, nil
	}
	if revision == 0 {
		var event Event
		var properties string
		err := s.db.QueryRowContext(ctx, `SELECT id,project_id,uuid,event,distinct_id,properties,timestamp,received_at,delivered,identity_snapshot FROM events WHERE project_id=? AND uuid=?`, project, uuid).Scan(&event.ID, &event.ProjectID, &event.UUID, &event.Event, &event.DistinctID, &properties, &event.Timestamp, &event.ReceivedAt, &event.Delivered, &event.IdentitySnapshot)
		if err == nil {
			event.Properties = json.RawMessage(properties)
			snapshot, err := originalSnapshot(event)
			if err != nil {
				return eventProjection{}, false, apiError(503, "event storage unavailable")
			}
			return eventProjection{Event: event, OriginalSnapshot: snapshot}, false, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return eventProjection{}, false, apiError(503, "event storage unavailable")
		}
	}
	if s.ch == nil {
		return eventProjection{}, false, apiError(404, "event not found")
	}
	if err := s.ch.initialize(ctx); err != nil {
		return eventProjection{}, false, apiError(503, "event storage unavailable")
	}
	projection, err := s.ch.loadProjection(ctx, project, uuid)
	if errors.Is(err, errEventNotFound) && revision == 0 {
		return eventProjection{}, false, apiError(404, "event not found")
	}
	if err != nil || projection.Revision < uint64(revision) {
		return eventProjection{}, false, apiError(503, "event storage unavailable")
	}
	return projection, false, nil
}

func (s *Service) editEvent(ctx context.Context, input *EventEditInput) (*MutationOutput, error) {
	snapshot, err := validateEventSnapshot(input.Body.Snapshot)
	if err != nil {
		return nil, err
	}
	return s.enqueueMutation(ctx, input.ID, input.UUID, "edit", input.Body.IdempotencyKey, input.Body.ExpectedRevision, &snapshot)
}

func (s *Service) deleteEvent(ctx context.Context, input *EventDeleteInput) (*MutationOutput, error) {
	return s.enqueueMutation(ctx, input.ID, input.UUID, "delete", input.Body.IdempotencyKey, input.Body.ExpectedRevision, nil)
}

func (s *Service) enqueueMutation(ctx context.Context, project, uuid, operation, key, expected string, snapshot *EventSnapshot) (*MutationOutput, error) {
	revision, err := parseExpectedRevision(expected)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(key) == "" || len(key) > 200 {
		return nil, apiError(400, "idempotency_key must contain 1 to 200 characters")
	}
	actorID := principal(ctx).ID
	canonical, err := json.Marshal(struct {
		Actor     string         `json:"actor"`
		UUID      string         `json:"uuid"`
		Operation string         `json:"operation"`
		Revision  uint64         `json:"revision"`
		Snapshot  *EventSnapshot `json:"snapshot"`
	}{actorID, uuid, operation, revision, snapshot})
	if err != nil {
		return nil, apiError(400, "invalid event snapshot")
	}
	requestHash, keyHash := hash(string(canonical)), hash(key)
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	previous, previousHash, err := s.findMutation(ctx, project, "idempotency_key_hash", keyHash)
	if err == nil {
		if previousHash != requestHash {
			return nil, apiError(409, "idempotency_key was used for a different request")
		}
		return &MutationOutput{Body: previous}, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, apiError(503, "mutation storage unavailable")
	}
	if s.ch == nil {
		return nil, apiError(503, "ClickHouse delivery is disabled")
	}
	current, pending, err := s.currentEvent(ctx, project, uuid)
	if err != nil {
		return nil, err
	}
	if current.IdentitySnapshot != "" {
		return nil, apiError(409, "identity control records cannot be edited or deleted")
	}
	if current.IsDeleted {
		return nil, apiError(410, "event was deleted")
	}
	if pending {
		return nil, apiError(409, "an event mutation is already pending")
	}
	if current.Revision != revision {
		return nil, apiError(409, "expected_revision does not match the current event")
	}
	projection := current
	if operation == "delete" {
		projection.IsDeleted = true
		projection.Event.Event, projection.DistinctID, projection.Timestamp = "", "", ""
		projection.Properties, projection.OriginalSnapshot = json.RawMessage(`{}`), ""
	} else {
		var originalProperties map[string]any
		if json.Unmarshal(current.Properties, &originalProperties) != nil {
			return nil, apiError(503, "event storage unavailable")
		}
		delete(snapshot.Properties, "token")
		properties, _ := json.Marshal(snapshot.Properties)
		if originalProperties["cookieless"] == true {
			if snapshot.DistinctID != current.DistinctID {
				return nil, apiError(400, "cookieless distinct_id cannot be changed")
			}
			day, ok := originalProperties["identity_day"].(string)
			if !ok || day == "" {
				return nil, apiError(503, "cookieless identity metadata is unavailable")
			}
			properties = cookielessProperties(properties, current.DistinctID, day)
		} else {
			delete(snapshot.Properties, "cookieless")
			delete(snapshot.Properties, "identity_day")
			properties, _ = json.Marshal(snapshot.Properties)
		}
		projection.Event.Event, projection.DistinctID = snapshot.Event, snapshot.DistinctID
		projection.Properties, projection.Timestamp = properties, snapshot.Timestamp
		if projection.OriginalSnapshot == "" {
			projection.OriginalSnapshot, err = originalSnapshot(current.Event)
			if err != nil {
				return nil, apiError(503, "event storage unavailable")
			}
		}
	}
	return s.commitMutation(ctx, projection, actorID, operation, keyHash, requestHash, revision)
}

func (s *Service) commitMutation(ctx context.Context, projection eventProjection, actorID, operation, keyHash, requestHash string, expected uint64) (*MutationOutput, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, apiError(503, "mutation storage unavailable")
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE metadata SET value=value WHERE key='initialized'`); err != nil {
		return nil, apiError(503, "mutation storage unavailable")
	}
	// A second service sharing this SQLite file must also respect CAS and the one-pending limit.
	var headRevision int64
	var pending bool
	err = tx.QueryRowContext(ctx, `SELECT revision,pending FROM event_heads WHERE project_id=? AND uuid=?`, projection.ProjectID, projection.UUID).Scan(&headRevision, &pending)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, apiError(503, "mutation storage unavailable")
	}
	if pending || uint64(headRevision) > expected {
		return nil, apiError(409, "event revision changed or a mutation is pending")
	}
	var count, bytes int64
	if err := tx.QueryRowContext(ctx, `SELECT count(*),coalesce(sum(length(CAST(payload AS BLOB))),0) FROM mutation_outbox`).Scan(&count, &bytes); err != nil {
		return nil, apiError(503, "mutation storage unavailable")
	}
	if count >= int64(min(s.queueMax, 1000)) {
		return nil, apiError(503, "mutation queue is full")
	}
	// A restored SQLite backup may lag behind an already applied ClickHouse
	// revision. Advance from the revision we loaded and checked, never from an
	// unchecked client claim, so the next accepted mutation still wins.
	advanced, err := tx.ExecContext(ctx, `UPDATE sqlite_sequence SET seq=max(seq,?) WHERE name='mutation_audit'`, int64(expected))
	if err != nil {
		return nil, apiError(503, "mutation revision unavailable")
	}
	if changed, err := advanced.RowsAffected(); err != nil {
		return nil, apiError(503, "mutation revision unavailable")
	} else if changed == 0 {
		if _, err := tx.ExecContext(ctx, `INSERT INTO sqlite_sequence(name,seq) VALUES('mutation_audit',?)`, int64(expected)); err != nil {
			return nil, apiError(503, "mutation revision unavailable")
		}
	}
	response := MutationResponse{MutationID: newID(), ProjectID: projection.ProjectID, UUID: projection.UUID, Operation: operation, State: "pending", CreatedAt: now()}
	result, err := tx.ExecContext(ctx, `INSERT INTO mutation_audit(mutation_id,project_id,uuid,actor_id,operation,idempotency_key_hash,request_hash,expected_revision,state,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)`, response.MutationID, response.ProjectID, response.UUID, actorID, operation, keyHash, requestHash, int64(expected), response.State, response.CreatedAt)
	if err != nil {
		return nil, apiError(409, "mutation request conflicts with an existing request")
	}
	seq, err := result.LastInsertId()
	if err != nil || seq <= 0 || uint64(seq) <= expected {
		return nil, apiError(503, "mutation revision unavailable")
	}
	projection.Revision = uint64(seq)
	payload, err := json.Marshal(projection)
	if err != nil {
		return nil, apiError(503, "cannot encode mutation")
	}
	if len(payload) > mutationMaxBytes {
		return nil, apiError(413, "mutation exceeds the payload limit")
	}
	if bytes+int64(len(payload)) > mutationMaxBytes {
		return nil, apiError(503, "mutation queue is full")
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO event_heads(project_id,uuid,revision,is_deleted,pending) VALUES(?,?,?,?,1) ON CONFLICT(project_id,uuid) DO UPDATE SET revision=excluded.revision,is_deleted=excluded.is_deleted,pending=1`, projection.ProjectID, projection.UUID, seq, projection.IsDeleted)
	if err == nil {
		_, err = tx.ExecContext(ctx, `INSERT INTO mutation_outbox(seq,project_id,uuid,payload) VALUES(?,?,?,?)`, seq, projection.ProjectID, projection.UUID, string(payload))
	}
	if err == nil && operation == "delete" {
		err = s.cancelWebhookEvent(ctx, tx, projection.ProjectID, projection.UUID)
		if err == nil {
			err = s.recordReceipt(ctx, tx, projection.ProjectID, projection.UUID, "")
		}
		if err == nil {
			_, err = tx.ExecContext(ctx, `DELETE FROM events WHERE project_id=? AND uuid=?`, projection.ProjectID, projection.UUID)
		}
	}
	if err != nil || tx.Commit() != nil {
		return nil, apiError(503, "mutation storage unavailable")
	}
	response.Revision = strconv.FormatInt(seq, 10)
	return &MutationOutput{Body: response}, nil
}

func (s *Service) findMutation(ctx context.Context, project, field, value string) (MutationResponse, string, error) {
	var response MutationResponse
	var revision int64
	var requestHash string
	// field is selected only by private callers, never from request input.
	err := s.db.QueryRowContext(ctx, `SELECT mutation_id,project_id,uuid,seq,operation,state,created_at,applied_at,request_hash FROM mutation_audit WHERE project_id=? AND `+field+`=?`, project, value).Scan(&response.MutationID, &response.ProjectID, &response.UUID, &revision, &response.Operation, &response.State, &response.CreatedAt, &response.AppliedAt, &requestHash)
	response.Revision = strconv.FormatInt(revision, 10)
	return response, requestHash, err
}

func (s *Service) getEventMutation(ctx context.Context, input *EventMutationInput) (*EventMutationOutput, error) {
	response, _, err := s.findMutation(ctx, input.ID, "mutation_id", input.MutationID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, apiError(404, "mutation not found")
	}
	if err != nil {
		return nil, apiError(503, "mutation storage unavailable")
	}
	return &EventMutationOutput{Body: response}, nil
}
