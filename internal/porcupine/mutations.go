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

	"github.com/olup/porcupine/internal/acceptance"
	"github.com/olup/porcupine/internal/apierr"
	"github.com/olup/porcupine/internal/output"

	eventapi "github.com/olup/porcupine/internal/event"
	ingestfmt "github.com/olup/porcupine/internal/ingest"
	lakeapi "github.com/olup/porcupine/internal/lake"
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
	Event    eventapi.Event `json:"event"`
	Revision string         `json:"revision" doc:"Current revision as a decimal string. Pass this as expected_revision to edit or delete."`
	Pending  bool           `json:"pending" doc:"True until the current mutation is acknowledged by DuckLake."`
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
	State      string `json:"state" doc:"pending means durably queued; applied means DuckLake committed the projection; failed means a terminal error was recorded."`
	CreatedAt  string `json:"created_at"`
	AppliedAt  string `json:"applied_at,omitempty"`
	ErrorCode  string `json:"error_code,omitempty" doc:"Stable code for a terminal mutation failure."`
}

type MutationOutput struct{ Body MutationResponse }
type EventMutationOutput = MutationOutput

func parseExpectedRevision(raw string) (uint64, error) {
	if raw == "" {
		return 0, apierr.New(400, "expected_revision must be a decimal revision string")
	}
	for _, digit := range raw {
		if digit < '0' || digit > '9' {
			return 0, apierr.New(400, "expected_revision must be a decimal revision string")
		}
	}
	revision, err := strconv.ParseUint(raw, 10, 63)
	if err != nil || revision > math.MaxInt64 {
		return 0, apierr.New(400, "expected_revision exceeds the supported range")
	}
	return revision, nil
}

func validateEventSnapshot(snapshot EventSnapshot) (EventSnapshot, error) {
	return validateEventSnapshotActor(snapshot, false)
}

func validateEventSnapshotActor(snapshot EventSnapshot, allowEmpty bool) (EventSnapshot, error) {
	if strings.TrimSpace(snapshot.Event) == "" || len(snapshot.Event) > 500 || !allowEmpty && strings.TrimSpace(snapshot.DistinctID) == "" || len(snapshot.DistinctID) > 1000 {
		return snapshot, apierr.New(400, "event and distinct_id are required and must fit limits")
	}
	if snapshot.DistinctID == ingestfmt.CookielessSentinel {
		return snapshot, apierr.New(400, "event edits require a resolved distinct_id")
	}
	if strings.HasPrefix(snapshot.Event, "$") && snapshot.Event != "$pageview" && snapshot.Event != "$pageleave" && snapshot.Event != "$autocapture" && snapshot.Event != "$screen" && snapshot.Event != "$exception" {
		return snapshot, apierr.New(400, "unsupported reserved control event")
	}
	stamp, err := time.Parse(time.RFC3339Nano, snapshot.Timestamp)
	if err != nil {
		return snapshot, apierr.New(400, "timestamp must be RFC3339")
	}
	snapshot.Timestamp = stamp.UTC().Format(time.RFC3339Nano)
	if snapshot.Properties == nil {
		return snapshot, apierr.New(400, "properties must be an object")
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		return snapshot, apierr.New(400, "invalid event snapshot")
	}
	if len(encoded) > maxBodyBytes {
		return snapshot, apierr.New(413, "event snapshot exceeds 1 MiB")
	}
	// Detach request maps before removing identity properties or server-owned markers.
	var detached EventSnapshot
	if err := ingestfmt.DecodeJSON(encoded, &detached); err != nil {
		return snapshot, apierr.New(400, "invalid event snapshot")
	}
	return detached, nil
}

func (s *Service) getEvent(ctx context.Context, input *EventGetInput) (*EventGetOutput, error) {
	var projection lakeapi.EventProjection
	var pending bool
	var err error
	if s.hostedMutations != nil {
		projection, pending, err = s.hostedMutations.currentHostedEvent(ctx, input.ID, input.UUID)
	} else {
		s.mutationMu.Lock()
		defer s.mutationMu.Unlock()
		projection, pending, err = s.currentEvent(ctx, input.ID, input.UUID)
	}
	if err != nil {
		return nil, err
	}
	if projection.IdentitySnapshot != "" {
		return nil, apierr.New(404, "event not found")
	}
	if projection.IsDeleted {
		return nil, apierr.New(410, "event was deleted")
	}
	return &EventGetOutput{Body: EventCurrent{Event: projection.Event, Revision: strconv.FormatUint(projection.Revision, 10), Pending: pending}}, nil
}

func (s *Service) currentEvent(ctx context.Context, project, uuid string) (lakeapi.EventProjection, bool, error) {
	var revision int64
	var deleted, pending bool
	err := s.db.QueryRowContext(ctx, `SELECT revision,is_deleted,pending FROM event_heads WHERE project_id=$1 AND uuid=$2`, project, uuid).Scan(&revision, &deleted, &pending)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return lakeapi.EventProjection{}, false, apierr.New(503, "mutation storage unavailable")
	}
	if err == nil && pending {
		var payload string
		if err := s.db.QueryRowContext(ctx, `SELECT payload FROM mutation_outbox WHERE seq=$1 AND project_id=$2 AND uuid=$3`, revision, project, uuid).Scan(&payload); err != nil {
			return lakeapi.EventProjection{}, false, apierr.New(503, "mutation storage unavailable")
		}
		var projection lakeapi.EventProjection
		if json.Unmarshal([]byte(payload), &projection) != nil {
			return lakeapi.EventProjection{}, false, apierr.New(503, "mutation storage unavailable")
		}
		return projection, true, nil
	}
	if deleted {
		return lakeapi.EventProjection{Event: eventapi.Event{ProjectID: project, UUID: uuid}, Revision: uint64(revision), IsDeleted: true}, false, nil
	}
	if revision == 0 {
		var event eventapi.Event
		var properties, ingestion string
		err := s.db.QueryRowContext(ctx, `SELECT id,project_id,uuid,event,distinct_id,properties,timestamp,received_at,delivered,identity_snapshot,ingestion_metadata,is_imported FROM events WHERE project_id=$1 AND uuid=$2`, project, uuid).Scan(&event.ID, &event.ProjectID, &event.UUID, &event.Event, &event.DistinctID, &properties, &event.Timestamp, &event.ReceivedAt, &event.Delivered, &event.IdentitySnapshot, &ingestion, &event.Imported)
		if err == nil {
			if err := decodeIngestionMetadata(&event, ingestion); err != nil {
				return lakeapi.EventProjection{}, false, apierr.New(503, "event storage unavailable")
			}
			event.Properties = json.RawMessage(properties)
			return lakeapi.EventProjection{Event: event}, false, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return lakeapi.EventProjection{}, false, apierr.New(503, "event storage unavailable")
		}
	}
	if s.lake == nil {
		return lakeapi.EventProjection{}, false, apierr.New(404, "event not found")
	}
	projection, err := s.loadAnalyticalProjection(ctx, project, uuid)
	if errors.Is(err, lakeapi.ErrEventNotFound) && revision == 0 {
		return lakeapi.EventProjection{}, false, apierr.New(404, "event not found")
	}
	if err != nil || projection.Revision < uint64(revision) {
		return lakeapi.EventProjection{}, false, apierr.New(503, "event storage unavailable")
	}
	return projection, false, nil
}

func (s *Service) editEvent(ctx context.Context, input *EventEditInput) (*MutationOutput, error) {
	snapshot, err := validateEventSnapshotActor(input.Body.Snapshot, true)
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
		return nil, apierr.New(400, "idempotency_key must contain 1 to 200 characters")
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
		return nil, apierr.New(400, "invalid event snapshot")
	}
	requestHash, keyHash := hash(string(canonical)), hash(key)
	if s.hostedMutations != nil {
		response, err := s.hostedMutations.acceptHostedMutation(ctx, project, uuid, operation, keyHash, requestHash, revision, snapshot, s.authorizeHostedMutation)
		if err != nil {
			return nil, err
		}
		return &MutationOutput{Body: response}, nil
	}
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	previous, previousHash, err := s.findMutation(ctx, project, "idempotency_key_hash", keyHash)
	if err == nil {
		if previousHash != requestHash {
			return nil, apierr.New(409, "idempotency_key was used for a different request")
		}
		return &MutationOutput{Body: previous}, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, apierr.New(503, "mutation storage unavailable")
	}
	if s.lake == nil {
		return nil, apierr.New(503, "analytical delivery is disabled")
	}
	current, pending, err := s.currentEvent(ctx, project, uuid)
	if err != nil {
		return nil, err
	}
	projection, err := prepareMutation(current, pending, operation, revision, snapshot)
	if err != nil {
		return nil, err
	}
	return s.commitMutation(ctx, projection, actorID, operation, keyHash, requestHash, revision)
}

func prepareMutation(current lakeapi.EventProjection, pending bool, operation string, revision uint64, snapshot *EventSnapshot) (lakeapi.EventProjection, error) {
	if current.IdentitySnapshot != "" {
		return lakeapi.EventProjection{}, apierr.New(409, "identity control records cannot be edited or deleted")
	}
	if current.IsDeleted {
		return lakeapi.EventProjection{}, apierr.New(410, "event was deleted")
	}
	if pending {
		return lakeapi.EventProjection{}, apierr.New(409, "an event mutation is already pending")
	}
	if current.Revision != revision {
		return lakeapi.EventProjection{}, apierr.New(409, "expected_revision does not match the current event")
	}
	current.Event = lakeapi.EventWithMetadata(current.Event)
	if snapshot != nil && strings.TrimSpace(snapshot.DistinctID) == "" && !(current.DistinctID == "" && current.SourceID != "posthog" && (current.EventType == "track" || current.EventType == "pageview")) {
		return lakeapi.EventProjection{}, apierr.New(400, "distinct_id is required for this event")
	}

	projection := current
	if operation == "delete" {
		projection.IsDeleted = true
		projection.Event.Event, projection.DistinctID, projection.Timestamp = "", "", ""
		projection.Properties, projection.OriginalSnapshot = json.RawMessage(`{}`), ""
		projection.SourceID, projection.MessageID, projection.EventType = "", "", ""
		projection.Context = json.RawMessage(`{}`)
	} else {
		var originalProperties map[string]any
		if json.Unmarshal(current.Properties, &originalProperties) != nil {
			return lakeapi.EventProjection{}, apierr.New(503, "event storage unavailable")
		}
		if current.SourceID != "posthog" && snapshot.Event != current.Event.Event && lakeapi.EventWithMetadata(eventapi.Event{Event: snapshot.Event}).EventType != current.EventType {
			return lakeapi.EventProjection{}, apierr.New(400, "event category cannot be changed")
		}
		if current.SourceID == "posthog" {
			delete(snapshot.Properties, "token")
		}
		properties, _ := json.Marshal(snapshot.Properties)
		if originalProperties["cookieless"] == true && (current.SourceID == "posthog" || strings.HasPrefix(current.DistinctID, "cookieless_")) {
			if snapshot.DistinctID != current.DistinctID {
				return lakeapi.EventProjection{}, apierr.New(400, "cookieless distinct_id cannot be changed")
			}
			day, ok := originalProperties["identity_day"].(string)
			if !ok || day == "" {
				return lakeapi.EventProjection{}, apierr.New(503, "cookieless identity metadata is unavailable")
			}
			properties = acceptance.CookielessProperties(properties, current.DistinctID, day)
		} else {
			if current.SourceID == "posthog" {
				delete(snapshot.Properties, "cookieless")
				delete(snapshot.Properties, "identity_day")
			}
			properties, _ = json.Marshal(snapshot.Properties)
		}
		projection.Event.Event, projection.DistinctID = snapshot.Event, snapshot.DistinctID
		projection.Properties, projection.Timestamp = properties, snapshot.Timestamp
		if projection.OriginalSnapshot == "" {
			original, err := lakeapi.OriginalSnapshot(current.Event)
			if err != nil {
				return lakeapi.EventProjection{}, apierr.New(503, "event storage unavailable")
			}
			projection.OriginalSnapshot = original
		}
	}
	return projection, nil
}

func (s *Service) commitMutation(ctx context.Context, projection lakeapi.EventProjection, actorID, operation, keyHash, requestHash string, expected uint64) (*MutationOutput, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, apierr.New(503, "mutation storage unavailable")
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE metadata SET value=value WHERE key='initialized'`); err != nil {
		return nil, apierr.New(503, "mutation storage unavailable")
	}
	// A second service sharing this SQLite file must also respect CAS and the one-pending limit.
	var headRevision int64
	var pending bool
	err = tx.QueryRowContext(ctx, `SELECT revision,pending FROM event_heads WHERE project_id=$1 AND uuid=$2`, projection.ProjectID, projection.UUID).Scan(&headRevision, &pending)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, apierr.New(503, "mutation storage unavailable")
	}
	if pending || uint64(headRevision) > expected {
		return nil, apierr.New(409, "event revision changed or a mutation is pending")
	}
	var count, bytes int64
	if err := tx.QueryRowContext(ctx, `SELECT count(*),coalesce(sum(length(CAST(payload AS BLOB))),0) FROM mutation_outbox`).Scan(&count, &bytes); err != nil {
		return nil, apierr.New(503, "mutation storage unavailable")
	}
	if count >= int64(min(s.queueMax, 1000)) {
		return nil, apierr.New(503, "mutation queue is full")
	}
	// A restored SQLite backup may lag behind an already applied native
	// revision. Advance from the revision we loaded and checked, never from an
	// unchecked client claim, so the next accepted mutation still wins.
	advanced, err := tx.ExecContext(ctx, `UPDATE sqlite_sequence SET seq=max(seq,$1) WHERE name='mutation_audit'`, int64(expected))
	if err != nil {
		return nil, apierr.New(503, "mutation revision unavailable")
	}
	if changed, err := advanced.RowsAffected(); err != nil {
		return nil, apierr.New(503, "mutation revision unavailable")
	} else if changed == 0 {
		if _, err := tx.ExecContext(ctx, `INSERT INTO sqlite_sequence(name,seq) VALUES('mutation_audit',$1)`, int64(expected)); err != nil {
			return nil, apierr.New(503, "mutation revision unavailable")
		}
	}
	response := MutationResponse{MutationID: eventapi.NewID(), ProjectID: projection.ProjectID, UUID: projection.UUID, Operation: operation, State: "pending", CreatedAt: now()}
	result, err := tx.ExecContext(ctx, `INSERT INTO mutation_audit(mutation_id,project_id,uuid,actor_id,operation,idempotency_key_hash,request_hash,expected_revision,state,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`, response.MutationID, response.ProjectID, response.UUID, actorID, operation, keyHash, requestHash, int64(expected), response.State, response.CreatedAt)
	if err != nil {
		return nil, apierr.New(409, "mutation request conflicts with an existing request")
	}
	seq, err := result.LastInsertId()
	if err != nil || seq <= 0 || uint64(seq) <= expected {
		return nil, apierr.New(503, "mutation revision unavailable")
	}
	projection.Revision = uint64(seq)
	payload, err := json.Marshal(projection)
	if err != nil {
		return nil, apierr.New(503, "cannot encode mutation")
	}
	if len(payload) > mutationMaxBytes {
		return nil, apierr.New(413, "mutation exceeds the payload limit")
	}
	if bytes+int64(len(payload)) > mutationMaxBytes {
		return nil, apierr.New(503, "mutation queue is full")
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO event_heads(project_id,uuid,revision,is_deleted,pending,event_id) VALUES($1,$2,$3,$4,1,$5) ON CONFLICT(project_id,uuid) DO UPDATE SET revision=excluded.revision,is_deleted=excluded.is_deleted,pending=1,event_id=CASE WHEN event_heads.event_id=0 THEN excluded.event_id ELSE event_heads.event_id END`, projection.ProjectID, projection.UUID, seq, projection.IsDeleted, projection.ID)
	if err == nil {
		_, err = tx.ExecContext(ctx, `INSERT INTO mutation_outbox(seq,project_id,uuid,payload) VALUES($1,$2,$3,$4)`, seq, projection.ProjectID, projection.UUID, string(payload))
	}
	if err == nil && operation == "delete" {
		err = output.CancelWebhookEvent(ctx, tx, projection.ProjectID, projection.UUID, s.clock)
		if err == nil {
			err = s.recordReceipt(ctx, tx, projection.ProjectID, projection.UUID, projection.ID)
		}
		if err == nil {
			_, err = tx.ExecContext(ctx, `DELETE FROM events WHERE project_id=$1 AND uuid=$2`, projection.ProjectID, projection.UUID)
		}
	}
	if err != nil || tx.Commit() != nil {
		return nil, apierr.New(503, "mutation storage unavailable")
	}
	response.Revision = strconv.FormatInt(seq, 10)
	return &MutationOutput{Body: response}, nil
}

func (s *Service) findMutation(ctx context.Context, project, field, value string) (MutationResponse, string, error) {
	var response MutationResponse
	var revision int64
	var requestHash string
	// field is selected only by private callers, never from request input.
	err := s.db.QueryRowContext(ctx, `SELECT mutation_id,project_id,uuid,seq,operation,state,created_at,applied_at,request_hash FROM mutation_audit WHERE project_id=$1 AND `+field+`=$2`, project, value).Scan(&response.MutationID, &response.ProjectID, &response.UUID, &revision, &response.Operation, &response.State, &response.CreatedAt, &response.AppliedAt, &requestHash)
	response.Revision = strconv.FormatInt(revision, 10)
	return response, requestHash, err
}

func (s *Service) getEventMutation(ctx context.Context, input *EventMutationInput) (*EventMutationOutput, error) {
	if s.hostedMutations != nil {
		response, err := s.hostedMutations.hostedMutationStatus(ctx, input.ID, input.MutationID)
		if err != nil {
			return nil, err
		}
		return &EventMutationOutput{Body: response}, nil
	}
	response, _, err := s.findMutation(ctx, input.ID, "mutation_id", input.MutationID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, apierr.New(404, "mutation not found")
	}
	if err != nil {
		return nil, apierr.New(503, "mutation storage unavailable")
	}
	return &EventMutationOutput{Body: response}, nil
}
