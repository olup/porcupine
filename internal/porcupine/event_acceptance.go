package porcupine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/netip"
	"strconv"
	"strings"
	"time"

	ingestfmt "github.com/olup/porcupine/internal/ingest"
)

// eventRules is the shared policy for live and historical acceptance. Its clock
// and budgets have one owner even when several entry points write the journal.
type eventRules struct {
	admission         *contextMutex
	clock             func() time.Time
	queueMax          int
	queueMaxBytes     int64
	allocateID        func(context.Context, *sql.Tx) (int64, error)
	consumeTenant     func(context.Context, *sql.Tx, string, time.Time, int64) error
	cookielessKey     func(context.Context, *sql.Tx, string) ([]byte, error)
	prepareAcceptance func(context.Context, string) error
	// receiptRetention bounds local receipts; knownEvents finds older events.
	receiptRetention time.Duration
	knownEvents      func(context.Context, string, []string) (map[string]int64, error)
}

type preparedLiveBatch struct {
	projectID  string
	events     []Event
	cookieless bool
}

type ingestionPeer struct {
	address   netip.Addr
	userAgent string
}

type liveAcceptanceResult struct {
	accepted, duplicates, identityIgnored int
	receipts                              []ingestionReceipt
}

type acceptanceFailure struct {
	message string
	retry   bool
}

func (e *acceptanceFailure) Error() string { return e.message }

// eventAcceptance owns the live commit. Destinations can join its transaction,
// but cannot decide when acceptance commits or acknowledges the producer.
type eventAcceptance struct {
	db              *sql.DB
	rules           *eventRules
	enqueueWebhooks func(context.Context, *sql.Tx, []Event) error
}

func prepareLiveBatch(projectID, source string, batch eventBatch, receiptTime time.Time) (preparedLiveBatch, error) {
	received := receiptTime.Format(time.RFC3339Nano)
	events := make([]Event, 0, len(batch))
	cookieless := false
	for _, e := range batch {
		if e.DistinctID == "" && e.ingestion == nil {
			if value, ok := e.Properties["distinct_id"].(string); ok {
				e.DistinctID = value
			}
		}
		if e.DistinctID == ingestfmt.CookielessSentinel && !isIdentityControl(e.Event) {
			cookieless = true
		}
		if strings.TrimSpace(e.Event) == "" || len(e.Event) > 500 || !isIdentityControl(e.Event) && (e.ingestion == nil && strings.TrimSpace(e.DistinctID) == "" || len(e.DistinctID) > 1000) {
			return preparedLiveBatch{}, errors.New("event and distinct_id are required and must fit limits")
		}
		if strings.HasPrefix(e.Event, "$") && !isIdentityControl(e.Event) && e.Event != "$pageview" && e.Event != "$pageleave" && e.Event != "$autocapture" && e.Event != "$screen" && e.Event != "$exception" {
			return preparedLiveBatch{}, errors.New("unsupported reserved control event")
		}
		if e.UUID == "" && e.ingestion == nil {
			e.UUID = newID()
		}
		if e.ingestion == nil && len(e.UUID) > 128 {
			return preparedLiveBatch{}, errors.New("uuid exceeds 128 characters")
		}
		if e.Timestamp == "" {
			e.Timestamp = received
		} else {
			t, err := time.Parse(time.RFC3339Nano, e.Timestamp)
			if err != nil {
				return preparedLiveBatch{}, errors.New("timestamp must be RFC3339")
			}
			e.Timestamp = t.UTC().Format(time.RFC3339Nano)
		}
		if e.Properties == nil {
			e.Properties = map[string]any{}
		}
		if isIdentityControl(e.Event) {
			for key, raw := range map[string]json.RawMessage{"$set": e.Set, "$set_once": e.SetOnce} {
				if len(raw) > 0 {
					var value any
					if err := json.Unmarshal(raw, &value); err != nil {
						return preparedLiveBatch{}, errors.New("invalid identity properties")
					}
					e.Properties[key] = value
				}
			}
		}
		if e.ingestion == nil {
			delete(e.Properties, "token")
			delete(e.Properties, "cookieless")
			delete(e.Properties, "identity_day")
		}
		props, err := json.Marshal(e.Properties)
		if err != nil {
			return preparedLiveBatch{}, errors.New("invalid properties")
		}
		prepared := Event{ProjectID: projectID, UUID: e.UUID, Event: e.Event, DistinctID: e.DistinctID, Properties: props, Timestamp: e.Timestamp, ReceivedAt: received}
		metadata := e.ingestion
		if metadata == nil && source != "" {
			enriched := eventWithMetadata(prepared)
			metadata = &ingestfmt.Metadata{MessageID: e.UUID, EventType: enriched.EventType, Context: enriched.Context, Protocol: "posthog"}
		}
		if metadata != nil {
			if err := prepareIngestionMetadata(&prepared, metadata, source); err != nil {
				return preparedLiveBatch{}, err
			}
		}
		events = append(events, prepared)
	}
	return preparedLiveBatch{projectID: projectID, events: events, cookieless: cookieless}, nil
}

func (a *eventAcceptance) Accept(ctx context.Context, batch preparedLiveBatch, peer ingestionPeer, withReceipts bool) (liveAcceptanceResult, error) {
	if a.rules.admission != nil {
		if err := a.rules.admission.Lock(ctx); err != nil {
			return liveAcceptanceResult{}, err
		}
		defer a.rules.admission.Unlock()
	}
	if a.rules.prepareAcceptance != nil {
		if err := a.rules.prepareAcceptance(ctx, batch.projectID); err != nil {
			return liveAcceptanceResult{}, &acceptanceFailure{message: "ingestion policy unavailable", retry: true}
		}
	}
	known, err := a.rules.coldKnown(ctx, batch.projectID, batch.events)
	if err != nil {
		return liveAcceptanceResult{}, &acceptanceFailure{message: "storage unavailable", retry: true}
	}
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return liveAcceptanceResult{}, &acceptanceFailure{message: "storage unavailable"}
	}
	defer tx.Rollback()
	// Serialize the owner's local receipt, identity and capacity checks.
	if _, err = tx.Exec(`UPDATE metadata SET value=value WHERE key='initialized'`); err != nil {
		return liveAcceptanceResult{}, &acceptanceFailure{message: "storage unavailable"}
	}
	acceptedAt := a.rules.clock().UTC()
	var pending, pendingBytes int64
	if err = tx.QueryRow(`SELECT pending_count,pending_bytes FROM queue_usage WHERE id=1`).Scan(&pending, &pendingBytes); err != nil {
		return liveAcceptanceResult{}, &acceptanceFailure{message: "storage unavailable"}
	}
	var cookielessID string
	var day string
	if batch.cookieless {
		// Choose the identity day under the SQLite write lock so delayed requests
		// cannot recreate an expired key after another request rotates it.
		day = acceptedAt.Format("2006-01-02")
		var secret []byte
		var err error
		if a.rules.cookielessKey != nil {
			secret, err = a.rules.cookielessKey(ctx, tx, day)
		} else {
			secret, err = dailyCookielessKey(tx, day)
		}
		if err != nil {
			return liveAcceptanceResult{}, &acceptanceFailure{message: "storage unavailable"}
		}
		cookielessID = cookielessDistinctID(secret, batch.projectID, peer.address, peer.userAgent)
	}
	accepted, duplicates, identityIgnored := 0, 0, 0
	var receipts []ingestionReceipt
	var acceptedBytes int64
	var webhookEvents []Event
	for _, e := range batch.events {
		exists, lookupErr := localReceipt(ctx, tx, batch.projectID, e.UUID)
		if lookupErr != nil {
			return liveAcceptanceResult{}, &acceptanceFailure{message: "storage unavailable"}
		}
		coldID, cold := known[e.UUID]
		if cold && !exists {
			if withReceipts {
				receipts = append(receipts, ingestionReceipt{MessageID: e.MessageID, EventID: strconv.FormatInt(coldID, 10), UUID: e.UUID, Duplicate: true})
			}
			duplicates++
			continue
		}
		if exists {
			if withReceipts {
				id, idErr := localReceiptEventID(ctx, tx, batch.projectID, e.UUID)
				if idErr != nil {
					return liveAcceptanceResult{}, &acceptanceFailure{message: "receipt event ID unavailable"}
				}
				if id <= 0 {
					return liveAcceptanceResult{}, &acceptanceFailure{message: "receipt event ID unavailable"}
				}
				receipts = append(receipts, ingestionReceipt{MessageID: e.MessageID, EventID: strconv.FormatInt(id, 10), UUID: e.UUID, Duplicate: true})
			}
			duplicates++
			continue
		}
		if pending+int64(accepted) >= int64(a.rules.queueMax) {
			return liveAcceptanceResult{}, &acceptanceFailure{message: "queue is full", retry: true}
		}
		if e.DistinctID == ingestfmt.CookielessSentinel && !isIdentityControl(e.Event) {
			e.DistinctID = cookielessID
			e.Properties = cookielessProperties(e.Properties, cookielessID, day)
			if e.SourceID != "" {
				e.Context = ingestfmt.ScrubContext(e.Context)
			}
		}
		identityInput := e
		if isIdentityControl(e.Event) {
			if e.SourceID != "" {
				e.Context = json.RawMessage(`{}`)
				identityInput.Context = e.Context
			}
			// Input IDs and trait operations remain in memory only. The queue stores
			// the canonical snapshot after validation, without redundant raw inputs.
			e.DistinctID, e.Properties = "", json.RawMessage(`{}`)
		}
		size := queueEventBytes(e)
		if size > a.rules.queueMaxBytes-pendingBytes-acceptedBytes {
			return liveAcceptanceResult{}, &acceptanceFailure{message: "queue is full", retry: true}
		}
		e.ID, err = a.rules.insertAcceptedEvent(ctx, tx, e)
		if err != nil {
			return liveAcceptanceResult{}, &acceptanceFailure{message: "storage unavailable"}
		}
		if isIdentityControl(e.Event) {
			identityInput.ID = e.ID
			ignored, identityErr := applyIdentityControl(ctx, tx, &identityInput)
			e = identityInput
			if identityErr != nil {
				return liveAcceptanceResult{}, &acceptanceFailure{message: "identity storage unavailable"}
			}
			if ignored {
				identityIgnored++
			}
			size = queueEventBytes(e)
			if size > a.rules.queueMaxBytes-pendingBytes-acceptedBytes {
				return liveAcceptanceResult{}, &acceptanceFailure{message: "queue is full", retry: true}
			}
		}
		if !isIdentityControl(e.Event) {
			webhookEvents = append(webhookEvents, e)
		}
		if withReceipts {
			receipts = append(receipts, ingestionReceipt{MessageID: e.MessageID, EventID: strconv.FormatInt(e.ID, 10), UUID: e.UUID})
		}
		accepted++
		acceptedBytes += size
	}
	if err = a.rules.consumeAcceptedEvents(ctx, tx, batch.projectID, acceptedAt, int64(accepted)); err != nil {
		var budget *tenantIngestionError
		if errors.As(err, &budget) {
			return liveAcceptanceResult{}, err
		}
		return liveAcceptanceResult{}, &acceptanceFailure{message: "storage unavailable"}
	}
	if a.enqueueWebhooks != nil {
		if err = a.enqueueWebhooks(ctx, tx, webhookEvents); err != nil {
			return liveAcceptanceResult{}, &acceptanceFailure{message: "webhook queue unavailable", retry: true}
		}
	}
	if err = tx.Commit(); err != nil {
		return liveAcceptanceResult{}, &acceptanceFailure{message: "storage unavailable"}
	}
	return liveAcceptanceResult{accepted: accepted, duplicates: duplicates, identityIgnored: identityIgnored, receipts: receipts}, nil
}

func (r *eventRules) insertAcceptedEvent(ctx context.Context, tx *sql.Tx, event Event) (int64, error) {
	args := []any{event.ProjectID, event.UUID, event.Event, event.DistinctID, string(event.Properties), event.Timestamp, event.ReceivedAt, encodeIngestionMetadata(event), event.Imported}
	query := `INSERT INTO events(project_id,uuid,event,distinct_id,properties,timestamp,received_at,ingestion_metadata,is_imported) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING id`
	if r.allocateID != nil {
		id, err := r.allocateID(ctx, tx)
		if err != nil {
			return 0, err
		}
		args = append(args, id)
		query = `INSERT INTO events(project_id,uuid,event,distinct_id,properties,timestamp,received_at,ingestion_metadata,is_imported,id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING id`
	}
	var id int64
	err := tx.QueryRowContext(ctx, query, args...).Scan(&id)
	return id, err
}

func (r *eventRules) consumeAcceptedEvents(ctx context.Context, tx *sql.Tx, project string, at time.Time, count int64) error {
	if r.consumeTenant != nil {
		return r.consumeTenant(ctx, tx, project, at, count)
	}
	return consumeTenantEventsTx(ctx, tx, project, at, count)
}
