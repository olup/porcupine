package porcupine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/olup/porcupine/internal/acceptance"
	"github.com/olup/porcupine/internal/apierr"
	eventapi "github.com/olup/porcupine/internal/event"
	identityapi "github.com/olup/porcupine/internal/identity"
	ingestfmt "github.com/olup/porcupine/internal/ingest"
	"github.com/olup/porcupine/internal/usage"
)

// eventRules is the shared policy for live and historical acceptance. Its clock
// and budgets have one owner even when several entry points write the journal.
type eventRules struct {
	usage             *usage.Recorder
	receipts          *acceptance.SQLiteReceipts
	clock             func() time.Time
	queueMax          int
	queueMaxBytes     int64
	prepareAcceptance func(context.Context, string) error

	queueProjectMaxEvents int
	queueProjectMaxBytes  int64
}

// durableAcceptance owns the live commit. Destinations can join its transaction,
// but cannot decide when acceptance commits or acknowledges the producer.
type durableAcceptance struct {
	db              *sql.DB
	rules           *eventRules
	enqueueWebhooks func(context.Context, *sql.Tx, []eventapi.Event) error
	wakeDelivery    func()
}

func prepareLiveBatch(projectID, source string, batch eventBatch, receiptTime time.Time, metrics *operationalMetrics) (acceptance.Batch, error) {
	received := receiptTime.Format(time.RFC3339Nano)
	events := make([]acceptance.Input, 0, len(batch))
	cookieless := false
	for _, e := range batch {
		if e.DistinctID == "" && e.ingestion == nil {
			if value, ok := e.Properties["distinct_id"].(string); ok {
				e.DistinctID = value
			}
		}
		if e.DistinctID == ingestfmt.CookielessSentinel && !identityapi.IsControl(e.Event) {
			cookieless = true
		}
		if strings.TrimSpace(e.Event) == "" || len(e.Event) > 500 || !identityapi.IsControl(e.Event) && (e.ingestion == nil && strings.TrimSpace(e.DistinctID) == "" || len(e.DistinctID) > 1000) {
			return acceptance.Batch{}, errors.New("event and distinct_id are required and must fit limits")
		}
		if strings.HasPrefix(e.Event, "$") && !identityapi.IsControl(e.Event) && e.Event != "$pageview" && e.Event != "$pageleave" && e.Event != "$autocapture" && e.Event != "$screen" && e.Event != "$exception" {
			return acceptance.Batch{}, errors.New("unsupported reserved control event")
		}
		if e.UUID == "" && e.ingestion == nil {
			e.UUID = eventapi.NewID()
		}
		if e.ingestion == nil && len(e.UUID) > 128 {
			return acceptance.Batch{}, errors.New("uuid exceeds 128 characters")
		}
		timestamp, adjustment, err := normalizeLiveTimestamp(e, receiptTime)
		if err != nil {
			return acceptance.Batch{}, errors.New("timestamp must be RFC3339")
		}
		if e.Properties == nil {
			e.Properties = map[string]any{}
		}
		if identityapi.IsControl(e.Event) {
			for key, raw := range map[string]json.RawMessage{"$set": e.Set, "$set_once": e.SetOnce} {
				if len(raw) > 0 {
					var value any
					if err := json.Unmarshal(raw, &value); err != nil {
						return acceptance.Batch{}, errors.New("invalid identity properties")
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
			return acceptance.Batch{}, errors.New("invalid properties")
		}
		prepared := eventapi.Event{ProjectID: projectID, UUID: e.UUID, Event: e.Event, DistinctID: e.DistinctID, Properties: props, Timestamp: timestamp, ReceivedAt: received}
		if err := prepareLiveMetadata(&prepared, e, source); err != nil {
			return acceptance.Batch{}, err
		}
		input, err := acceptance.ValidateInput(prepared)
		if err != nil {
			return acceptance.Batch{}, err
		}
		if metrics != nil && adjustment != "" {
			metrics.observeTimestampAdjustment(adjustment)
		}
		events = append(events, input)
	}
	return acceptance.Batch{ProjectID: projectID, Events: events, Cookieless: cookieless}, nil
}

func (*durableAcceptance) Durability() acceptance.Durability { return acceptance.Durable }

func (a *durableAcceptance) Accept(ctx context.Context, batch acceptance.Batch, peer acceptance.Peer, withReceipts bool) (acceptance.Result, error) {
	uuids := make([]string, 0, len(batch.Events))
	for _, input := range batch.Events {
		uuids = append(uuids, input.Event.UUID)
	}
	known, release, err := a.rules.receipts.BeginAdmission(ctx, batch.ProjectID, uuids)
	if errors.Is(err, acceptance.ErrReceiptLookup) {
		return nil, &acceptance.Failure{Message: "storage unavailable", Retry: true}
	}
	if err != nil {
		return nil, err
	}
	defer release()
	if a.rules.prepareAcceptance != nil {
		if err := a.rules.prepareAcceptance(ctx, batch.ProjectID); err != nil {
			var managed *apierr.Error
			if errors.As(err, &managed) && managed.GetStatus() >= 400 && managed.GetStatus() < 500 {
				return nil, managed
			}
			return nil, &acceptance.Failure{Message: "ingestion policy unavailable", Retry: true}
		}
	}
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, &acceptance.Failure{Message: "storage unavailable"}
	}
	defer tx.Rollback()
	// Serialize the journal's receipt, identity and capacity checks.
	if _, err = tx.Exec(`UPDATE metadata SET value=value WHERE key='initialized'`); err != nil {
		return nil, &acceptance.Failure{Message: "storage unavailable"}
	}
	acceptedAt := a.rules.clock().UTC()
	room, err := a.rules.loadLiveQueueRoom(ctx, tx, batch.ProjectID)
	if err != nil {
		return nil, &acceptance.Failure{Message: "storage unavailable"}
	}
	var cookielessID string
	var day string
	if batch.Cookieless {
		// Choose the identity day under the SQLite write lock so delayed requests
		// cannot recreate an expired key after another request rotates it.
		day = acceptedAt.Format("2006-01-02")
		secret, err := dailyCookielessKey(tx, day)
		if err != nil {
			return nil, &acceptance.Failure{Message: "storage unavailable"}
		}
		cookielessID = acceptance.CookielessDistinctID(secret, batch.ProjectID, peer.Address, peer.UserAgent)
	}
	accepted, duplicates, identityIgnored := 0, 0, 0
	var receipts []acceptance.DurableReceipt
	var acceptedBytes int64
	var webhookEvents []eventapi.Event
	for _, input := range batch.Events {
		e := input.Event
		exists, lookupErr := acceptance.HasLocalReceipt(ctx, tx, batch.ProjectID, e.UUID)
		if lookupErr != nil {
			return nil, &acceptance.Failure{Message: "storage unavailable"}
		}
		coldID, cold := known[e.UUID]
		if cold && !exists {
			if withReceipts {
				receipts = append(receipts, acceptance.DurableReceipt{MessageID: e.MessageID, EventID: strconv.FormatInt(coldID, 10), UUID: e.UUID, Duplicate: true})
			}
			duplicates++
			continue
		}
		if exists {
			if withReceipts {
				id, idErr := acceptance.LocalReceiptEventID(ctx, tx, batch.ProjectID, e.UUID)
				if idErr != nil {
					return nil, &acceptance.Failure{Message: "receipt event ID unavailable"}
				}
				if id <= 0 {
					return nil, &acceptance.Failure{Message: "receipt event ID unavailable"}
				}
				receipts = append(receipts, acceptance.DurableReceipt{MessageID: e.MessageID, EventID: strconv.FormatInt(id, 10), UUID: e.UUID, Duplicate: true})
			}
			duplicates++
			continue
		}
		if e.DistinctID == ingestfmt.CookielessSentinel && !identityapi.IsControl(e.Event) {
			e = acceptance.ResolveCookieless(e, cookielessID, day)
		}
		identityInput := e
		if identityapi.IsControl(e.Event) {
			if e.SourceID != "" {
				e.Context = json.RawMessage(`{}`)
				identityInput.Context = e.Context
			}
			// Input IDs and trait operations remain in memory only. The queue stores
			// the canonical snapshot after validation, without redundant raw inputs.
			e.DistinctID, e.Properties = "", json.RawMessage(`{}`)
		}
		size := queueEventBytes(e)
		if err := room.check(int64(accepted)+1, acceptedBytes+size); err != nil {
			return nil, err
		}
		e.ID, err = insertAcceptedEvent(ctx, tx, e)
		if err != nil {
			return nil, &acceptance.Failure{Message: "storage unavailable"}
		}
		if identityapi.IsControl(e.Event) {
			identityInput.ID = e.ID
			ignored, identityErr := applyParsedIdentityControl(ctx, tx, &identityInput, *input.Identity)
			e = identityInput
			if identityErr != nil {
				return nil, &acceptance.Failure{Message: "identity storage unavailable"}
			}
			if ignored {
				identityIgnored++
			}
			size = queueEventBytes(e)
			if err := room.check(int64(accepted)+1, acceptedBytes+size); err != nil {
				return nil, err
			}
		}
		if !identityapi.IsControl(e.Event) {
			webhookEvents = append(webhookEvents, e)
		}
		if withReceipts {
			receipts = append(receipts, acceptance.DurableReceipt{MessageID: e.MessageID, EventID: strconv.FormatInt(e.ID, 10), UUID: e.UUID})
		}
		accepted++
		acceptedBytes += size
	}
	if err = consumeTenantEventsTx(ctx, tx, batch.ProjectID, acceptedAt, int64(accepted)); err != nil {
		var budget *tenantIngestionError
		if errors.As(err, &budget) {
			return nil, err
		}
		return nil, &acceptance.Failure{Message: "storage unavailable"}
	}
	if a.enqueueWebhooks != nil {
		if err = a.enqueueWebhooks(ctx, tx, webhookEvents); err != nil {
			return nil, &acceptance.Failure{Message: "webhook queue unavailable", Retry: true}
		}
	}
	if a.rules.usage != nil {
		if err = a.rules.usage.AddTx(ctx, tx, batch.ProjectID, acceptedAt, usage.Counters{Live: int64(accepted)}, 0); err != nil {
			return nil, &acceptance.Failure{Message: "storage unavailable"}
		}
	}
	if err = tx.Commit(); err != nil {
		return nil, &acceptance.Failure{Message: "storage unavailable"}
	}
	if accepted > 0 && a.wakeDelivery != nil {
		a.wakeDelivery()
	}
	return acceptance.DurableResult{Accepted: accepted, Duplicates: duplicates, IdentityIgnored: identityIgnored, Receipts: receipts}, nil
}

func insertAcceptedEvent(ctx context.Context, tx *sql.Tx, event eventapi.Event) (int64, error) {
	args := []any{event.ProjectID, event.UUID, event.Event, event.DistinctID, string(event.Properties), event.Timestamp, event.ReceivedAt, encodeIngestionMetadata(event), event.Imported}
	query := `INSERT INTO events(project_id,uuid,event,distinct_id,properties,timestamp,received_at,ingestion_metadata,is_imported) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING id`
	var id int64
	err := tx.QueryRowContext(ctx, query, args...).Scan(&id)
	return id, err
}
