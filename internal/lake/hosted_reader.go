package lake

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/olup/porcupine/internal/apierr"
	"github.com/olup/porcupine/internal/event"
)

type analyticalProjectLease func(context.Context, string) (*ProjectLake, func(), error)

// HostedReader attaches only existing, initialized hosted catalogs. It never
// creates a project, migrates its schema, or falls back to the legacy opener.
type HostedReader struct {
	store *DuckLakeStore
}

func NewHostedReader(store *DuckLakeStore) (*HostedReader, error) {
	if store == nil || store.SharedCatalog == nil || store.Config.SharedSourceID == "" {
		return nil, errors.New("hosted reads require a shared native catalog")
	}
	return &HostedReader{store: store}, nil
}

func (r *HostedReader) Query(ctx context.Context, project, rawSQL string, parameters map[string]string, limits QueryExecutionLimits) (AnalyticsResult, error) {
	return queryProject(ctx, r.lease, project, rawSQL, parameters, limits)
}

func (r *HostedReader) Catalog(ctx context.Context, input EventCatalogInput) (EventCatalog, error) {
	return catalogProject(ctx, r.lease, input)
}

func (r *HostedReader) Profile(ctx context.Context, project string, options EventProfileOptions, asOf time.Time) (EventProfileRead, error) {
	return profileProject(ctx, r.lease, project, options, asOf)
}

func (r *HostedReader) lease(ctx context.Context, project string) (*ProjectLake, func(), error) {
	p, release, err := r.store.leaseHostedProjectExisting(ctx, project)
	if err != nil {
		return nil, nil, err
	}
	key := ProjectWriteKey{SourceID: r.store.Config.SharedSourceID, ProjectID: project}
	if _, err := readHostedProjectScope(ctx, p.DB, key); err != nil {
		release()
		return nil, nil, err
	}
	return p, release, nil
}

const duckLakeListEventBoundsSQL = `SELECT coalesce(min(id),0),coalesce(max(id),0) FROM (
 SELECT id FROM lake.current_events WHERE identity_snapshot='' ORDER BY id DESC LIMIT ?
)`

const duckLakeListEventsSQL = `SELECT id,project_id,uuid,event,distinct_id,properties,timestamp,received_at,
 source_id,message_id,event_type,context,is_imported FROM lake.current_events
 WHERE identity_snapshot='' AND id BETWEEN ? AND ? ORDER BY id DESC LIMIT ?`

// ListEvents returns a bounded view of recently retained public event rows.
func (r *HostedReader) ListEvents(ctx context.Context, project string, maxRows, maxBytes int) ([]event.Event, error) {
	p, release, err := r.lease(ctx, project)
	if err != nil {
		return nil, DuckLakeQueryError(ctx, err)
	}
	defer release()
	tx, err := p.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, DuckLakeQueryError(ctx, err)
	}
	defer tx.Rollback()
	var minID, maxID int64
	if err := tx.QueryRowContext(ctx, duckLakeListEventBoundsSQL, maxRows).Scan(&minID, &maxID); err != nil {
		return nil, DuckLakeQueryError(ctx, err)
	}
	rows, err := tx.QueryContext(ctx, duckLakeListEventsSQL, minID, maxID, maxRows)
	if err != nil {
		return nil, DuckLakeQueryError(ctx, err)
	}
	defer rows.Close()
	events := []event.Event{}
	size := len(`{"events":[]}`)
	for rows.Next() {
		var e event.Event
		var properties, body string
		var imported uint8
		if err := rows.Scan(&e.ID, &e.ProjectID, &e.UUID, &e.Event, &e.DistinctID, &properties,
			&e.Timestamp, &e.ReceivedAt, &e.SourceID, &e.MessageID, &e.EventType, &body, &imported); err != nil {
			return nil, DuckLakeQueryError(ctx, err)
		}
		e.Properties, e.Context = json.RawMessage(properties), json.RawMessage(body)
		e.Delivered, e.Imported = true, imported != 0
		if body == "" {
			e.Context = nil
		}
		encoded, err := json.Marshal(e)
		if err != nil {
			return nil, DuckLakeQueryError(ctx, err)
		}
		size += len(encoded) + 1
		if size > maxBytes {
			return nil, apierr.Detailed(429, "result_byte_limit", "analytics result byte limit reached", "Retry with a smaller limit.", "")
		}
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		return nil, DuckLakeQueryError(ctx, err)
	}
	if err := tx.Commit(); err != nil {
		return nil, DuckLakeQueryError(ctx, err)
	}
	return events, nil
}
