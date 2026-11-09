package porcupine

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const eventTable = "events_v2"

var errEventNotFound = errors.New("event not found")

type eventProjection struct {
	Event
	Revision         uint64
	IsDeleted        bool
	OriginalSnapshot string
}

type originalEventSnapshot struct {
	Event      string          `json:"event"`
	DistinctID string          `json:"distinct_id"`
	Properties json.RawMessage `json:"properties"`
	Timestamp  string          `json:"timestamp"`
}

func originalSnapshot(event Event) (string, error) {
	var body bytes.Buffer
	encoder := json.NewEncoder(&body)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(originalEventSnapshot{event.Event, event.DistinctID, event.Properties, event.Timestamp}); err != nil {
		return "", errors.New("cannot encode original event")
	}
	return strings.TrimSuffix(body.String(), "\n"), nil
}

type clickHouseEvent struct {
	EventID          uint64 `json:"event_id"`
	ProjectID        string `json:"project_id"`
	UUID             string `json:"uuid"`
	Event            string `json:"event"`
	DistinctID       string `json:"distinct_id"`
	Properties       string `json:"properties"`
	Timestamp        string `json:"timestamp"`
	TimestampMS      int64  `json:"timestamp_ms"`
	ReceivedAt       string `json:"received_at"`
	ReceivedAtMS     int64  `json:"received_at_ms"`
	Revision         uint64 `json:"revision"`
	IsDeleted        uint8  `json:"is_deleted"`
	OriginalSnapshot string `json:"original_snapshot"`
	IdentitySnapshot string `json:"identity_snapshot"`
}

func (c *clickHouse) insert(ctx context.Context, events []Event) error {
	projections := make([]eventProjection, 0, len(events))
	for _, event := range events {
		timestamp, err := time.Parse(time.RFC3339Nano, event.Timestamp)
		if err != nil {
			return errors.New("invalid queued event timestamp")
		}
		event.Timestamp = timestamp.UTC().Format(time.RFC3339Nano)
		snapshot, err := originalSnapshot(event)
		if err != nil {
			return err
		}
		projections = append(projections, eventProjection{Event: event, OriginalSnapshot: snapshot})
	}
	return c.insertProjections(ctx, projections)
}

func (c *clickHouse) insertProjections(ctx context.Context, projections []eventProjection) error {
	ctx, release, err := c.projectionGuard.shared(ctx)
	if err != nil {
		return err
	}
	defer release()
	if len(projections) == 0 {
		return nil
	}
	var body bytes.Buffer
	encoder := json.NewEncoder(&body)
	encoder.SetEscapeHTML(false)
	for _, projection := range projections {
		event := projection.Event
		received, err := time.Parse(time.RFC3339Nano, event.ReceivedAt)
		if err != nil || event.ID <= 0 || event.ProjectID == "" || event.UUID == "" {
			return errors.New("invalid event projection identity")
		}
		row := clickHouseEvent{EventID: uint64(event.ID), ProjectID: event.ProjectID, UUID: event.UUID, ReceivedAt: received.UTC().Format(time.RFC3339Nano), ReceivedAtMS: received.UnixMilli(), Revision: projection.Revision}
		if projection.IsDeleted {
			if projection.Revision == 0 {
				return errors.New("invalid event deletion revision")
			}
			row.IsDeleted = 1
			row.Properties = "{}"
		} else {
			timestamp, err := time.Parse(time.RFC3339Nano, event.Timestamp)
			if err != nil || !json.Valid(event.Properties) || !json.Valid([]byte(projection.OriginalSnapshot)) {
				return errors.New("invalid event projection payload")
			}
			row.Event = event.Event
			row.DistinctID = event.DistinctID
			row.Properties = string(event.Properties)
			row.Timestamp = timestamp.UTC().Format(time.RFC3339Nano)
			row.TimestampMS = timestamp.UnixMilli()
			row.OriginalSnapshot = projection.OriginalSnapshot
			if event.IdentitySnapshot != "" && !json.Valid([]byte(event.IdentitySnapshot)) {
				return errors.New("invalid identity snapshot")
			}
			row.IdentitySnapshot = event.IdentitySnapshot
		}
		if err := encoder.Encode(row); err != nil {
			return errors.New("cannot encode event projection")
		}
	}
	parameters := url.Values{"query": {"INSERT INTO " + c.config.Database + "." + eventTable + " FORMAT JSONEachRow"}, "async_insert": {"0"}}
	_, err = c.request(ctx, c.config.User, c.config.Password, "", parameters, &body, 64<<10)
	return err
}

func (c *clickHouse) loadProjection(ctx context.Context, project, uuid string) (eventProjection, error) {
	sql := `SELECT event_id,project_id,uuid,event,distinct_id,properties,timestamp,timestamp_ms,received_at,received_at_ms,revision,is_deleted,original_snapshot,identity_snapshot FROM ` + c.config.Database + `.` + eventTable + ` WHERE project_id=unhex('` + hex.EncodeToString([]byte(project)) + `') AND uuid=unhex('` + hex.EncodeToString([]byte(uuid)) + `') ORDER BY revision DESC LIMIT 1 SETTINGS final=0,output_format_json_quote_64bit_integers=1 FORMAT JSONEachRow`
	data, err := c.request(ctx, c.config.User, c.config.Password, "", nil, strings.NewReader(sql), 64<<20)
	if err != nil {
		return eventProjection{}, err
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return eventProjection{}, errEventNotFound
	}
	var row struct {
		EventID          json.Number `json:"event_id"`
		ProjectID        string      `json:"project_id"`
		UUID             string      `json:"uuid"`
		Event            string      `json:"event"`
		DistinctID       string      `json:"distinct_id"`
		Properties       string      `json:"properties"`
		Timestamp        string      `json:"timestamp"`
		ReceivedAt       string      `json:"received_at"`
		Revision         json.Number `json:"revision"`
		IsDeleted        uint8       `json:"is_deleted"`
		OriginalSnapshot string      `json:"original_snapshot"`
		IdentitySnapshot string      `json:"identity_snapshot"`
	}
	if json.Unmarshal(data, &row) != nil {
		return eventProjection{}, errors.New("invalid stored event projection")
	}
	id, err := strconv.ParseUint(string(row.EventID), 10, 63)
	if err != nil || id == 0 || id > math.MaxInt64 {
		return eventProjection{}, errors.New("invalid stored event identity")
	}
	revision, err := strconv.ParseUint(string(row.Revision), 10, 64)
	if err != nil || row.IsDeleted > 1 || row.ProjectID != project || row.UUID != uuid {
		return eventProjection{}, errors.New("invalid stored event projection")
	}
	if _, err = time.Parse(time.RFC3339Nano, row.ReceivedAt); err != nil {
		return eventProjection{}, errors.New("invalid stored event receipt time")
	}
	if row.IdentitySnapshot != "" && !json.Valid([]byte(row.IdentitySnapshot)) {
		return eventProjection{}, errors.New("invalid stored identity snapshot")
	}
	if row.IsDeleted == 0 {
		if _, err = time.Parse(time.RFC3339Nano, row.Timestamp); err != nil || !json.Valid([]byte(row.Properties)) || !json.Valid([]byte(row.OriginalSnapshot)) {
			return eventProjection{}, errors.New("invalid stored event payload")
		}
	}
	return eventProjection{Event: Event{ID: int64(id), ProjectID: row.ProjectID, UUID: row.UUID, Event: row.Event, DistinctID: row.DistinctID, Properties: json.RawMessage(row.Properties), Timestamp: row.Timestamp, ReceivedAt: row.ReceivedAt, Delivered: true, IdentitySnapshot: row.IdentitySnapshot}, Revision: revision, IsDeleted: row.IsDeleted == 1, OriginalSnapshot: row.OriginalSnapshot}, nil
}
