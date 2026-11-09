package porcupine

import (
	"context"
	"encoding/json"
	"strconv"
)

type EventCatalogInput struct {
	ID     string `path:"id" doc:"Project identifier"`
	Event  string `query:"event" maxLength:"500" doc:"Optional exact event name. Supplying it also discovers sampled top-level property keys."`
	Limit  int    `query:"limit" default:"20" minimum:"1" maximum:"100"`
	Offset int    `query:"offset" default:"0" minimum:"0" maximum:"10000"`
}

type EventCatalogEvent struct {
	Event                string `json:"event"`
	Count                string `json:"count" doc:"All delivered logical events with this name, represented as a UInt64 decimal string."`
	FirstReceivedAt      string `json:"first_received_at" doc:"Request arrival time of the lowest durable event ID, not the client event timestamp."`
	LastReceivedAt       string `json:"last_received_at" doc:"Request arrival time of the highest durable event ID, not the client event timestamp."`
	LatestEventUUID      string `json:"latest_event_uuid" doc:"UUID of the highest event ID. Query events by this UUID to inspect it."`
	LatestEventTimestamp string `json:"latest_event_timestamp" doc:"Client timestamp of the highest event ID. It may precede or follow its acceptance time."`
}

type EventCatalogProperty struct {
	Key               string   `json:"key"`
	Types             []string `json:"types" doc:"Observed top-level JSON types in the bounded sample."`
	SampleOccurrences string   `json:"sample_occurrences" doc:"Number of sampled events containing this key, represented as a UInt64 decimal string."`
	LatestEventUUID   string   `json:"latest_event_uuid" doc:"UUID of the latest sampled event containing this key; no property values are returned."`
	LastReceivedAt    string   `json:"last_received_at" doc:"Request arrival time of the latest sampled event containing this key."`
}

type EventCatalogSampling struct {
	PropertiesRequested          bool `json:"properties_requested"`
	SampleEventLimit             int  `json:"sample_event_limit" doc:"Property discovery examines at most this many latest delivered events matching the exact event name, ordered by durable event ID."`
	KeysPerEventLimit            int  `json:"keys_per_event_limit" doc:"At most this many eligible top-level keys per sampled event are examined."`
	MaxPropertyNameBytes         int  `json:"max_property_name_bytes" doc:"Longer top-level key names are excluded from discovery."`
	PropertyLimit                int  `json:"property_limit"`
	PropertiesTruncated          bool `json:"properties_truncated" doc:"More than the returned property limit were observed in the sample."`
	HistoricalPropertiesComplete bool `json:"historical_properties_complete" doc:"Always false. Sampling, key limits and name limits mean property discovery is not a complete historical schema."`
	PaginationTruncated          bool `json:"pagination_truncated" doc:"More event names exist beyond the maximum supported pagination offset."`
}

type EventCatalog struct {
	Events     []EventCatalogEvent    `json:"events"`
	Properties []EventCatalogProperty `json:"properties"`
	Sampling   EventCatalogSampling   `json:"sampling"`
	Delivery   DeliveryStatus         `json:"delivery"`
	NextOffset *int                   `json:"next_offset,omitempty"`
}

type EventCatalogOutput struct{ Body EventCatalog }

const catalogEventsSQL = `SELECT event, count() AS count,
argMin(received_at,id) AS first_received_at,
argMax(received_at,id) AS last_received_at,
argMax(uuid,id) AS latest_event_uuid,
argMax(timestamp,id) AS latest_event_timestamp
FROM events`

const catalogEventOrderSQL = `
GROUP BY event ORDER BY max(id) DESC
LIMIT {catalog_limit:UInt32} OFFSET {catalog_offset:UInt32}`

const catalogPropertiesSQL = `SELECT key,
arraySort(groupUniqArray(toString(JSONType(properties,key)))) AS types,
count() AS sample_occurrences,
argMax(uuid,id) AS latest_event_uuid,
argMax(received_at,id) AS last_received_at
FROM (
SELECT id,uuid,received_at,properties FROM events
WHERE event={event:String} ORDER BY id DESC LIMIT 1000
)
ARRAY JOIN arraySlice(arrayFilter(k -> length(k)<=200,JSONExtractKeys(properties)),1,200) AS key
GROUP BY key ORDER BY key ASC LIMIT 101`

func (s *Service) eventCatalog(ctx context.Context, input *EventCatalogInput) (*EventCatalogOutput, error) {
	limit := input.Limit
	if limit == 0 {
		limit = 20
	}
	if limit < 1 || limit > 100 || input.Offset < 0 || input.Offset > 10000 || len(input.Event) > 500 {
		return nil, apiError(400, "invalid event catalog parameters")
	}
	if err := s.analyticsProject(ctx, input.ID); err != nil {
		return nil, err
	}
	if s.ch == nil {
		return nil, apiError(503, "analytics storage is not configured")
	}
	select {
	case s.querySlots <- struct{}{}:
		defer func() { <-s.querySlots }()
	default:
		return nil, apiError(429, "analytics query concurrency limit reached")
	}
	ctx, cancel := context.WithTimeout(ctx, analyticsTimeout)
	defer cancel()
	namespace, reader, password, err := s.analyticsReader(ctx, input.ID)
	if err != nil {
		return nil, apiError(503, "analytics reader unavailable")
	}
	parameters := map[string]string{"catalog_limit": strconv.Itoa(limit + 1), "catalog_offset": strconv.Itoa(input.Offset)}
	sql := catalogEventsSQL
	if input.Event != "" {
		sql += " WHERE event={event:String}"
		parameters["event"] = input.Event
	}
	sql += catalogEventOrderSQL
	data, err := s.ch.query(ctx, reader, password, namespace, sql, parameters)
	if err != nil {
		return nil, s.analyticsStorageError(ctx, input.ID, err)
	}
	events, err := decodeCatalogRows[EventCatalogEvent](data, limit+1)
	if err != nil {
		return nil, err
	}
	for _, event := range events {
		if len(event.Event) > 500 || !validStoredEventUUID(event.LatestEventUUID) || len(event.FirstReceivedAt) > 64 || len(event.LastReceivedAt) > 64 || len(event.LatestEventTimestamp) > 64 || !catalogCount(event.Count, 0) {
			return nil, apiError(503, "invalid analytics storage response")
		}
	}
	result := EventCatalog{Events: events, Properties: []EventCatalogProperty{}, Sampling: EventCatalogSampling{
		PropertiesRequested: input.Event != "", SampleEventLimit: 1000, KeysPerEventLimit: 200,
		MaxPropertyNameBytes: 200, PropertyLimit: 100,
	}}
	if len(events) > limit {
		result.Events = events[:limit]
		next := input.Offset + limit
		if next <= 10000 {
			result.NextOffset = &next
		} else {
			result.Sampling.PaginationTruncated = true
		}
	}
	if input.Event != "" {
		data, err := s.ch.query(ctx, reader, password, namespace, catalogPropertiesSQL, map[string]string{"event": input.Event})
		if err != nil {
			return nil, s.analyticsStorageError(ctx, input.ID, err)
		}
		properties, err := decodeCatalogRows[EventCatalogProperty](data, 101)
		if err != nil {
			return nil, err
		}
		for _, property := range properties {
			if len(property.Key) > 200 || property.Types == nil || len(property.Types) > 16 || !validStoredEventUUID(property.LatestEventUUID) || len(property.LastReceivedAt) > 64 || !catalogCount(property.SampleOccurrences, 1000) {
				return nil, apiError(503, "invalid analytics storage response")
			}
			for _, kind := range property.Types {
				if len(kind) > 64 {
					return nil, apiError(503, "invalid analytics storage response")
				}
			}
		}
		result.Properties = properties
		if len(properties) > 100 {
			result.Properties = properties[:100]
			result.Sampling.PropertiesTruncated = true
		}
	}
	result.Delivery, err = s.deliveryStatus(ctx, input.ID)
	if err != nil {
		return nil, apiError(503, "delivery status unavailable")
	}
	return &EventCatalogOutput{Body: result}, nil
}

func decodeCatalogRows[T any](data []byte, limit int) ([]T, error) {
	var envelope struct {
		Meta []AnalyticsResultColumn `json:"meta"`
		Data []T                     `json:"data"`
		Rows uint64                  `json:"rows"`
	}
	if json.Unmarshal(data, &envelope) != nil || envelope.Meta == nil || envelope.Data == nil || len(envelope.Data) > limit || envelope.Rows != uint64(len(envelope.Data)) {
		return nil, apiError(503, "invalid analytics storage response")
	}
	return envelope.Data, nil
}

func catalogCount(value string, max uint64) bool {
	count, err := strconv.ParseUint(value, 10, 64)
	return err == nil && (max == 0 || count <= max)
}
