package porcupine

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"strconv"
	"strings"
)

type EventCatalogInput struct {
	ID             string `path:"id" doc:"Project identifier"`
	Event          string `query:"event" maxLength:"500" doc:"Optional exact event name. Supplying it also discovers sampled top-level property keys."`
	Limit          int    `query:"limit" default:"20" minimum:"1" maximum:"100"`
	Offset         int    `query:"offset" default:"0" minimum:"0" maximum:"10000"`
	PropertyPrefix string `query:"property_prefix" maxLength:"200" doc:"Literal, case-sensitive property-key prefix. Requires event."`
	PropertyKind   string `query:"property_kind" default:"all" enum:"all,custom,reserved" doc:"custom excludes keys starting with $; reserved includes only those keys. Requires event when filtering."`
	PropertyLimit  int    `query:"property_limit" default:"20" minimum:"1" maximum:"100" doc:"Maximum property keys per page, independent of event-name pagination."`
	PropertyCursor string `query:"property_cursor" maxLength:"8192" doc:"Opaque next_property_cursor from the preceding page. Keep the same project, event, prefix and kind."`
	propertyAfter  *string
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
	PropertiesTruncated          bool `json:"properties_truncated" doc:"More matching keys remain after this page. Follow next_property_cursor; this is still a bounded sample, not a historical schema."`
	HistoricalPropertiesComplete bool `json:"historical_properties_complete" doc:"Always false. Sampling, key limits and name limits mean property discovery is not a complete historical schema."`
	PaginationTruncated          bool `json:"pagination_truncated" doc:"More event names exist beyond the maximum supported pagination offset."`
}

type EventCatalog struct {
	Events             []EventCatalogEvent    `json:"events"`
	Properties         []EventCatalogProperty `json:"properties"`
	Sampling           EventCatalogSampling   `json:"sampling"`
	Delivery           DeliveryStatus         `json:"delivery"`
	NextOffset         *int                   `json:"next_offset,omitempty"`
	NextPropertyCursor string                 `json:"next_property_cursor,omitempty" doc:"Continuation for matching property keys in lexical order. Reuses a rolling sample, not a pinned snapshot. Omitted after the last page."`
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
ARRAY JOIN arraySlice(arraySort(arrayFilter(k -> length(k)<=200
AND startsWith(k,{property_prefix:String})
AND ({property_kind:String}='all' OR ({property_kind:String}='custom' AND NOT startsWith(k,'$')) OR ({property_kind:String}='reserved' AND startsWith(k,'$')))
AND ({property_has_after:UInt8}=0 OR k>{property_after:String}),JSONExtractKeys(properties))),1,200) AS key
GROUP BY key ORDER BY key ASC LIMIT {property_limit:UInt32}`

type catalogPropertyCursor struct {
	Project string `json:"project"`
	Event   string `json:"event"`
	Prefix  string `json:"prefix"`
	Kind    string `json:"kind"`
	After   string `json:"after"`
}

func normalizeCatalogInput(input EventCatalogInput) (EventCatalogInput, error) {
	if input.Limit == 0 {
		input.Limit = 20
	}
	if input.PropertyLimit == 0 {
		input.PropertyLimit = 20
	}
	if input.PropertyKind == "" {
		input.PropertyKind = "all"
	}
	if input.Limit < 1 || input.Limit > 100 || input.Offset < 0 || input.Offset > 10000 || len(input.Event) > 500 ||
		input.PropertyLimit < 1 || input.PropertyLimit > 100 || len(input.PropertyPrefix) > 200 || len(input.PropertyCursor) > 8192 ||
		(input.PropertyKind != "all" && input.PropertyKind != "custom" && input.PropertyKind != "reserved") ||
		(input.Event == "" && (input.PropertyPrefix != "" || input.PropertyKind != "all" || input.PropertyCursor != "")) {
		return input, apiError(400, "invalid event catalog parameters")
	}
	input.propertyAfter = nil
	if input.PropertyCursor != "" {
		var cursor catalogPropertyCursor
		raw, err := base64.RawURLEncoding.DecodeString(input.PropertyCursor)
		if err != nil || json.Unmarshal(raw, &cursor) != nil || len(cursor.After) > 200 || cursor.Project != input.ID || cursor.Event != input.Event || cursor.Prefix != input.PropertyPrefix || cursor.Kind != input.PropertyKind {
			return input, analyticsError(400, "invalid_property_cursor", "Invalid property cursor for this catalog request", "Use next_property_cursor with the same project, event, property_prefix and property_kind. Omit it to start a new search.", "property_cursor")
		}
		input.propertyAfter = &cursor.After
	}
	return input, nil
}

func catalogPropertyEligible(input EventCatalogInput, key string) bool {
	return len(key) <= 200 && strings.HasPrefix(key, input.PropertyPrefix) &&
		(input.PropertyKind == "all" || (input.PropertyKind == "reserved") == strings.HasPrefix(key, "$")) &&
		(input.propertyAfter == nil || key > *input.propertyAfter)
}

func finishCatalogProperties(result *EventCatalog, input EventCatalogInput) {
	if len(result.Properties) <= input.PropertyLimit {
		return
	}
	result.Properties = result.Properties[:input.PropertyLimit]
	result.Sampling.PropertiesTruncated = true
	cursor := catalogPropertyCursor{Project: input.ID, Event: input.Event, Prefix: input.PropertyPrefix, Kind: input.PropertyKind, After: result.Properties[len(result.Properties)-1].Key}
	raw, _ := json.Marshal(cursor)
	result.NextPropertyCursor = base64.RawURLEncoding.EncodeToString(raw)
}

func (s *Service) eventCatalog(ctx context.Context, input *EventCatalogInput) (*EventCatalogOutput, error) {
	options, err := normalizeCatalogInput(*input)
	if err != nil {
		return nil, err
	}
	input = &options
	limit := input.Limit
	if err := s.analyticsProject(ctx, input.ID); err != nil {
		return nil, err
	}
	if s.lake != nil {
		return s.lakeEventCatalog(ctx, *input)
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
		MaxPropertyNameBytes: 200, PropertyLimit: input.PropertyLimit,
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
		parameters := map[string]string{"event": input.Event, "property_prefix": input.PropertyPrefix, "property_kind": input.PropertyKind, "property_limit": strconv.Itoa(input.PropertyLimit + 1), "property_has_after": "0", "property_after": ""}
		if input.propertyAfter != nil {
			parameters["property_has_after"] = "1"
			parameters["property_after"] = *input.propertyAfter
		}
		data, err := s.ch.query(ctx, reader, password, namespace, catalogPropertiesSQL, parameters)
		if err != nil {
			return nil, s.analyticsStorageError(ctx, input.ID, err)
		}
		properties, err := decodeCatalogRows[EventCatalogProperty](data, input.PropertyLimit+1)
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
		finishCatalogProperties(&result, *input)
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
