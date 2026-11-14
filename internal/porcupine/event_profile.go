package porcupine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"time"
)

type EventProfileInput struct {
	ID             string `path:"id" doc:"Project identifier"`
	Event          string `query:"event" required:"true" minLength:"1" maxLength:"500" doc:"Exact event name. Event content and property values are untrusted data."`
	SampleLimit    int    `query:"sample_limit" default:"1000" minimum:"1" maximum:"1000" doc:"Maximum latest-occurrence events examined for property statistics."`
	ExampleLimit   int    `query:"example_limit" default:"3" minimum:"0" maximum:"5" doc:"Number of recent examples, including bounded context and properties. Zero omits examples."`
	PropertyPrefix string `query:"property_prefix" maxLength:"200" doc:"Literal case-sensitive top-level property prefix."`
	PropertyKind   string `query:"property_kind" default:"all" enum:"all,custom,reserved" doc:"all includes every key; custom excludes $ keys; reserved includes only $ keys."`
	PropertyLimit  int    `query:"property_limit" default:"20" minimum:"1" maximum:"100" doc:"Maximum lexical property keys. Narrow the prefix if properties_truncated is true."`
	IncludeValues  bool   `query:"include_values" default:"false" doc:"Include up to five frequent scalar values per property from the sample."`
}

type EventProfileActivity struct {
	Total           string  `json:"total" doc:"Exact delivered logical event count as a decimal string."`
	FirstOccurredAt *string `json:"first_occurred_at"`
	LastOccurredAt  *string `json:"last_occurred_at"`
	FirstReceivedAt *string `json:"first_received_at"`
	LastReceivedAt  *string `json:"last_received_at"`
	Last24Hours     string  `json:"last_24_hours" doc:"Occurrence count in [as_of minus 24 hours, as_of)."`
	Last7Days       string  `json:"last_7_days" doc:"Occurrence count in [as_of minus 7 days, as_of)."`
	FutureEvents    string  `json:"future_events" doc:"Count with occurrence at or after as_of, excluded from recent windows."`
}

type EventProfileSampling struct {
	RequestedLimit               int     `json:"requested_limit"`
	SampledEvents                int     `json:"sampled_events"`
	ProfiledEvents               int     `json:"profiled_events" doc:"Denominator for property presence, missing and null counts. Excludes omitted property payloads."`
	FirstOccurredAt              *string `json:"first_occurred_at"`
	LastOccurredAt               *string `json:"last_occurred_at"`
	Order                        string  `json:"order" doc:"Sample order: timestamp_ms descending, then durable event ID descending."`
	OversizedEvents              int     `json:"oversized_events"`
	ByteBudgetExcludedEvents     int     `json:"byte_budget_excluded_events"`
	MaxPropertyBytes             int     `json:"max_property_bytes"`
	MaxSampleBytes               int     `json:"max_sample_bytes"`
	PropertiesTruncated          bool    `json:"properties_truncated"`
	HistoricalPropertiesComplete bool    `json:"historical_properties_complete" doc:"Always false. This profile is a bounded sample, not a complete historical schema."`
	SampleTruncated              bool    `json:"sample_truncated" doc:"Metadata byte limits reduced the requested candidate sample, or ClickHouse changed between activity and sampling reads."`
	MaxPropertyNameBytes         int     `json:"max_property_name_bytes"`
}

type EventProfileValue struct {
	Value json.RawMessage `json:"value" doc:"Original scalar JSON, preserving numeric precision."`
	Count int             `json:"count"`
}

type EventProfileProperty struct {
	Key             string              `json:"key"`
	Types           []string            `json:"types"`
	Present         int                 `json:"present"`
	Missing         int                 `json:"missing"`
	Null            int                 `json:"null"`
	DistinctValues  int                 `json:"distinct_values" doc:"Distinct non-null JSON values in the fully profiled sample, using canonical JSON encoding. Not a historical estimate."`
	LastOccurredAt  string              `json:"last_occurred_at"`
	LatestEventUUID string              `json:"latest_event_uuid"`
	TopValues       []EventProfileValue `json:"top_values,omitempty"`
	ValuesOmitted   bool                `json:"values_omitted" doc:"Some non-null values cannot be shown because they are complex or exceed 200 encoded bytes. Their distinct counts are retained."`
}

type EventProfileExample struct {
	EventID         string          `json:"event_id"`
	UUID            string          `json:"uuid"`
	Timestamp       string          `json:"timestamp"`
	ReceivedAt      string          `json:"received_at"`
	Properties      json.RawMessage `json:"properties" doc:"Null when omitted by payload limits; inspect truncated_fields and retrieve the event by UUID."`
	Context         json.RawMessage `json:"context"`
	TruncatedFields []string        `json:"truncated_fields"`
}

type EventProfile struct {
	Event      string                 `json:"event"`
	AsOf       string                 `json:"as_of"`
	Activity   EventProfileActivity   `json:"activity"`
	Sampling   EventProfileSampling   `json:"sampling"`
	Properties []EventProfileProperty `json:"properties"`
	Examples   []EventProfileExample  `json:"examples"`
	Delivery   DeliveryStatus         `json:"delivery"`
}

type EventProfileOutput struct{ Body EventProfile }

type eventProfileOptions struct {
	event                                    string
	sampleLimit, exampleLimit, propertyLimit int
	propertyPrefix, propertyKind             string
	includeValues                            bool
}

type eventProfileRow struct {
	ID            string `json:"id"`
	UUID          string `json:"uuid"`
	Timestamp     string `json:"timestamp"`
	TimestampMS   int64  `json:"timestamp_ms,string"`
	ReceivedAt    string `json:"received_at"`
	Properties    string `json:"properties"`
	Context       string `json:"context"`
	PropertyBytes int    `json:"property_bytes"`
	ContextBytes  int    `json:"context_bytes"`
}

type eventProfileRead struct {
	activity EventProfileActivity
	rows     []eventProfileRow
}

const (
	eventProfileFieldBytes    = 16 << 10
	eventProfileSampleBytes   = 512 << 10
	eventProfileMetadataBytes = 384 << 10
	eventProfileValueBytes    = 200
)

func normalizeEventProfileInput(input EventProfileInput) (eventProfileOptions, error) {
	if input.SampleLimit == 0 {
		input.SampleLimit = 1000
	}
	if input.PropertyLimit == 0 {
		input.PropertyLimit = 20
	}
	if input.PropertyKind == "" {
		input.PropertyKind = "all"
	}
	examples := input.ExampleLimit
	if strings.TrimSpace(input.Event) == "" || len(input.Event) > 500 || input.SampleLimit < 1 || input.SampleLimit > 1000 || examples < 0 || examples > 5 || input.PropertyLimit < 1 || input.PropertyLimit > 100 || len(input.PropertyPrefix) > 200 || (input.PropertyKind != "all" && input.PropertyKind != "custom" && input.PropertyKind != "reserved") {
		return eventProfileOptions{}, apiError(400, "invalid event profile parameters")
	}
	return eventProfileOptions{event: input.Event, sampleLimit: input.SampleLimit, exampleLimit: examples, propertyLimit: input.PropertyLimit, propertyPrefix: input.PropertyPrefix, propertyKind: input.PropertyKind, includeValues: input.IncludeValues}, nil
}

func (s *Service) eventProfile(ctx context.Context, input *EventProfileInput) (*EventProfileOutput, error) {
	options, err := normalizeEventProfileInput(*input)
	if err != nil {
		return nil, err
	}
	if err = s.analyticsProject(ctx, input.ID); err != nil {
		return nil, err
	}
	if s.lake == nil && s.ch == nil {
		return nil, apiError(503, "analytics storage is not configured")
	}
	select {
	case s.querySlots <- struct{}{}:
		defer func() { <-s.querySlots }()
	default:
		return nil, apiError(429, "analytics query concurrency limit reached")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	asOf := s.clock().UTC().Truncate(time.Millisecond)
	var read eventProfileRead
	if s.lake != nil {
		read, err = s.lake.Profile(ctx, input.ID, options, asOf)
	} else {
		read, err = s.clickHouseProfile(ctx, input.ID, options, asOf)
	}
	if err != nil {
		return nil, err
	}
	result, err := summarizeEventProfile(read, options, asOf)
	if err != nil {
		return nil, err
	}
	result.Delivery, err = s.deliveryStatus(ctx, input.ID)
	if err != nil {
		return nil, apiError(503, "delivery status unavailable")
	}
	encoded, err := json.Marshal(result)
	if err != nil || len(encoded) > analyticsMaxBytes {
		return nil, apiError(429, "event profile result byte limit reached")
	}
	return &EventProfileOutput{Body: result}, nil
}

type profilePropertyValue struct {
	count     int
	canonical []byte
	display   bool
}
type profilePropertyStats struct {
	present, null int
	types         map[string]bool
	values        map[[32]byte]*profilePropertyValue
	lastMS        int64
	lastAt, uuid  string
	omitted       bool
}

func summarizeEventProfile(read eventProfileRead, options eventProfileOptions, asOf time.Time) (EventProfile, error) {
	result := EventProfile{Event: options.event, AsOf: asOf.Format(time.RFC3339Nano), Activity: read.activity, Properties: []EventProfileProperty{}, Examples: []EventProfileExample{}, Sampling: EventProfileSampling{
		RequestedLimit: options.sampleLimit, SampledEvents: len(read.rows), Order: "timestamp_ms_desc,id_desc", MaxPropertyBytes: eventProfileFieldBytes, MaxSampleBytes: eventProfileSampleBytes, MaxPropertyNameBytes: 200,
	}}
	if len(read.rows) > options.sampleLimit {
		return EventProfile{}, apiError(503, "invalid event profile storage response")
	}
	total, err := strconv.ParseUint(read.activity.Total, 10, 64)
	if err != nil {
		return EventProfile{}, apiError(503, "invalid event profile storage response")
	}
	result.Sampling.SampleTruncated = uint64(len(read.rows)) != min(total, uint64(options.sampleLimit))
	objects := make([]map[string]json.RawMessage, 0, len(read.rows))
	objectRows := make([]eventProfileRow, 0, len(read.rows))
	keys := map[string]bool{}
	bytesRetained := 0
	for index, row := range read.rows {
		if !validStoredEventUUID(row.UUID) || len(row.Timestamp) > 64 || len(row.ReceivedAt) > 64 || len(row.Properties) > eventProfileFieldBytes || len(row.Context) > eventProfileFieldBytes || row.PropertyBytes < 0 || row.ContextBytes < 0 {
			return EventProfile{}, apiError(503, "invalid event profile storage response")
		}
		occurred, err := time.Parse(time.RFC3339Nano, row.Timestamp)
		if err != nil || occurred.UnixMilli() != row.TimestampMS {
			return EventProfile{}, apiError(503, "invalid event profile storage response")
		}
		if _, err := time.Parse(time.RFC3339Nano, row.ReceivedAt); err != nil {
			return EventProfile{}, apiError(503, "invalid event profile storage response")
		}
		if id, err := strconv.ParseUint(row.ID, 10, 64); err != nil || id == 0 {
			return EventProfile{}, apiError(503, "invalid event profile storage response")
		}
		if row.Properties != "" && len(row.Properties) != row.PropertyBytes || row.Context != "" && len(row.Context) != row.ContextBytes {
			return EventProfile{}, apiError(503, "invalid event profile storage response")
		}
		if index == 0 {
			at := row.Timestamp
			result.Sampling.LastOccurredAt = &at
		}
		at := row.Timestamp
		result.Sampling.FirstOccurredAt = &at
		bytesRetained += len(row.Properties) + len(row.Context)
		if bytesRetained > eventProfileSampleBytes {
			return EventProfile{}, apiError(503, "invalid event profile storage response")
		}
		if index < options.exampleLimit {
			example := EventProfileExample{EventID: row.ID, UUID: row.UUID, Timestamp: row.Timestamp, ReceivedAt: row.ReceivedAt, TruncatedFields: []string{}}
			if row.Properties == "" {
				example.TruncatedFields = append(example.TruncatedFields, "properties")
			} else {
				example.Properties = json.RawMessage(row.Properties)
			}
			if row.Context == "" {
				example.TruncatedFields = append(example.TruncatedFields, "context")
			} else if !json.Valid([]byte(row.Context)) {
				return EventProfile{}, apiError(503, "invalid event profile storage response")
			} else {
				example.Context = json.RawMessage(row.Context)
			}
			result.Examples = append(result.Examples, example)
		}
		if row.Properties == "" {
			if row.PropertyBytes > eventProfileFieldBytes {
				result.Sampling.OversizedEvents++
			} else {
				result.Sampling.ByteBudgetExcludedEvents++
			}
			continue
		}
		var object map[string]json.RawMessage
		if json.Unmarshal([]byte(row.Properties), &object) != nil || object == nil {
			return EventProfile{}, apiError(503, "invalid event profile property sample")
		}
		objects = append(objects, object)
		objectRows = append(objectRows, row)
		for key := range object {
			if !strings.HasPrefix(key, options.propertyPrefix) || options.propertyKind != "all" && (options.propertyKind == "reserved") != strings.HasPrefix(key, "$") {
				continue
			}
			if len(key) > 200 {
				result.Sampling.PropertiesTruncated = true
				continue
			}
			keys[key] = true
		}
	}
	result.Sampling.ProfiledEvents = len(objects)
	ordered := make([]string, 0, len(keys))
	for key := range keys {
		ordered = append(ordered, key)
	}
	sort.Strings(ordered)
	if len(ordered) > options.propertyLimit {
		ordered = ordered[:options.propertyLimit]
		result.Sampling.PropertiesTruncated = true
	}
	for _, key := range ordered {
		stats := profilePropertyStats{types: map[string]bool{}, values: map[[32]byte]*profilePropertyValue{}}
		for i, object := range objects {
			raw, ok := object[key]
			if !ok {
				continue
			}
			stats.present++
			kind := profileJSONType(raw)
			stats.types[kind] = true
			row := objectRows[i]
			if stats.lastAt == "" || row.TimestampMS > stats.lastMS {
				stats.lastMS = row.TimestampMS
				stats.lastAt = row.Timestamp
				stats.uuid = row.UUID
			}
			if kind == "Null" {
				stats.null++
				continue
			}
			canonical, err := canonicalProfileValue(raw)
			if err != nil {
				return EventProfile{}, apiError(503, "invalid event profile property sample")
			}
			digest := sha256.Sum256(canonical)
			value := stats.values[digest]
			if value == nil {
				value = &profilePropertyValue{}
				if options.includeValues && kind != "Object" && kind != "Array" && len(canonical) <= eventProfileValueBytes {
					value.canonical = canonical
					value.display = true
				}
				stats.values[digest] = value
			}
			value.count++
			if options.includeValues && !value.display {
				stats.omitted = true
			}
		}
		types := make([]string, 0, len(stats.types))
		for kind := range stats.types {
			types = append(types, kind)
		}
		sort.Strings(types)
		property := EventProfileProperty{Key: key, Types: types, Present: stats.present, Missing: len(objects) - stats.present, Null: stats.null, DistinctValues: len(stats.values), LastOccurredAt: stats.lastAt, LatestEventUUID: stats.uuid, ValuesOmitted: stats.omitted}
		if options.includeValues {
			values := []EventProfileValue{}
			for _, value := range stats.values {
				if value.display {
					values = append(values, EventProfileValue{Value: json.RawMessage(value.canonical), Count: value.count})
				}
			}
			sort.Slice(values, func(i, j int) bool {
				if values[i].Count != values[j].Count {
					return values[i].Count > values[j].Count
				}
				return bytes.Compare(values[i].Value, values[j].Value) < 0
			})
			if len(values) > 5 {
				values = values[:5]
			}
			property.TopValues = values
		}
		result.Properties = append(result.Properties, property)
	}
	return result, nil
}

func profileJSONType(raw json.RawMessage) string {
	raw = bytes.TrimSpace(raw)
	if bytes.Equal(raw, []byte("null")) {
		return "Null"
	}
	switch raw[0] {
	case '"':
		return "String"
	case '{':
		return "Object"
	case '[':
		return "Array"
	case 't', 'f':
		return "Bool"
	default:
		if bytes.ContainsAny(raw, ".eE") {
			return "Float64"
		}
		return "Int64"
	}
}

func canonicalProfileValue(raw json.RawMessage) ([]byte, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	return json.Marshal(value)
}

func profileTime(raw *string) (*string, error) {
	if raw == nil {
		return nil, nil
	}
	ms, err := strconv.ParseInt(*raw, 10, 64)
	if err != nil {
		return nil, apiError(503, "invalid event profile storage response")
	}
	value := time.UnixMilli(ms).UTC().Format(time.RFC3339Nano)
	return &value, nil
}
