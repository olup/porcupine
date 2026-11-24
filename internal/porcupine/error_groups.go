package porcupine

import (
	"context"
	"encoding/json"
	"regexp"
	"strconv"
	"time"

	"github.com/olup/porcupine/internal/apierr"
	"github.com/olup/porcupine/internal/errtrack"
	"github.com/olup/porcupine/internal/lake"
)

type ErrorGroupWindowInput struct {
	ID     string `path:"id" doc:"Project identifier"`
	From   string `query:"from" format:"date-time" doc:"Inclusive RFC3339 occurrence time. Supply with to. Defaults to seven days before to."`
	To     string `query:"to" format:"date-time" doc:"Exclusive RFC3339 occurrence time. Supply with from. Defaults to the current time. Maximum window is 31 days."`
	Limit  int    `query:"limit" default:"20" minimum:"1" maximum:"100"`
	Offset int    `query:"offset" default:"0" minimum:"0" maximum:"10000"`
}

type ErrorGroupsInput struct {
	ErrorGroupWindowInput
	Sort   string `query:"sort" default:"recent" enum:"recent,count" doc:"Sort by latest occurrence or descending count."`
	Search string `query:"q" maxLength:"128" doc:"Case-insensitive literal substring of an exception type or message in the group."`
}

type ErrorGroupInput struct {
	ErrorGroupWindowInput
	Fingerprint string `path:"fingerprint" doc:"Lowercase hexadecimal exception fingerprint."`
}

type ErrorGroupSummary struct {
	Fingerprint   string `json:"fingerprint"`
	Type          string `json:"type,omitempty"`
	Message       string `json:"message,omitempty"`
	Count         string `json:"count" doc:"Occurrence count as a decimal string."`
	FirstSeen     string `json:"first_seen" format:"date-time"`
	LastSeen      string `json:"last_seen" format:"date-time"`
	LatestEventID string `json:"latest_event_id" doc:"Decimal analytics event ID."`
}

type ErrorGroupOccurrence struct {
	EventID    string `json:"event_id" doc:"Decimal analytics event ID."`
	OccurredAt string `json:"occurred_at" format:"date-time"`
	BuildID    string `json:"build_id,omitempty"`
	Release    string `json:"release,omitempty"`
	Degraded   bool   `json:"degraded,omitempty"`
}

type ErrorGroupBucket struct {
	Start string `json:"start" format:"date-time"`
	Count string `json:"count" doc:"Occurrence count as a decimal string."`
}

type ErrorGroupsOutput struct {
	Body struct {
		From       time.Time           `json:"from"`
		To         time.Time           `json:"to"`
		Groups     []ErrorGroupSummary `json:"groups"`
		NextOffset *int                `json:"next_offset,omitempty"`
	}
}

type ErrorGroupOutput struct {
	Body struct {
		From          time.Time              `json:"from"`
		To            time.Time              `json:"to"`
		Group         ErrorGroupSummary      `json:"group"`
		BucketSeconds int                    `json:"bucket_seconds"`
		Distribution  []ErrorGroupBucket     `json:"distribution"`
		Occurrences   []ErrorGroupOccurrence `json:"occurrences"`
		NextOffset    *int                   `json:"next_offset,omitempty"`
	}
}

const errorGroupFingerprintPattern = "[0-9a-f]{64}"

var errorGroupFingerprint = regexp.MustCompile("^" + errorGroupFingerprintPattern + "$")

func (input ErrorGroupWindowInput) window(now time.Time) (lake.QueryTimeRange, error) {
	if input.From == "" && input.To == "" {
		return lake.QueryTimeRange{From: now.UTC().Add(-7 * 24 * time.Hour), To: now.UTC()}, nil
	}
	from, fromErr := time.Parse(time.RFC3339Nano, input.From)
	to, toErr := time.Parse(time.RFC3339Nano, input.To)
	if fromErr != nil || toErr != nil || !from.Before(to) || to.Sub(from) > 31*24*time.Hour ||
		from.UTC().Year() < 0 || to.UTC().Year() > 9999 {
		return lake.QueryTimeRange{}, apierr.New(400, "from and to must specify an increasing RFC3339 window of at most 31 days")
	}
	return lake.QueryTimeRange{From: from.UTC(), To: to.UTC()}, nil
}

func (input ErrorGroupWindowInput) pageParameters() map[string]string {
	return map[string]string{"limit": strconv.Itoa(input.Limit + 1), "offset": strconv.Itoa(input.Offset)}
}

func (input ErrorGroupWindowInput) nextOffset(rows int) *int {
	next := input.Offset + input.Limit
	if rows <= input.Limit || next > 10000 {
		return nil
	}
	return &next
}

func (s *Service) listErrorGroups(ctx context.Context, input *ErrorGroupsInput) (*ErrorGroupsOutput, error) {
	window, err := input.window(s.clock())
	if err != nil {
		return nil, err
	}
	if len(input.Search) > 128 {
		return nil, apierr.New(400, "q must be at most 128 bytes")
	}
	params := input.pageParameters()
	params["q"] = input.Search
	order := "last_key DESC, fingerprint"
	if input.Sort == "count" {
		order = "matches DESC, " + order
	}
	data, maxBytes, err := s.readErrorGroups(ctx, input.ID, errorGroupsListSQL+order+errorGroupPageSQL, params, &window)
	if err != nil {
		return nil, err
	}
	output := &ErrorGroupsOutput{}
	output.Body.From, output.Body.To = window.From, window.To
	output.Body.Groups = make([]ErrorGroupSummary, 0, min(input.Limit, len(data)))
	for _, row := range data[:min(input.Limit, len(data))] {
		var group ErrorGroupSummary
		if err := decodeErrorGroupRow(row, &group); err != nil {
			return nil, err
		}
		output.Body.Groups = append(output.Body.Groups, safeErrorGroupSummary(group))
	}
	output.Body.NextOffset = input.nextOffset(len(data))
	return output, checkErrorGroupsSize(output.Body, maxBytes)
}

func (s *Service) getErrorGroup(ctx context.Context, input *ErrorGroupInput) (*ErrorGroupOutput, error) {
	if !errorGroupFingerprint.MatchString(input.Fingerprint) {
		return nil, apierr.New(400, "fingerprint must be 64 lowercase hexadecimal characters")
	}
	window, err := input.window(s.clock())
	if err != nil {
		return nil, err
	}
	duration, unit := time.Hour, "hour"
	if window.To.Sub(window.From) > 48*time.Hour {
		duration, unit = 24*time.Hour, "day"
	}
	params := input.pageParameters()
	params["fingerprint"] = input.Fingerprint
	params["bucket"] = unit
	data, maxBytes, err := s.readErrorGroups(ctx, input.ID, errorGroupDetailSQL, params, &window)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, apierr.New(404, "error group not found in the requested window")
	}
	output := &ErrorGroupOutput{}
	output.Body.From, output.Body.To = window.From, window.To
	output.Body.BucketSeconds = int(duration / time.Second)
	output.Body.Occurrences = []ErrorGroupOccurrence{}
	buckets := map[string]string{}
	for _, row := range data {
		switch string(row["kind"]) {
		case `"summary"`:
			if err := decodeErrorGroupRow(row, &output.Body.Group); err != nil {
				return nil, err
			}
			output.Body.Group = safeErrorGroupSummary(output.Body.Group)
		case `"bucket"`:
			var bucket ErrorGroupBucket
			if err := decodeErrorGroupRow(row, &bucket); err != nil {
				return nil, err
			}
			buckets[bucket.Start] = bucket.Count
		case `"occurrence"`:
			var occurrence ErrorGroupOccurrence
			if err := decodeErrorGroupRow(row, &occurrence); err != nil {
				return nil, err
			}
			output.Body.Occurrences = append(output.Body.Occurrences, occurrence)
		default:
			return nil, apierr.New(503, "invalid error group query result")
		}
	}
	for start := window.From.Truncate(duration); start.Before(window.To); start = start.Add(duration) {
		at := start.Format(time.RFC3339)
		count := buckets[at]
		if count == "" {
			count = "0"
		}
		output.Body.Distribution = append(output.Body.Distribution, ErrorGroupBucket{Start: at, Count: count})
	}
	output.Body.NextOffset = input.nextOffset(len(output.Body.Occurrences))
	if len(output.Body.Occurrences) > input.Limit {
		output.Body.Occurrences = output.Body.Occurrences[:input.Limit]
	}
	return output, checkErrorGroupsSize(output.Body, maxBytes)
}

func (s *Service) readErrorGroups(ctx context.Context, project, statement string, params map[string]string, window *lake.QueryTimeRange) ([]map[string]json.RawMessage, int, error) {
	if err := s.analyticsProject(ctx, project); err != nil {
		return nil, 0, err
	}
	reader := s.interactiveReader()
	if reader == nil {
		return nil, 0, apierr.New(503, "analytics storage is not configured")
	}
	result, bounds, err := s.queries.SQL(ctx, reader, project, statement, params, window)
	return result.Data, bounds.MaxBytes, err
}

func decodeErrorGroupRow(row map[string]json.RawMessage, target any) error {
	raw, err := json.Marshal(row)
	if err != nil || json.Unmarshal(raw, target) != nil {
		return apierr.New(503, "invalid error group query result")
	}
	return nil
}

func safeErrorGroupSummary(group ErrorGroupSummary) ErrorGroupSummary {
	properties := errtrack.Normalize(map[string]any{
		"$exception_list": []any{map[string]any{"type": group.Type, "value": group.Message}},
	})
	if len(properties.List) == 0 {
		group.Type, group.Message = "", ""
	} else {
		group.Type, group.Message = properties.List[0].Type, properties.List[0].Value
	}
	return group
}

func checkErrorGroupsSize(body any, maxBytes int) error {
	encoded, err := json.Marshal(body)
	if err != nil || len(encoded) > maxBytes {
		return apierr.New(429, "error group result byte limit reached")
	}
	return nil
}
