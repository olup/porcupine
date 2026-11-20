package porcupine

import (
	"encoding/json"
	"strconv"
	"time"
)

type clientSentAt struct {
	raw   string
	value time.Time
	valid bool
}

func sentAtString(raw json.RawMessage) string {
	var value string
	_ = json.Unmarshal(raw, &value)
	return value
}

func parseClientSentAt(values ...string) (result clientSentAt) {
	for _, raw := range values {
		if raw == "" || len(raw) > 128 {
			continue
		}
		if result.raw == "" {
			result.raw = raw
		}
		value, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			millis, parseErr := strconv.ParseInt(raw, 10, 64)
			if parseErr != nil {
				continue
			}
			value = time.UnixMilli(millis).UTC()
		}
		if value.Year() >= 0 && value.Year() <= 9999 {
			return clientSentAt{raw: raw, value: value, valid: true}
		}
	}
	return result
}

func normalizeLiveTimestamp(input eventInput, receipt time.Time) (string, string, error) {
	timestamp, reason := receipt, ""
	var err error
	if input.Timestamp != "" {
		timestamp, err = time.Parse(time.RFC3339Nano, input.Timestamp)
	}
	if input.ingestion == nil || input.sentAt.valid {
		if ignore, _ := input.Properties["$ignore_sent_at"].(bool); !ignore || input.ingestion != nil {
			offset, offsetErr := strconv.ParseInt(string(input.Offset), 10, 64)
			switch {
			case input.Timestamp != "" && err == nil && input.sentAt.valid:
				sentAt := input.sentAt.value
				seconds := receipt.Unix() + timestamp.Unix() - sentAt.Unix()
				nanos := receipt.Nanosecond() + timestamp.Nanosecond() - sentAt.Nanosecond()
				timestamp = time.Unix(seconds, int64(nanos))
				reason = "sent_at"
			case offsetErr == nil && offset >= 0:
				seconds := receipt.Unix() - offset/1000
				nanos := int64(receipt.Nanosecond()) - offset%1000*int64(time.Millisecond)
				timestamp = time.Unix(seconds, nanos)
				reason, err = "offset", nil
			}
		}
		if timestamp.After(receipt.Add(23 * time.Hour)) {
			timestamp, reason = receipt, "future_clamped"
		}
		if timestamp.UTC().Year() < 0 {
			timestamp = time.Unix(0, 0)
		}
	}
	return timestamp.UTC().Format(time.RFC3339Nano), reason, err
}
