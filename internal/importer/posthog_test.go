package importer

import (
	"fmt"
	"strings"
	"testing"
)

func TestImportRowsActualPostHogJSONLines(t *testing.T) {
	t.Parallel()
	for _, properties := range []string{`{"large":9007199254740993,"$current_url":"https://example.test/"}`, `"{\"large\":9007199254740993,\"$current_url\":\"https://example.test/\"}"`} {
		raw := fmt.Sprintf(`{"uuid":"archive-1","event":"$autocapture","distinct_id":"anon","timestamp":"2024-01-02T03:04:05.123456Z","properties":%s,"person_id":"internal-not-app","person_properties":{"stale":true},"elements_chain":"button.example","created_at":"2024-01-03T01:00:00Z"}`, properties)
		row, err := ParsePostHog([]byte(raw), "events")
		if err != nil || row.Event.Timestamp != "2024-01-02T03:04:05.123456Z" || !strings.Contains(string(row.Event.Properties), `9007199254740993`) || !strings.Contains(string(row.Event.Properties), `$elements_chain`) {
			t.Fatal("actual PostHog event normalization failed", err)
		}
	}
	for _, raw := range []string{`{}`, `{"uuid":"x","uuid":"y","event":"v","distinct_id":"u","timestamp":"2024-01-01T00:00:00Z"}`, `{"uuid":"x","event":"v","distinct_id":"$posthog_cookieless","timestamp":"2024-01-01T00:00:00Z"}`, `{"uuid":"x","event":"v","distinct_id":"u","timestamp":"yesterday"}`, `{"uuid":"x","event":"v","distinct_id":"u","timestamp":"2024-01-01T00:00:00Z","properties":[]}`} {
		if _, err := ParsePostHog([]byte(raw), "events"); err == nil {
			t.Errorf("malformed import row accepted: %s", raw)
		}
	}
	row, err := ParsePostHog([]byte(`{"uuid":"old-cookieless","event":"$pageview","distinct_id":"cookieless_archived_daily_hash","timestamp":"2024-01-01 00:00:00.001000","properties":{"cookieless":true,"$session_id":"private","$ip":"192.0.2.1"}}`), "events")
	if err != nil || strings.Contains(string(row.Event.Properties), "private") || strings.Contains(string(row.Event.Properties), "192.0.2.1") {
		t.Fatal("archived cookieless data reconstructed identity or kept known identifiers")
	}
}
