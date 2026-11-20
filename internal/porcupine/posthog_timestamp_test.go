package porcupine

import (
	"encoding/json"
	"fmt"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	ingestfmt "github.com/olup/porcupine/internal/ingest"
)

func TestPostHogClockCorrection(t *testing.T) {
	t.Parallel()
	receipt := time.Date(2026, 11, 20, 12, 0, 0, 500_000_000, time.UTC)
	received := receipt.Format(time.RFC3339Nano)
	s, err := Open(Config{
		DBPath: filepath.Join(t.TempDir(), "journal.db"), BootstrapToken: adminToken,
		Clock: func() time.Time { return receipt }, MetricsEnabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	projectID, token := project(t, s)

	const client = "2026-11-20T13:50:00+00:00"
	const sent = "2026-11-20T14:00:00Z"
	const eventSent = "2026-11-20T14:05:00Z"
	queryMillis := strconv.FormatInt(receipt.Add(2*time.Hour-500*time.Millisecond).UnixMilli(), 10)
	capture := func(t *testing.T, path, rootSent string, event map[string]any) map[string]any {
		t.Helper()
		event["api_key"] = token
		event["event"] = "$pageview"
		if event["distinct_id"] == nil {
			event["distinct_id"] = "visitor"
		}
		var body any = event
		switch {
		case strings.HasPrefix(path, "/batch/"):
			body = map[string]any{"sent_at": rootSent, "batch": []any{event}}
		case strings.HasPrefix(path, "/e/"):
			body = []any{event}
		}
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		return request(t, s, "POST", path, "", string(raw), 200)
	}

	for _, tc := range []struct {
		name, path, rootSent string
		input                map[string]any
		timestamp, sentAt    string
	}{
		{"batch-root", "/batch/?sent_at=2026-11-20T15:00:00Z", sent, map[string]any{"timestamp": client}, "2026-11-20T11:50:00.5Z", sent},
		{"event-beats-root", "/batch/", sent, map[string]any{"timestamp": client, "sent_at": eventSent, "offset": 1000}, "2026-11-20T11:45:00.5Z", eventSent},
		{"array-event", "/e/", "", map[string]any{"timestamp": client, "sent_at": sent}, "2026-11-20T11:50:00.5Z", sent},
		{"query-iso", "/capture/?sent_at=" + url.QueryEscape(eventSent) + "&_=" + queryMillis, "", map[string]any{"timestamp": client}, "2026-11-20T11:45:00.5Z", eventSent},
		{"query-ms", "/capture/?sent_at=" + queryMillis, "", map[string]any{"timestamp": client}, "2026-11-20T11:50:00.5Z", queryMillis},
		{"query-underscore", "/capture/?sent_at=unreadable&_=" + queryMillis, "", map[string]any{"timestamp": client}, "2026-11-20T11:50:00.5Z", queryMillis},
		{"offset", "/capture/", "", map[string]any{"timestamp": client, "offset": 1500}, "2026-11-20T11:59:59Z", ""},
		{"offset-without-timestamp", "/capture/", "", map[string]any{"offset": 0}, received, ""},
		{"offset-with-invalid-timestamp", "/capture/", "", map[string]any{"timestamp": "unreadable", "offset": 2500}, "2026-11-20T11:59:58Z", ""},
		{"bad-offset", "/capture/", "", map[string]any{"timestamp": client, "offset": 1.5}, "2026-11-20T13:50:00Z", ""},
		{"negative-offset", "/capture/", "", map[string]any{"timestamp": client, "offset": -1}, "2026-11-20T13:50:00Z", ""},
		{"ignore", "/capture/", "", map[string]any{"timestamp": client, "sent_at": sent, "offset": 1000, "properties": map[string]any{"$ignore_sent_at": true}}, "2026-11-20T13:50:00Z", sent},
		{"bad-sent-at", "/capture/", "", map[string]any{"timestamp": client, "sent_at": "unreadable"}, "2026-11-20T13:50:00Z", "unreadable"},
		{"bad-sent-at-type", "/capture/", "", map[string]any{"timestamp": client, "sent_at": []int{1, 2}}, "2026-11-20T13:50:00Z", ""},
		{"bad-event-sent-at", "/batch/", sent, map[string]any{"timestamp": client, "sent_at": "unreadable"}, "2026-11-20T11:50:00.5Z", sent},
		{"absent", "/capture/", "", map[string]any{}, received, ""},
		{"future-clamped", "/capture/", "", map[string]any{"timestamp": "2026-11-21T13:00:00Z", "sent_at": received}, received, received},
		{"future-clamped-even-when-ignored", "/capture/", "", map[string]any{"timestamp": "2026-11-21T13:00:00Z", "properties": map[string]any{"$ignore_sent_at": true}}, received, ""},
		{"future-at-cutoff", "/capture/", "", map[string]any{"timestamp": "2026-11-21T11:00:00.5Z"}, "2026-11-21T11:00:00.5Z", ""},
		{"cookieless", "/capture/", "", map[string]any{"timestamp": client, "sent_at": sent, "distinct_id": ingestfmt.CookielessSentinel, "properties": map[string]any{"$session_id": "secret", "$browser": "Firefox"}}, "2026-11-20T11:50:00.5Z", sent},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.input["uuid"] = tc.name
			capture(t, tc.path, tc.rootSent, tc.input)
			var stamp, actualReceipt string
			var storedClient, storedSent, browser, session *string
			err := s.db.QueryRowContext(t.Context(), `
SELECT timestamp, received_at,
       json_extract(NULLIF(ingestion_metadata,''),'$.context.client_timestamp'),
       json_extract(NULLIF(ingestion_metadata,''),'$.context.sent_at'),
       json_extract(NULLIF(ingestion_metadata,''),'$.context.browser.name'),
       json_extract(NULLIF(ingestion_metadata,''),'$.context.session_id')
FROM events WHERE project_id=? AND uuid=?`, projectID, tc.name).Scan(&stamp, &actualReceipt, &storedClient, &storedSent, &browser, &session)
			if err != nil {
				t.Fatal(err)
			}
			if stamp != tc.timestamp || actualReceipt != received {
				t.Fatalf("timestamp = %q, received_at = %q; want %q, %q", stamp, actualReceipt, tc.timestamp, received)
			}
			for column, want := range map[string]any{"client_timestamp": tc.input["timestamp"], "sent_at": tc.sentAt} {
				actual := storedClient
				if column == "sent_at" {
					actual = storedSent
				}
				if want == nil || want == "" {
					if actual != nil {
						t.Errorf("%s = %q; want NULL", column, *actual)
					}
				} else if actual == nil || *actual != want {
					t.Errorf("%s = %v; want %q", column, actual, want)
				}
			}
			if tc.name == "cookieless" && (browser == nil || *browser != "Firefox" || session != nil) {
				t.Fatal("cookieless capture changed public browser context or stored a session ID")
			}
		})
	}

	multi := fmt.Sprintf(`{"api_key":%q,"sent_at":%q,"batch":[
		{"uuid":"first","event":"view","distinct_id":"visitor","timestamp":%q},
		{"uuid":"second","event":"purchase","distinct_id":"visitor","timestamp":"2026-11-20T13:55:00Z"}
	]}`, token, sent, client)
	request(t, s, "POST", "/batch/", "", multi, 200)
	retry := capture(t, "/capture/", "", map[string]any{"uuid": "first", "timestamp": client, "sent_at": eventSent})
	if retry["duplicates"] != float64(1) {
		t.Fatalf("retry was not deduplicated: %v", retry)
	}
	for id, want := range map[string]string{"first": "2026-11-20T11:50:00.5Z", "second": "2026-11-20T11:55:00.5Z"} {
		var stamp, rawSent string
		if err := s.db.QueryRowContext(t.Context(), `SELECT timestamp,json_extract(ingestion_metadata,'$.context.sent_at') FROM events WHERE uuid=?`, id).Scan(&stamp, &rawSent); err != nil || stamp != want || rawSent != sent {
			t.Fatalf("%s: timestamp=%q sent_at=%q err=%v, want %q and %q", id, stamp, rawSent, err, want, sent)
		}
	}
	metrics := scrape(t, s)
	requireMetric(t, metrics, `porcupine_ingestion_timestamp_adjusted_total{profile="standalone",reason="sent_at"} 11`)
	requireMetric(t, metrics, `porcupine_ingestion_timestamp_adjusted_total{profile="standalone",reason="offset"} 3`)
	requireMetric(t, metrics, `porcupine_ingestion_timestamp_adjusted_total{profile="standalone",reason="future_clamped"} 2`)
}
