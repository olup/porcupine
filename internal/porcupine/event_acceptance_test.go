package porcupine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/netip"
	"strings"
	"testing"
	"time"

	ingestfmt "github.com/olup/porcupine/internal/ingest"
)

func TestLiveAcceptanceParticipantFailureRollsBackAllState(t *testing.T) {
	t.Parallel()
	for _, rotate := range []bool{false, true} {
		name := "first daily key"
		if rotate {
			name = "daily key rotation"
		}
		t.Run(name, func(t *testing.T) {
			s := service(t, 20)
			projectID, _ := project(t, s)
			if _, err := s.db.Exec(`CREATE TABLE acceptance_test_destination(event_id INTEGER); INSERT INTO identity_profiles(project_id,identified_id,is_identified,traits,version) VALUES(?,'person',1,'{"plan":"old"}',1)`, projectID); err != nil {
				t.Fatal(err)
			}
			if rotate {
				if _, err := s.db.Exec(`INSERT INTO cookieless_keys(day,secret) VALUES('2026-11-01',zeroblob(32))`); err != nil {
					t.Fatal(err)
				}
			}
			now := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
			rules := &eventRules{clock: func() time.Time { return now }, queueMax: 20, queueMaxBytes: DefaultQueueMaxBytes}
			prepared, err := prepareLiveBatch(projectID, "", eventBatch{
				{UUID: "identify", Event: "$identify", DistinctID: "person", Properties: map[string]any{"$anon_distinct_id": "browser", "$set": map[string]any{"plan": "new"}}},
				{UUID: "page", Event: "$pageview", DistinctID: ingestfmt.CookielessSentinel, Properties: map[string]any{"$current_url": "https://example.test"}},
			}, now)
			if err != nil {
				t.Fatal(err)
			}
			participantCalls := 0
			writer := eventAcceptance{db: s.db, rules: rules, enqueueWebhooks: func(ctx context.Context, tx *sql.Tx, events []Event) error {
				participantCalls++
				if len(events) != 1 || events[0].Event != "$pageview" || !strings.HasPrefix(events[0].DistinctID, "cookieless_") {
					t.Fatal("participant did not receive the accepted ordinary event")
				}
				var profiles, links, keys int
				if err := tx.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM identity_profiles WHERE traits='{"plan":"new"}'),(SELECT count(*) FROM identity_links),(SELECT count(*) FROM cookieless_keys WHERE day='2026-11-02')`).Scan(&profiles, &links, &keys); err != nil || profiles != 1 || links != 1 || keys != 1 {
					t.Fatalf("participant was not enlisted with identity and daily key state: %d %d %d %v", profiles, links, keys, err)
				}
				if _, err := tx.ExecContext(ctx, `INSERT INTO acceptance_test_destination(event_id) VALUES(?)`, events[0].ID); err != nil {
					t.Fatal(err)
				}
				return errors.New("destination storage unavailable")
			}}
			result, err := writer.Accept(t.Context(), prepared, ingestionPeer{address: netip.MustParseAddr("192.0.2.1"), userAgent: "browser"}, true)
			var failure *acceptanceFailure
			if !errors.As(err, &failure) || failure.message != "webhook queue unavailable" || !failure.retry || participantCalls != 1 || result.accepted != 0 || result.receipts != nil {
				t.Fatalf("failed commit returned an acknowledgment: %+v %v", result, err)
			}
			for _, table := range []string{"events", "identity_links", "event_receipts", "acceptance_test_destination", "tenant_usage"} {
				var count int
				if err := s.db.QueryRow(`SELECT count(*) FROM ` + table).Scan(&count); err != nil || count != 0 {
					t.Fatalf("failed participant committed %s: %d %v", table, count, err)
				}
			}
			var traits string
			var version int
			if err := s.db.QueryRow(`SELECT traits,version FROM identity_profiles WHERE project_id=? AND identified_id='person'`, projectID).Scan(&traits, &version); err != nil || traits != `{"plan":"old"}` || version != 1 {
				t.Fatal("failed participant changed the prior identity profile", traits, version, err)
			}
			var keys, today int
			if err := s.db.QueryRow(`SELECT count(*),count(*) FILTER(WHERE day='2026-11-02') FROM cookieless_keys`).Scan(&keys, &today); err != nil || today != 0 || rotate && keys != 1 || !rotate && keys != 0 {
				t.Fatal("failed participant committed daily secret creation or rotation", keys, today, err)
			}
			if count, bytes := queueUsage(t, s); count != 0 || bytes != 0 {
				t.Fatal("failed participant consumed queue capacity")
			}
		})
	}
}

func TestLiveAcceptanceHTTPDoesNotAcknowledgeFailedParticipant(t *testing.T) {
	t.Parallel()
	s := service(t, 10)
	projectID, ingest := project(t, s)
	s.acceptance.enqueueWebhooks = func(context.Context, *sql.Tx, []Event) error { return errors.New("destination failure") }
	response := request(t, s, "POST", "/capture/", "", captureBody(ingest, "not-acknowledged"), 503)
	if response["accepted"] != nil || response["receipts"] != nil || eventCount(t, s) != 0 {
		t.Fatal("transport acknowledged an uncommitted event", projectID, response)
	}
}

func TestLivePreparationPreservesSourceBoundPostHogResponse(t *testing.T) {
	t.Parallel()
	s := service(t, 10)
	_, ingest := project(t, s)
	bindIngestionSource(t, s, ingest, "web")
	response := request(t, s, "POST", "/capture/", "", captureBody(ingest, "message"), 200)
	if response["accepted"] != float64(1) || response["receipts"] != nil {
		t.Fatal("source enrichment changed the SDK response flavor", response)
	}
	var metadata string
	if err := s.db.QueryRow(`SELECT ingestion_metadata FROM events`).Scan(&metadata); err != nil || !json.Valid([]byte(metadata)) || !strings.Contains(metadata, `"source_id":"web"`) {
		t.Fatal("authenticated source binding was not persisted", metadata, err)
	}
}
