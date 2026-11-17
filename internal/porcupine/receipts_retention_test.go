package porcupine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	eventapi "github.com/olup/porcupine/internal/event"
	"github.com/olup/porcupine/internal/importer"
	lakeapi "github.com/olup/porcupine/internal/lake"
)

func TestBoundedReceiptsFallBackToDuckLake(t *testing.T) {
	t.Parallel()
	s := duckLakeJourneyService(t)
	var now atomic.Int64
	now.Store(time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC).UnixMilli())
	s.eventRules.clock = func() time.Time { return time.UnixMilli(now.Load()).UTC() }
	s.receiptRetention = 48 * time.Hour
	projectID, writer := project(t, s)
	ctx := context.Background()
	receipts := func() (n int) {
		if err := s.db.QueryRow(`SELECT count(*) FROM event_receipts WHERE project_id=?`, projectID).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	body := `{"type":"track","message_id":"old-retry","name":"purchase","timestamp":"2026-11-01T10:00:00Z"}`
	first := nativeRequest(t, s, writer, body, 200)
	id := receiptFrom(t, first, 0)["event_id"]
	duckLakeJourneyDrain(t, s)
	if receipts() != 1 {
		t.Fatal("projected event has no receipt")
	}
	if err := s.pruneReceipts(ctx); err != nil || receipts() != 1 {
		t.Fatal("receipt pruned inside the retention window", err)
	}
	now.Add((72 * time.Hour).Milliseconds())
	if err := s.pruneReceipts(ctx); err != nil || receipts() != 0 {
		t.Fatal("expired receipt kept", err)
	}
	replay := nativeRequest(t, s, writer, body, 200)
	if replay["duplicates"] != float64(1) || receiptFrom(t, replay, 0)["event_id"] != id {
		t.Fatalf("pruned receipt lost deduplication: %v", replay)
	}
	fresh := nativeRequest(t, s, writer, `{"type":"track","message_id":"old-new","name":"purchase","timestamp":"2026-11-01T10:00:00Z"}`, 200)
	if fresh["duplicates"] != float64(0) {
		t.Fatalf("new historical event treated as duplicate: %v", fresh)
	}
	duckLakeJourneyDrain(t, s)
	var total int
	if err := s.db.QueryRow(`SELECT count(*) FROM event_receipts WHERE project_id=?`, projectID).Scan(&total); err != nil || total != 1 {
		t.Fatal("new event was not projected exactly once", total, err)
	}
}

func TestReceiptRetentionZeroKeepsReceiptsAndSkipsLookups(t *testing.T) {
	t.Parallel()
	s := duckLakeJourneyService(t)
	s.receiptRetention = 0
	_, writer := project(t, s)
	nativeRequest(t, s, writer, `{"type":"track","message_id":"kept","name":"purchase","timestamp":"2020-01-01T00:00:00Z"}`, 200)
	duckLakeJourneyDrain(t, s)
	if err := s.pruneReceipts(context.Background()); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := s.db.QueryRow(`SELECT count(*) FROM event_receipts`).Scan(&n); err != nil || n != 1 {
		t.Fatal("retention 0 pruned a receipt", n, err)
	}
	known, err := s.eventRules.coldKnown(context.Background(), "any", []eventapi.Event{{UUID: "u", Timestamp: "2020-01-01T00:00:00Z"}})
	if err != nil || known != nil {
		t.Fatal("retention 0 looked up DuckLake", known, err)
	}
}

func TestColdKnownDoesNotTrustProducerTimestamps(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 30, 0, 0, 0, 0, time.UTC)
	var asked []string
	rules := &eventRules{clock: func() time.Time { return now }, receiptRetention: 30 * 24 * time.Hour,
		knownEvents: func(_ context.Context, _ string, uuids []string) (map[string]int64, error) {
			asked = append(asked, uuids...)
			return map[string]int64{}, nil
		}}
	events := []eventapi.Event{
		{UUID: "recent", Timestamp: now.Add(-time.Hour).Format(time.RFC3339Nano)},
		{UUID: "inside-window", Timestamp: now.Add(-20 * 24 * time.Hour).Format(time.RFC3339Nano)},
		{UUID: "within-margin", Timestamp: now.Add(-25 * 24 * time.Hour).Format(time.RFC3339Nano)},
		{UUID: "old", Timestamp: now.Add(-40 * 24 * time.Hour).Format(time.RFC3339Nano)},
		{UUID: "unparsed", Timestamp: "not a time"},
	}
	if _, err := rules.coldKnown(context.Background(), "project", events); err != nil {
		t.Fatal(err)
	}
	if len(asked) != len(events) {
		t.Fatalf("looked up %v", asked)
	}
}

func TestPrunedReceiptsKeepIDsAndIdentityAcrossRestart(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	var clock atomic.Int64
	clock.Store(time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC).UnixMilli())
	directory := t.TempDir()
	config := Config{DBPath: filepath.Join(directory, "journal.sqlite"), BootstrapToken: adminToken,
		Clock: func() time.Time { return time.UnixMilli(clock.Load()).UTC() }, ReceiptRetention: 48 * time.Hour,
		DuckLake: &lakeapi.DuckLakeConfig{Directory: filepath.Join(directory, "lake"), MemoryLimitMB: 128, Threads: 1}}
	s, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { s.Close() }()
	stopCoreDelivery(s)
	projectID, writer := project(t, s)
	ids, uuids := map[string]any{}, map[string]string{}
	for _, name := range []string{"original", "edited", "deleted"} {
		body := fmt.Sprintf(`{"type":"track","message_id":%q,"actor_id":"browser","name":%q,"timestamp":"2026-11-01T10:00:00.123456789Z","properties":{"value":1}}`, name, name)
		r := receiptFrom(t, nativeRequest(t, s, writer, body, 200), 0)
		ids[name], uuids[name] = r["event_id"], r["uuid"].(string)
	}
	identify := `{"type":"identify","message_id":"identity","anonymous_id":"browser","user_id":"person","set":{"plan":"team"}}`
	r := receiptFrom(t, nativeRequest(t, s, writer, identify, 200), 0)
	ids["identity"] = r["event_id"]
	duckLakeJourneyDrain(t, s)
	_, manager := key(t, s, []string{"events:read", "events:edit", "events:delete"}, []string{projectID})
	path := "/api/v1/projects/" + projectID + "/events/"
	request(t, s, "PATCH", path+uuids["edited"], manager, `{"expected_revision":"0","idempotency_key":"edit","snapshot":{"event":"corrected","distinct_id":"browser","timestamp":"2026-11-01T11:00:00Z","properties":{"value":2}}}`, 202)
	request(t, s, "DELETE", path+uuids["deleted"], manager, `{"expected_revision":"0","idempotency_key":"delete"}`, 202)
	duckLakeJourneyDrain(t, s)
	clock.Add((72 * time.Hour).Milliseconds())
	if err := s.pruneReceipts(ctx); err != nil {
		t.Fatal(err)
	}
	var receipts int
	if err := s.db.QueryRow("SELECT count(*) FROM event_receipts").Scan(&receipts); err != nil || receipts != 0 {
		t.Fatal("receipts not pruned", receipts, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// Disabling future pruning cannot restore deleted local receipt history.
	config.ReceiptRetention = 0
	s, err = Open(config)
	if err != nil {
		t.Fatal(err)
	}
	stopCoreDelivery(s)
	for _, name := range []string{"original", "edited", "deleted"} {
		// Producers often omit timestamps on retries. Even the current date and a
		// changed payload must not turn an existing message into a new event.
		body := fmt.Sprintf(`{"type":"track","message_id":%q,"actor_id":"browser","name":"unwanted","properties":{"value":999}}`, name)
		replay := nativeRequest(t, s, writer, body, 200)
		if replay["duplicates"] != float64(1) || receiptFrom(t, replay, 0)["event_id"] != ids[name] {
			t.Fatalf("retry changed %s: %v", name, replay)
		}
	}
	replay := nativeRequest(t, s, writer, `{"type":"identify","message_id":"identity","anonymous_id":"browser","user_id":"attacker","set":{"plan":"changed"}}`, 200)
	if replay["duplicates"] != float64(1) || receiptFrom(t, replay, 0)["event_id"] != ids["identity"] {
		t.Fatalf("identity replay was accepted: %v", replay)
	}
	duckLakeJourneyDrain(t, s)
	original, err := s.lake.Load(ctx, projectID, uuids["original"])
	if err != nil || original.Event.Event != "original" || original.Timestamp != "2026-11-01T10:00:00.123456789Z" {
		t.Fatal("original changed", original, err)
	}
	edited, err := s.lake.Load(ctx, projectID, uuids["edited"])
	if err != nil || edited.Event.Event != "corrected" || edited.Revision != 1 {
		t.Fatal("correction lost", edited, err)
	}
	deleted, err := s.lake.Load(ctx, projectID, uuids["deleted"])
	if err != nil || !deleted.IsDeleted {
		t.Fatal("deleted event resurrected", deleted, err)
	}
	rows := duckLakeJourneyQuery(t, s, projectID, manager, "SELECT DISTINCT person_id,person_properties FROM events", nil)
	if len(rows) != 1 || rows[0].(map[string]any)["person_id"] != "person" {
		t.Fatal("identity changed on replay", rows)
	}
}

func TestReceiptPruningRequiresConfirmedAnalyticalHeads(t *testing.T) {
	t.Parallel()
	s := duckLakeJourneyService(t)
	projectID, writer := project(t, s)
	r := receiptFrom(t, nativeRequest(t, s, writer, `{"type":"track","message_id":"pending","name":"pending"}`, 200), 0)
	// A receipt alone is insufficient evidence: a queued original may still
	// have an outstanding delete or an interrupted delivery.
	tx, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	var id int64
	if err := tx.QueryRow("SELECT id FROM events WHERE uuid=?", r["uuid"]).Scan(&id); err != nil {
		t.Fatal(err)
	}
	if err := s.recordReceipt(context.Background(), tx, projectID, r["uuid"].(string), id); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	s.receiptRetention = time.Hour
	if _, err := s.db.Exec("UPDATE event_receipts SET recorded_at_ms=0 WHERE project_id=?", projectID); err != nil {
		t.Fatal(err)
	}
	if err := s.pruneReceipts(context.Background()); err != nil {
		t.Fatal(err)
	}
	if receiptCount(t, s) != 1 {
		t.Fatal("pending event receipt pruned")
	}
	duckLakeJourneyDrain(t, s)
	if err := s.lake.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.pruneReceipts(context.Background()); err == nil {
		t.Fatal("pruning accepted unavailable analytical history")
	}
	if receiptCount(t, s) != 1 {
		t.Fatal("unconfirmed receipt pruned")
	}
}

func TestPrunedReceiptLookupFailureRollsBackWholeBatch(t *testing.T) {
	t.Parallel()
	s := duckLakeJourneyService(t)
	projectID, writer := project(t, s)
	if _, err := s.db.Exec("INSERT INTO metadata(key,value) VALUES('receipts_pruned:' || ?,'1')", projectID); err != nil {
		t.Fatal(err)
	}
	s.eventRules.knownEvents = func(context.Context, string, []string) (map[string]int64, error) {
		return nil, errors.New("unavailable")
	}
	nativeRequest(t, s, writer, `[{"type":"track","message_id":"a","name":"a"},{"type":"track","message_id":"b","name":"b"}]`, 503)
	var n int
	if err := s.db.QueryRow("SELECT count(*) FROM events").Scan(&n); err != nil || n != 0 {
		t.Fatal("unverified batch accepted", n, err)
	}
}

func TestHistoricalImportsDeduplicatePrunedReceiptsAndRetryUnavailableLake(t *testing.T) {
	t.Parallel()
	for _, format := range []string{"native", "posthog", "segment"} {
		t.Run(format, func(t *testing.T) {
			t.Parallel()
			s := duckLakeJourneyService(t)
			var clock atomic.Int64
			clock.Store(time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC).UnixMilli())
			s.eventRules.clock = func() time.Time { return time.UnixMilli(clock.Load()).UTC() }
			s.receiptRetention = 48 * time.Hour
			projectID, _ := project(t, s)
			_, writer := key(t, s, []string{"imports:write", "events:write"}, []string{projectID})
			bindIngestionSource(t, s, writer, "checkout")
			data := historicalJourney(t, format)
			job := createFormatImport(t, s, projectID, writer, format, "initial", data)
			completed := finishFormatImport(t, s, projectID, writer, job, data)
			if completed.Counts.Accepted != 3 {
				t.Fatal(completed.Counts)
			}
			duckLakeJourneyDrain(t, s)
			clock.Add((72 * time.Hour).Milliseconds())
			if err := s.pruneReceipts(context.Background()); err != nil {
				t.Fatal(err)
			}
			if receiptCount(t, s) != 0 {
				t.Fatal("historical receipts not pruned")
			}
			// Even a changed occurrence time cannot reuse an accepted producer ID.
			retryData := bytes.ReplaceAll(data, []byte("2026-01-10"), []byte("2026-11-04"))
			retryJob := createFormatImport(t, s, projectID, writer, format, "new-job", retryData)
			retry := finishFormatImport(t, s, projectID, writer, retryJob, retryData)
			if retry.Counts.Duplicates != 3 || retry.Counts.Accepted != 0 {
				t.Fatal("pruned import was reaccepted", retry.Counts)
			}
			blocked := createFormatImport(t, s, projectID, writer, format, "blocked-job", retryData)
			store := s.lake.(*lakeapi.DuckLakeStore)
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			importTestPut(t, s, projectID, blocked, blocked.Files[0], writer, 0, retryData, 503)
			var offset int64
			if err := s.db.QueryRow("SELECT offset_bytes FROM import_upload_files WHERE id=?", blocked.Files[0].ID).Scan(&offset); err != nil || offset != 0 {
				t.Fatal("failed lookup advanced import", offset, err)
			}
			reopened, err := lakeapi.NewDuckLake(store.Config)
			if err != nil {
				t.Fatal(err)
			}
			s.lake = reopened
			resumed := finishFormatImport(t, s, projectID, writer, blocked, retryData)
			if resumed.Counts.Duplicates != 3 || resumed.Counts.Accepted != 0 {
				t.Fatal("retry after lake recovery changed counts", resumed.Counts)
			}
		})
	}
}

func TestHistoricalPersonsDeduplicateAfterReceiptPruning(t *testing.T) {
	t.Parallel()
	s := duckLakeJourneyService(t)
	var clock atomic.Int64
	clock.Store(time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC).UnixMilli())
	s.eventRules.clock = func() time.Time { return time.UnixMilli(clock.Load()).UTC() }
	s.receiptRetention = 48 * time.Hour
	projectID, _ := project(t, s)
	data := append([]byte(`{"person_id":"posthog-person","distinct_id":"browser","person_version":1,"person_distinct_id_version":1,"properties":{"plan":"team"}}`), '\n')
	create := func(key string) importer.ImportUploadJob {
		return importTestCreate(t, s, projectID, adminToken, key, []importer.ImportUploadFileSpec{{Path: "persons.jsonl", Kind: "persons", SizeBytes: int64(len(data)), SHA256: importTestSHA(data)}})
	}
	initial := finishFormatImport(t, s, projectID, adminToken, create("first-persons"), data)
	if initial.Counts.Accepted != 1 {
		t.Fatal(initial.Counts)
	}
	duckLakeJourneyDrain(t, s)
	var traits string
	var version, usage int64
	if err := s.db.QueryRow("SELECT traits,version FROM identity_profiles WHERE project_id=? AND identified_id='browser'", projectID).Scan(&traits, &version); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow("SELECT coalesce(sum(accepted_events),0) FROM tenant_usage").Scan(&usage); err != nil {
		t.Fatal(err)
	}
	clock.Add((72 * time.Hour).Milliseconds())
	if err := s.pruneReceipts(context.Background()); err != nil {
		t.Fatal(err)
	}
	if receiptCount(t, s) != 0 {
		t.Fatal("person snapshot receipt was not pruned")
	}
	retry := finishFormatImport(t, s, projectID, adminToken, create("retried-persons"), data)
	if retry.Counts.Duplicates != 1 || retry.Counts.Accepted != 0 || retry.Counts.Ignored != 0 || retry.Counts.Unresolved != 0 {
		t.Fatal("person snapshot retry changed classification", retry.Counts)
	}
	var gotTraits string
	var gotVersion, gotUsage int64
	if err := s.db.QueryRow("SELECT traits,version FROM identity_profiles WHERE project_id=? AND identified_id='browser'", projectID).Scan(&gotTraits, &gotVersion); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow("SELECT coalesce(sum(accepted_events),0) FROM tenant_usage").Scan(&gotUsage); err != nil {
		t.Fatal(err)
	}
	if gotTraits != traits || gotVersion != version || gotUsage != usage {
		t.Fatal("person retry changed identity or usage", gotTraits, gotVersion, gotUsage)
	}
}
