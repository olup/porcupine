package lake

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"
)

const legacyDuckLakeProfileSampleSQL = `WITH selected AS (
 SELECT id AS sort_id, CAST(id AS VARCHAR) AS id,uuid,timestamp,timestamp_ms,received_at,properties,context,
 row_number() OVER (ORDER BY timestamp_ms DESC,id DESC) AS rn
 FROM events WHERE event=? ORDER BY timestamp_ms DESC,id DESC LIMIT ?
), bounded AS (
 SELECT *,octet_length(encode(properties)) AS property_bytes,octet_length(encode(context)) AS context_bytes,
 CASE WHEN octet_length(encode(properties))<=? THEN properties ELSE '' END AS safe_properties,
 CASE WHEN rn<=? AND octet_length(encode(context))<=? THEN context ELSE '' END AS safe_context
 FROM selected
), costs AS (
 SELECT *,octet_length(encode(CAST(to_json(safe_properties) AS VARCHAR)))
 +octet_length(encode(CAST(to_json(safe_context) AS VARCHAR))) AS payload_cost,
 octet_length(encode(CAST(to_json(struct_pack(id:=id,uuid:=uuid,timestamp:=timestamp,
 timestamp_ms:=CAST(timestamp_ms AS VARCHAR),received_at:=received_at,
 property_bytes:=property_bytes,context_bytes:=context_bytes)) AS VARCHAR)))+100 AS metadata_cost
 FROM bounded
), budgeted AS (
 SELECT *,sum(payload_cost) OVER (ORDER BY rn ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS payload_used,
 sum(metadata_cost) OVER (ORDER BY rn ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS metadata_used
 FROM costs
)
SELECT id,uuid,timestamp,timestamp_ms,received_at,
 CASE WHEN payload_used<=? THEN safe_properties ELSE '' END,
 CASE WHEN payload_used<=? THEN safe_context ELSE '' END,
 property_bytes,context_bytes FROM budgeted WHERE metadata_used<=?
 ORDER BY timestamp_ms DESC,sort_id DESC`

func TestDuckLakeProfileSampleMatchesEventsView(t *testing.T) {
	t.Parallel()
	store := openLakeStorageTest(t, t.TempDir(), 1)
	const project = "profile-equivalence"
	first := lakeStorageProjection(t, project, "tie-low", 1)
	old := lakeStorageProjection(t, project, "old-in-range", 2)
	old.Timestamp = "2026-11-12T10:00:00Z"
	other := lakeStorageProjection(t, project, "other-in-range", 3)
	other.Event.Event = "another_event"
	wide := lakeStorageProjection(t, project, "wide-payload", 4)
	wide.Timestamp = "2026-11-13T09:00:00Z"
	wide.Properties = json.RawMessage(`{"wide":"` + strings.Repeat("é", EventProfileFieldBytes) + `"}`)
	last := lakeStorageProjection(t, project, "tie-high", 5)
	last.Context = json.RawMessage(`{"user_agent":"browser"}`)
	archive := lakeStorageProjection(t, project, "archive", 6)
	archive.Imported = true
	archive.Timestamp = "2001-01-01T00:00:00Z"
	link := lakeStorageProjection(t, project, "hidden", 7)
	link.IdentitySnapshot = `{"version":"7","identified_id":"identified","traits":{"plan":"paid"},"links":[{"anonymous_id":"anonymous","identified_id":"identified","version":"7"}]}`
	if err := store.Apply(t.Context(), []EventProjection{first, old, other, wide, last, archive, link}); err != nil {
		t.Fatal(err)
	}

	asOf := time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		event        string
		limit        int
		exampleLimit int
		uuids        []string
	}{
		{"page_viewed", 1, 0, []string{"tie-high"}},
		{"page_viewed", 3, 2, []string{"tie-high", "tie-low", "wide-payload"}},
		{"page_viewed", 1000, 5, []string{"tie-high", "tie-low", "wide-payload", "old-in-range", "archive"}},
		{"absent", 3, 2, nil},
	} {
		opts, err := NormalizeEventProfileInput(EventProfileInput{Event: test.event, SampleLimit: test.limit, ExampleLimit: test.exampleLimit})
		if err != nil {
			t.Fatal(err)
		}
		got, err := store.Profile(t.Context(), project, opts, asOf)
		if err != nil {
			t.Fatal(err)
		}
		want := readLegacyProfileSample(t, store, project, opts)
		if len(got.Rows) != len(want) {
			t.Fatalf("sample size differs from the events view for %v: got %d want %d", test, len(got.Rows), len(want))
		}
		var uuids []string
		for i, row := range got.Rows {
			if row != want[i] {
				t.Fatalf("sample row %d differs from the events view for %v", i, test)
			}
			uuids = append(uuids, row.UUID)
		}
		if !slices.Equal(uuids, test.uuids) {
			t.Fatalf("got sample order %v, want %v", uuids, test.uuids)
		}
	}
}

func readLegacyProfileSample(t *testing.T, store *DuckLakeStore, project string, opts EventProfileOptions) []EventProfileRow {
	t.Helper()
	p, release, err := store.LeaseProject(t.Context(), project)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	rows, err := p.DB.QueryContext(t.Context(), legacyDuckLakeProfileSampleSQL, opts.event, opts.sampleLimit,
		EventProfileFieldBytes, opts.exampleLimit, EventProfileFieldBytes,
		EventProfileSampleBytes, EventProfileSampleBytes, eventProfileMetadataBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var sample []EventProfileRow
	for rows.Next() {
		var row EventProfileRow
		if err := rows.Scan(&row.ID, &row.UUID, &row.Timestamp, &row.TimestampMS, &row.ReceivedAt,
			&row.Properties, &row.Context, &row.PropertyBytes, &row.ContextBytes); err != nil {
			t.Fatal(err)
		}
		sample = append(sample, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return sample
}
