package lake

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/event"
)

func TestDuckLakeCatalogMatchesEventsView(t *testing.T) {
	t.Parallel()
	store := openLakeStorageTest(t, t.TempDir(), 2)
	const project, neighbor = "catalog-primary", "catalog-neighbor"
	var batch []EventProjection
	for id := int64(1); id <= 1002; id++ {
		row := lakeStorageProjection(t, project, fmt.Sprintf("purchase-%d", id), id)
		row.Event.Event = "purchase"
		row.Properties = json.RawMessage(`{"mixed":1}`)
		switch id {
		case 1:
			row.ReceivedAt = "2026-11-13T10:01:00Z"
		case 3:
			row.Properties = json.RawMessage(`{"mixed":{},"outside_sample":true}`)
		case 4:
			row.Properties = json.RawMessage(`{"mixed":1,"at_boundary":true}`)
		case 1002:
			row.Properties = json.RawMessage(`{"$reserved":null,"mixed":true,"zeta":[]}`)
		}
		batch = append(batch, row)
	}
	preview := lakeStorageProjection(t, project, "preview", 1003)
	preview.Event.Event = "preview"
	obsolete := lakeStorageProjection(t, project, "edited", 1004)
	obsolete.Event.Event = "obsolete"
	obsolete.DistinctID = "identified"
	deleted := lakeStorageProjection(t, project, "removed", 1005)
	deleted.Event.Event = "deleted"
	link := lakeStorageProjection(t, project, "link", 1006)
	link.Event.Event = "$identify"
	link.IdentitySnapshot = `{"version":"1006","identified_id":"identified","traits":{"plan":"free"},"links":[{"anonymous_id":"anonymous","identified_id":"identified","version":"1006"}]}`
	foreign := lakeStorageProjection(t, neighbor, "foreign", 1009)
	foreign.Event.Event = "neighbor-only"
	batch = append(batch, preview, obsolete, deleted, link, foreign)
	if err := store.Apply(t.Context(), batch); err != nil {
		t.Fatal(err)
	}

	edit := obsolete
	edit.Revision = 1
	edit.Event.Event = "signup"
	edit.Properties = json.RawMessage(`{"revision":true}`)
	deleted.Revision, deleted.IsDeleted = 1, true
	relink := lakeStorageProjection(t, project, "relink", 1007)
	relink.Event.Event = "purchase"
	relink.Properties = json.RawMessage(`{"private":true}`)
	relink.IdentitySnapshot = `{"version":"1007","identified_id":"identified","traits":{"plan":"paid"},"links":[{"anonymous_id":"anonymous","identified_id":"identified","version":"1007"}]}`
	archived := lakeStorageProjection(t, project, "archive", 1010)
	archived.Event.Event = "purchase"
	archived.Imported = true
	archived.Timestamp = "2001-01-01T00:00:00Z"
	archived.ReceivedAt = "2026-11-13T10:00:30Z"
	archived.Properties = json.RawMessage(`{"mixed":"old","zeta":[]}`)
	if err := store.Apply(t.Context(), []EventProjection{edit, deleted, relink, archived}); err != nil {
		t.Fatal(err)
	}
	identity, err := store.Query(t.Context(), project, `SELECT person_id, json_extract_string(person_properties,'$.plan') AS plan FROM events WHERE uuid='purchase-1002'`, nil, DefaultQueryExecutionLimits())
	if err != nil || identity.Rows != 1 || string(identity.Data[0]["person_id"]) != `"identified"` || string(identity.Data[0]["plan"]) != `"paid"` {
		t.Fatalf("replacing a link and profile must still yield one joined row: %+v, %v", identity, err)
	}

	for _, name := range []string{"", "purchase", "signup", "neighbor-only"} {
		for _, offset := range []int{0, 1, 2, 3, 10000} {
			assertCatalogMatchesEventsView(t, store.Catalog, store.LeaseProject, EventCatalogInput{ID: project, Event: name, Limit: 1, Offset: offset})
		}
	}
	all := assertCatalogMatchesEventsView(t, store.Catalog, store.LeaseProject, EventCatalogInput{ID: project})
	var names []string
	for _, item := range all.Events {
		names = append(names, item.Event)
	}
	if !slices.Equal(names, []string{"purchase", "signup", "preview"}) {
		t.Fatalf("unexpected project catalog names: %v", names)
	}
	assertCatalogIsolated(t, store.Catalog, store.LeaseProject, neighbor, "neighbor-only", "purchase")

	input := EventCatalogInput{ID: project, Event: "purchase"}
	catalog := assertCatalogMatchesEventsView(t, store.Catalog, store.LeaseProject, input)
	wantEvent := EventCatalogEvent{Event: "purchase", Count: "1003", FirstReceivedAt: batch[0].ReceivedAt,
		LastReceivedAt: archived.ReceivedAt, LatestEventUUID: archived.UUID, LatestEventTimestamp: archived.Timestamp}
	if !slices.Equal(catalog.Events, []EventCatalogEvent{wantEvent}) {
		t.Fatalf("unexpected purchase event metadata: %+v", catalog.Events)
	}
	lastLive := batch[1001]
	wantProperties := []EventCatalogProperty{
		{Key: "$reserved", Types: []string{"Null"}, SampleOccurrences: "1", LatestEventUUID: lastLive.UUID, LastReceivedAt: lastLive.ReceivedAt},
		{Key: "at_boundary", Types: []string{"Bool"}, SampleOccurrences: "1", LatestEventUUID: batch[3].UUID, LastReceivedAt: batch[3].ReceivedAt},
		{Key: "mixed", Types: []string{"Bool", "Int64", "String"}, SampleOccurrences: "1000", LatestEventUUID: archived.UUID, LastReceivedAt: archived.ReceivedAt},
		{Key: "zeta", Types: []string{"Array"}, SampleOccurrences: "2", LatestEventUUID: archived.UUID, LastReceivedAt: archived.ReceivedAt},
	}
	if !reflect.DeepEqual(catalog.Properties, wantProperties) {
		t.Fatalf("unexpected purchase sample: %+v", catalog.Properties)
	}

	input.PropertyLimit = 1
	for i, property := range wantProperties {
		page := assertCatalogMatchesEventsView(t, store.Catalog, store.LeaseProject, input)
		if !reflect.DeepEqual(page.Properties, []EventCatalogProperty{property}) || (page.NextPropertyCursor != "") != (i+1 < len(wantProperties)) {
			t.Fatalf("unexpected purchase property page %d: %+v", i, page)
		}
		input.PropertyCursor = page.NextPropertyCursor
	}
}

func TestDuckLakeS3SharedPostgresHostedCatalogProjectIsolation(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	ctx, writer, cold, config := sharedNativeStores(t)
	control, coordinator := nativeFenceControl(t)
	if _, err := control.ExecContext(ctx, `CREATE SEQUENCE shared_event_id_sequence AS BIGINT INCREMENT BY 1024 MINVALUE 1 CACHE 1 NO CYCLE`); err != nil {
		t.Fatal(err)
	}
	reader, err := NewHostedReader(cold)
	if err != nil {
		t.Fatal(err)
	}
	for _, pair := range []struct{ project, own, other string }{
		{"catalog-a", "a-only", "b-only"},
		{"catalog-b", "b-only", "a-only"},
	} {
		key := ProjectWriteKey{SourceID: config.SharedSourceID, ProjectID: pair.project}
		scope, err := writer.InitializeHostedProject(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		owner := acquireFenceSession(t, coordinator, key)
		fence := owner.Fence(scope.StreamID)
		if err := writer.AdvanceWriteFence(ctx, fence); err != nil {
			t.Fatal(err)
		}
		live := newLiveInput(t, pair.project, pair.own, event.NewID(), "person", `{"plan":true}`)
		batch := LiveCommitBatch{ID: "catalog-live", TenantID: "tenant-a", Scope: scope, Inputs: []LiveCommitInput{live}}
		commitLiveFixture(t, writer, owner, batch, &liveProofStager{objects: writer.Remote, attempt: fence.AcquisitionID})
		archive := nativeImportRow(pair.project, event.NewID(), pair.own, "person")
		archive.Event.Properties = json.RawMessage(`{"plan":"archived"}`)
		chunk := nativeImportInput(t, writer, scope, "catalog-import", "events", ImportFileCheckpoint{}, "bytes", archive)
		if proof := commitNativeImport(t, writer, owner, chunk); proof.AcceptedImport != 1 {
			t.Fatalf("historical import was not accepted: %+v", proof)
		}
		catalog := assertCatalogMatchesEventsView(t, reader.Catalog, reader.lease, EventCatalogInput{ID: pair.project, Event: pair.own})
		if len(catalog.Events) != 1 || catalog.Events[0].Count != "2" || catalog.Events[0].LatestEventUUID != archive.Event.UUID ||
			len(catalog.Properties) != 1 || catalog.Properties[0].LatestEventUUID != archive.Event.UUID {
			t.Fatalf("historical import must be latest by durable ID: %+v", catalog)
		}
	}
	assertCatalogIsolated(t, reader.Catalog, reader.lease, "catalog-a", "a-only", "b-only")
	assertCatalogIsolated(t, reader.Catalog, reader.lease, "catalog-b", "b-only", "a-only")
}

func assertCatalogIsolated(t *testing.T, catalog func(context.Context, EventCatalogInput) (EventCatalog, error), lease analyticalProjectLease, project, own, other string) {
	t.Helper()
	all := assertCatalogMatchesEventsView(t, catalog, lease, EventCatalogInput{ID: project})
	if len(all.Events) != 1 || all.Events[0].Event != own {
		t.Fatalf("unexpected catalog for %s: %+v", project, all.Events)
	}
	filtered := assertCatalogMatchesEventsView(t, catalog, lease, EventCatalogInput{ID: project, Event: other})
	if len(filtered.Events) != 0 || len(filtered.Properties) != 0 {
		t.Fatalf("catalog for %s returned another project's event: %+v", project, filtered)
	}
}

func assertCatalogMatchesEventsView(t *testing.T, catalog func(context.Context, EventCatalogInput) (EventCatalog, error), lease analyticalProjectLease, input EventCatalogInput) EventCatalog {
	t.Helper()
	want := legacyDuckLakeCatalog(t, lease, input)
	got, err := catalog(t.Context(), input)
	if err != nil {
		t.Fatalf("catalog for %+v: %v", input, err)
	}
	if !reflect.DeepEqual(got, want) {
		actual, _ := json.Marshal(got)
		expected, _ := json.Marshal(want)
		t.Fatalf("catalog for %+v differs from prior SQL\n got %s\nwant %s", input, actual, expected)
	}
	return got
}

func legacyDuckLakeCatalog(t *testing.T, lease analyticalProjectLease, input EventCatalogInput) EventCatalog {
	t.Helper()
	input, err := NormalizeCatalogInput(input)
	if err != nil {
		t.Fatal(err)
	}
	p, release, err := lease(t.Context(), input.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	result := EventCatalog{Events: []EventCatalogEvent{}, Properties: []EventCatalogProperty{}, Sampling: EventCatalogSampling{
		PropertiesRequested: input.Event != "", SampleEventLimit: 1000, KeysPerEventLimit: 200, MaxPropertyNameBytes: 200, PropertyLimit: input.PropertyLimit,
	}}
	query := `SELECT event, CAST(count(*) AS VARCHAR), arg_min(received_at,id), arg_max(received_at,id), arg_max(uuid,id), arg_max(timestamp,id) FROM events`
	var args []any
	if input.Event != "" {
		query += " WHERE event=?"
		args = append(args, input.Event)
	}
	query += " GROUP BY event ORDER BY max(id) DESC LIMIT ? OFFSET ?"
	rows, err := p.DB.QueryContext(t.Context(), query, append(args, input.Limit+1, input.Offset)...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var item EventCatalogEvent
		if err := rows.Scan(&item.Event, &item.Count, &item.FirstReceivedAt, &item.LastReceivedAt, &item.LatestEventUUID, &item.LatestEventTimestamp); err != nil {
			t.Fatal(err)
		}
		result.Events = append(result.Events, item)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(result.Events) > input.Limit {
		result.Events = result.Events[:input.Limit]
		next := input.Offset + input.Limit
		if next <= 10000 {
			result.NextOffset = &next
		} else {
			result.Sampling.PaginationTruncated = true
		}
	}
	if input.Event == "" {
		return result
	}
	rows, err = p.DB.QueryContext(t.Context(), `SELECT uuid,received_at,properties FROM events WHERE event=? ORDER BY id DESC LIMIT 1000`, input.Event)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	type sample struct {
		count int
		types map[string]struct{}
		uuid  string
		at    string
	}
	properties := map[string]*sample{}
	for rows.Next() {
		var uuid, at, raw string
		if err := rows.Scan(&uuid, &at, &raw); err != nil {
			t.Fatal(err)
		}
		var object map[string]json.RawMessage
		if err := json.Unmarshal([]byte(raw), &object); err != nil {
			t.Fatal(err)
		}
		var keys []string
		for key := range object {
			if catalogPropertyEligible(input, key) {
				keys = append(keys, key)
			}
		}
		slices.Sort(keys)
		for _, key := range keys[:min(len(keys), 200)] {
			if properties[key] == nil {
				properties[key] = &sample{types: map[string]struct{}{}, uuid: uuid, at: at}
			}
			properties[key].count++
			properties[key].types[duckLakeJSONType(object[key])] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	for key, sample := range properties {
		var types []string
		for kind := range sample.types {
			types = append(types, kind)
		}
		slices.Sort(types)
		result.Properties = append(result.Properties, EventCatalogProperty{Key: key, Types: types, SampleOccurrences: strconv.Itoa(sample.count), LatestEventUUID: sample.uuid, LastReceivedAt: sample.at})
	}
	slices.SortFunc(result.Properties, func(a, b EventCatalogProperty) int { return cmp.Compare(a.Key, b.Key) })
	finishCatalogProperties(&result, input)
	return result
}
