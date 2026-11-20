package lake

import (
	"reflect"
	"testing"
	"time"
)

const legacyDuckLakeProfileActivitySQL = `SELECT CAST(count(*) AS VARCHAR),
 CAST(min(timestamp_ms) AS VARCHAR), CAST(max(timestamp_ms) AS VARCHAR),
 CAST(min(received_at_ms) AS VARCHAR), CAST(max(received_at_ms) AS VARCHAR),
 CAST(count(*) FILTER (WHERE timestamp_ms>=? AND timestamp_ms<?) AS VARCHAR),
 CAST(count(*) FILTER (WHERE timestamp_ms>=? AND timestamp_ms<?) AS VARCHAR),
 CAST(count(*) FILTER (WHERE timestamp_ms>=?) AS VARCHAR)
 FROM events WHERE event=?`

func TestDuckLakeProfileActivityMatchesEventsView(t *testing.T) {
	t.Parallel()
	store := openLakeStorageTest(t, t.TempDir(), 2)
	const project, neighbor = "activity-primary", "activity-neighbor"
	first := lakeStorageProjection(t, project, "first", 1)
	edited := lakeStorageProjection(t, project, "edited", 2)
	edited.Event.Event = "obsolete"
	edited.Timestamp = "2026-11-12T12:00:00Z"
	removed := lakeStorageProjection(t, project, "removed", 3)
	hidden := lakeStorageProjection(t, project, "hidden", 4)
	hidden.IdentitySnapshot = `{"version":"4","identified_id":"identified","traits":{"plan":"paid"},"links":[{"anonymous_id":"anonymous","identified_id":"identified","version":"4"}]}`
	future := lakeStorageProjection(t, project, "future", 5)
	future.Timestamp = "2026-11-13T12:00:00Z"
	foreign := lakeStorageProjection(t, neighbor, "foreign", 5)
	foreign.Event.Event = "neighbor-only"
	if err := store.Apply(t.Context(), []EventProjection{first, edited, removed, hidden, future, foreign}); err != nil {
		t.Fatal(err)
	}
	edited.Revision, edited.Event.Event = 1, first.Event.Event
	removed.Revision, removed.IsDeleted = 1, true
	archived := lakeStorageProjection(t, project, "archive", 6)
	archived.Imported = true
	archived.Timestamp, archived.ReceivedAt = "2001-01-01T00:00:00Z", "2026-11-13T10:30:00Z"
	if err := store.Apply(t.Context(), []EventProjection{edited, removed, archived}); err != nil {
		t.Fatal(err)
	}

	asOf := time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC)
	for _, test := range []struct{ project, name, count string }{
		{project, "page_viewed", "4"},
		{project, "obsolete", "0"},
		{project, "neighbor-only", "0"},
		{neighbor, "page_viewed", "0"},
		{neighbor, "neighbor-only", "1"},
	} {
		opts, err := NormalizeEventProfileInput(EventProfileInput{Event: test.name})
		if err != nil {
			t.Fatal(err)
		}
		got, err := store.Profile(t.Context(), test.project, opts, asOf)
		if err != nil {
			t.Fatal(err)
		}
		want := readLegacyProfileActivity(t, store, test.project, test.name, asOf)
		if !reflect.DeepEqual(got.activity, want) || got.activity.Total != test.count {
			t.Fatalf("activity for %s/%s differs from events view or expected count: got %+v, want %+v (%s total)",
				test.project, test.name, got.activity, want, test.count)
		}
		if test.name == "page_viewed" && test.project == project &&
			(got.activity.Last24Hours != "2" || got.activity.Last7Days != "2" || got.activity.FutureEvents != "1") {
			t.Fatalf("unexpected activity windows: %+v", got.activity)
		}
	}
}

func readLegacyProfileActivity(t *testing.T, store *DuckLakeStore, project, name string, asOf time.Time) EventProfileActivity {
	t.Helper()
	p, release, err := store.LeaseProject(t.Context(), project)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	var activity EventProfileActivity
	err = p.DB.QueryRowContext(t.Context(), legacyDuckLakeProfileActivitySQL,
		asOf.Add(-24*time.Hour).UnixMilli(), asOf.UnixMilli(),
		asOf.Add(-7*24*time.Hour).UnixMilli(), asOf.UnixMilli(), asOf.UnixMilli(), name,
	).Scan(&activity.Total, &activity.FirstOccurredAt, &activity.LastOccurredAt,
		&activity.FirstReceivedAt, &activity.LastReceivedAt, &activity.Last24Hours, &activity.Last7Days, &activity.FutureEvents)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []**string{&activity.FirstOccurredAt, &activity.LastOccurredAt, &activity.FirstReceivedAt, &activity.LastReceivedAt} {
		*field, err = profileTime(*field)
		if err != nil {
			t.Fatal(err)
		}
	}
	return activity
}
