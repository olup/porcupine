package lake

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/apierr"
	"github.com/olup/porcupine/internal/nativequery"
)

func TestDuckLakeEventSourcesMatch(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	store := openLakeStorageTest(t, t.TempDir(), 2)
	const project, neighbor = "query-primary", "query-neighbor"
	populateCatalogEquivalenceFixture(t, store, project, neighbor)
	ctx := t.Context()
	p, release, err := store.LeaseProject(ctx, project)
	if err != nil {
		t.Fatal(err)
	}
	defer release()

	start := time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC)
	window := QueryTimeRange{From: start, To: start.Add(time.Second)}
	for _, tc := range []struct {
		name, sql   string
		needsPerson bool
	}{
		{"count", `SELECT count(*) AS n FROM events`, false},
		{"count window", `SELECT count(*) OVER () AS n FROM events LIMIT 1`, false},
		{"person ID", `SELECT person_id FROM events ORDER BY id DESC LIMIT 4`, true},
		{"quoted person ID", `SELECT "PERSON_ID" FROM events ORDER BY id DESC LIMIT 4`, true},
		{"star", `SELECT * FROM events ORDER BY id DESC LIMIT 4`, true},
		{"qualified star", `SELECT e.* FROM events e ORDER BY e.id DESC LIMIT 4`, true},
		{"whole row", `SELECT "E" FROM events e ORDER BY e.id DESC LIMIT 4`, true},
		{"whole row JSON", `SELECT to_json(events) AS event_json FROM events ORDER BY id DESC LIMIT 4`, true},
		{"CTE person data", `WITH x AS (SELECT id, person_properties AS p FROM events) SELECT p FROM x ORDER BY id DESC LIMIT 4`, true},
		{"CTE star", `WITH x AS (SELECT * FROM events) SELECT event FROM x ORDER BY id DESC LIMIT 4`, true},
		{"where subquery", `SELECT uuid FROM events WHERE distinct_id IN (SELECT person_id FROM events) ORDER BY id`, true},
		{"union branch", `SELECT distinct_id AS value, id FROM events WHERE uuid='archive'
			UNION ALL SELECT person_id AS value, id FROM events WHERE uuid='edited' ORDER BY id`, true},
		{"property grouping", `SELECT json_extract(properties,'$.plan') AS plan, count(*) AS n FROM events GROUP BY 1 ORDER BY 1`, false},
		{"distinct count", `SELECT count(DISTINCT distinct_id) AS n FROM events`, false},
		{"natural join", `WITH x AS (SELECT 'identified' AS person_id) SELECT count(*) AS n FROM events NATURAL JOIN x`, true},
		{"person join key", `WITH x AS (SELECT 'identified' AS person_id) SELECT count(*) AS n FROM events JOIN x USING ("PERSON_ID")`, true},
		{"distinct join key", `WITH x AS (SELECT 'identified' AS distinct_id) SELECT count(*) AS n FROM events JOIN x USING (distinct_id)`, false},
		{"column aliases", `SELECT p FROM events e(c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,p) ORDER BY c1 DESC LIMIT 4`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			checked, err := nativequery.Inspect(ctx, p.DB, tc.sql)
			if err != nil {
				t.Fatal(err)
			}
			if checked.NeedsPerson() != tc.needsPerson {
				t.Fatalf("NeedsPerson() = %v, want %v", checked.NeedsPerson(), tc.needsPerson)
			}
			for _, bounds := range []*QueryTimeRange{nil, &window} {
				limits := DefaultQueryExecutionLimits()
				limits.TimeRange = bounds
				got, err := store.Query(ctx, project, tc.sql, nil, limits)
				if err != nil {
					t.Fatal(err)
				}
				// The unused CTE requires the full view without affecting the results.
				legacy := "WITH keep_person AS (SELECT person_id FROM events) SELECT * FROM (" + tc.sql + ")"
				want, err := store.Query(ctx, project, legacy, nil, limits)
				if err != nil {
					t.Fatal(err)
				}
				if got.Rows != want.Rows || !reflect.DeepEqual(got.Meta, want.Meta) || !reflect.DeepEqual(got.Data, want.Data) {
					t.Fatalf("window=%v: got %v (%v), want %v (%v)", bounds, got.Data, got.Meta, want.Data, want.Meta)
				}
			}
		})
	}

	for _, sql := range []string{
		`SELECT count(*) FROM _pc_events_core`,
		`SELECT count(*) FROM main._pc_events_core`,
		`SELECT identity_snapshot FROM events`,
		`WITH "EvEnTs" AS (SELECT 1 AS id) SELECT id FROM "EvEnTs"`,
	} {
		_, err := store.Query(ctx, project, sql, nil, DefaultQueryExecutionLimits())
		var policy *apierr.Error
		if !errors.As(err, &policy) || policy.Code != "unsupported_sql" {
			t.Fatalf("query %q accepted or returned the wrong error: %v", sql, err)
		}
	}
	// A schema-qualified row reference binds to the full view outside a window.
	qualified := `SELECT to_json(main.events) AS event_json FROM events WHERE uuid='purchase-1002'`
	got, err := store.Query(ctx, project, qualified, nil, DefaultQueryExecutionLimits())
	want, wantErr := store.Query(ctx, project, `SELECT to_json(events) AS event_json FROM events WHERE uuid='purchase-1002'`, nil, DefaultQueryExecutionLimits())
	if err != nil || wantErr != nil || got.Rows != 1 || !reflect.DeepEqual(got.Data, want.Data) {
		t.Fatalf("schema-qualified whole row differs: got %+v (%v), want %+v (%v)", got, err, want, wantErr)
	}
	for _, tc := range []struct {
		project, foreignEvent string
	}{{project, "neighbor-only"}, {neighbor, "purchase"}} {
		for _, bounds := range []*QueryTimeRange{nil, &window} {
			limits := DefaultQueryExecutionLimits()
			limits.TimeRange = bounds
			got, err := store.Query(ctx, tc.project, `SELECT count(*) AS n FROM events WHERE event=$foreign`,
				map[string]string{"foreign": tc.foreignEvent}, limits)
			if err != nil || got.Rows != 1 || string(got.Data[0]["n"]) != `"0"` {
				t.Fatalf("%s can query %q outside its project: %+v, %v", tc.project, tc.foreignEvent, got, err)
			}
		}
	}
}
