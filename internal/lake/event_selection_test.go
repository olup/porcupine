package lake

import (
	"database/sql"
	"errors"
	"testing"

	"github.com/olup/porcupine/internal/nativequery"
)

func TestEventDeletionPredicateParsing(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	_, err = db.ExecContext(t.Context(), `CREATE VIEW events AS SELECT 'keep; --' AS uuid, 0::UBIGINT AS revision,
 9007199254740993::BIGINT AS id, '2026-01-01T00:00:00Z' AS received_at, 0::UTINYINT AS is_imported`)
	if err != nil {
		t.Fatal(err)
	}
	predicate := "/* ordinary comment */ uuid=$uuid AND id=9007199254740993 AND uuid IN (SELECT uuid FROM events) -- trailing comment"
	canonical, args, statement, err := nativequery.PrepareEventSelection(t.Context(), db, predicate, map[string]string{"uuid": "keep; --"})
	if err != nil {
		t.Fatal(err)
	}
	if !statement.UsesEvents() || !statement.HasParameter("uuid") {
		t.Fatal("selection lost its project source or parameter")
	}
	var uuid, received string
	var revision uint64
	var id int64
	var imported uint8
	if err := db.QueryRowContext(t.Context(), canonical, args...).Scan(&uuid, &revision, &id, &received, &imported); err != nil {
		t.Fatal(err)
	}
	if uuid != "keep; --" || id != 9007199254740993 {
		t.Fatalf("unexpected match %q / %d", uuid, id)
	}
	for _, bad := range []string{
		"true;", "true; SELECT false", "true) OR (false", "true) --", "true /*", "true, false",
		"true AS predicate", "true FROM events", "true ORDER BY uuid", "true LIMIT 0",
		"EXISTS (SELECT uuid FROM other.events)", "EXISTS (SELECT uuid FROM lake.current_events)", "identity_snapshot IS NULL", "current_setting('threads') IS NULL",
	} {
		_, _, _, err := nativequery.PrepareEventSelection(t.Context(), db, bad, nil)
		var policyErr *nativequery.Error
		if !errors.As(err, &policyErr) {
			t.Errorf("predicate %q returned %v, want a policy error", bad, err)
		}
	}
}
