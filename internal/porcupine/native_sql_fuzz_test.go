package porcupine

import (
	"context"
	"database/sql"
	"testing"
	"time"

	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/nativequery"
)

func FuzzValidateNativeSQL(f *testing.F) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		f.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	f.Cleanup(func() { db.Close() })
	if _, err = db.Exec(`SET threads=1; SET memory_limit='64MiB'; SET enable_external_access=false; SET autoinstall_known_extensions=false; SET autoload_known_extensions=false; SET lock_configuration=true`); err != nil {
		f.Fatal(err)
	}
	for _, query := range []string{
		"SELECT count(*) FROM events",
		"WITH recent AS (SELECT uuid FROM events) SELECT count(*) FROM recent",
		"SELECT '雪 ; DELETE FROM events' AS value FROM events",
		"SELECT row_number() OVER (PARTITION BY event ORDER BY id) FROM events",
		"SELECT * FROM events UNION ALL SELECT * FROM lake.current_events",
		"SELECT * FROM read_csv('/etc/passwd')",
		"SELECT current_setting('threads') FROM events",
		"SELECT * FROM events; DROP TABLE events",
	} {
		f.Add(query)
	}
	f.Fuzz(func(t *testing.T, query string) {
		if len(query) > lakeapi.MaxSQLBytes {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		canonical, args, err := nativequery.Prepare(ctx, db, query, nil)
		if err != nil {
			return
		}
		if canonical == "" || len(args) != 0 {
			t.Fatal("accepted unbound SQL has invalid canonical result")
		}
		// Round-trip only the checked AST. Neither the original nor canonical query
		// is executed, so external-source seeds cannot read files or issue requests.
		again, _, err := nativequery.Prepare(ctx, db, canonical, nil)
		if ctx.Err() != nil {
			return
		}
		if err != nil || again != canonical {
			t.Fatalf("accepted SQL is not stable under canonical validation: %v", err)
		}
	})
}
