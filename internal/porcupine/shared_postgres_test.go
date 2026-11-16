package porcupine

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"net/url"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// sharedPostgresDSN gives each test its own disposable database. Absence of the
// integration setting skips this suite; a configured but unreachable DB fails.
func sharedPostgresDSN(t *testing.T) string {
	t.Helper()
	raw := os.Getenv("PORCUPINE_TEST_POSTGRES_DSN")
	if raw == "" {
		t.Skip("set PORCUPINE_TEST_POSTGRES_DSN or run make shared-postgres-test")
	}
	dsn, err := url.Parse(raw)
	if err != nil || (dsn.Scheme != "postgres" && dsn.Scheme != "postgresql") || dsn.Host == "" {
		t.Fatal("PORCUPINE_TEST_POSTGRES_DSN must be a PostgreSQL URL")
	}
	admin, err := sql.Open("pgx", raw)
	if err != nil {
		t.Fatal("cannot open the configured PostgreSQL fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := admin.PingContext(ctx); err != nil {
		admin.Close()
		t.Fatal("configured PostgreSQL fixture is unavailable")
	}
	var suffix [12]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	name := "porcupine_test_" + hex.EncodeToString(suffix[:])
	// Only this generated ASCII identifier is interpolated; caller input is never
	// used in a CREATE/DROP DATABASE statement.
	if _, err := admin.ExecContext(ctx, `CREATE DATABASE "`+name+`"`); err != nil {
		admin.Close()
		t.Fatal("cannot create an isolated PostgreSQL test database", err)
	}
	t.Cleanup(func() {
		defer admin.Close()
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if _, err := admin.ExecContext(cleanup, `DROP DATABASE "`+name+`" WITH (FORCE)`); err != nil {
			t.Error("cannot remove the isolated PostgreSQL test database", err)
		}
	})
	dsn.Path, dsn.RawPath = "/"+name, ""
	return dsn.String()
}

func TestSharedPostgresFixture(t *testing.T) {
	dsn := sharedPostgresDSN(t)
	first, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := first.ExecContext(ctx, `CREATE TABLE fixture_probe(id integer PRIMARY KEY); INSERT INTO fixture_probe VALUES(1)`); err != nil {
		t.Fatal(err)
	}
	var rows int
	if err := second.QueryRowContext(ctx, `SELECT count(*) FROM fixture_probe`).Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("independent PostgreSQL connections did not share committed data: rows=%d error=%v", rows, err)
	}
}
