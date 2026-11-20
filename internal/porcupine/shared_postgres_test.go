package porcupine

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/testfixtures/postgresfixture"
)

func sharedPostgresDSN(t *testing.T) string {
	t.Helper()
	return postgresfixture.DSN(t)
}

func TestSharedPostgresFixture(t *testing.T) {
	t.Parallel()
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
