package porcupine

import (
	"context"
	"database/sql"
	"testing"
	"time"

	lakeapi "github.com/olup/porcupine/internal/lake"
)

func TestSharedNativeMetadataLockTimeout(t *testing.T) {
	t.Parallel()
	dsn := sharedPostgresDSN(t)
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Second)
	defer cancel()
	native, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer native.Close()
	for _, extension := range []string{"ducklake", "postgres"} {
		if _, err := native.ExecContext(ctx, "LOAD "+extension); err != nil {
			if _, err := native.ExecContext(ctx, "INSTALL "+extension+"; LOAD "+extension); err != nil {
				t.Fatal("native PostgreSQL test extension unavailable")
			}
		}
	}
	if _, err := native.ExecContext(ctx, "SET allow_persistent_secrets=false"); err != nil {
		t.Fatal(err)
	}
	store := lakeapi.DuckLakeStore{Config: lakeapi.DuckLakeConfig{PostgresURL: dsn, MetadataTimeoutSeconds: 2}}
	if err := store.ConfigureSharedPostgres(ctx, native); err != nil {
		t.Fatal(err)
	}
	attach := "ATTACH 'ducklake:postgres:' AS lake (META_SECRET 'pc_analytics_postgres',METADATA_SCHEMA 'pc_timeout',DATA_PATH " + lakeapi.LakeLiteral(t.TempDir()) + ",DATA_INLINING_ROW_LIMIT 0)"
	if _, err := native.ExecContext(ctx, attach); err != nil {
		t.Fatal("cannot attach isolated native metadata catalog")
	}
	if _, err := native.ExecContext(ctx, "CREATE TABLE lake.timeout_probe AS SELECT 1 AS id"); err != nil {
		t.Fatal(err)
	}
	control, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer control.Close()
	lock, err := control.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback()
	if _, err := lock.ExecContext(ctx, "LOCK TABLE pc_timeout.ducklake_snapshot IN ACCESS EXCLUSIVE MODE"); err != nil {
		t.Fatal(err)
	}
	read, stop := context.WithCancel(ctx)
	defer stop()
	returned := make(chan error, 1)
	started := time.Now()
	go func() {
		var rows int
		returned <- native.QueryRowContext(read, "SELECT count(*) FROM lake.timeout_probe").Scan(&rows)
	}()
	blocked := false
	for time.Since(started) < time.Second {
		if err := control.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND wait_event_type='Lock' AND query ILIKE '%ducklake_snapshot%')`).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		if blocked {
			break
		}
		select {
		case err := <-returned:
			t.Fatal("metadata read did not reach the isolated server lock", err)
		case <-time.After(10 * time.Millisecond):
		}
	}
	if !blocked {
		t.Fatal("did not observe native metadata waiting for its PostgreSQL lock")
	}
	stop()
	select {
	case err := <-returned:
		if err == nil {
			t.Fatal("blocked native metadata read unexpectedly succeeded")
		}
		if time.Since(started) > 4*time.Second {
			t.Fatal("native metadata lock wait exceeded configured grace")
		}
	case <-time.After(4 * time.Second):
		t.Fatal("native metadata read ignored server-side timeout after context cancellation")
	}
}

func TestSharedNativeSourceIDMetadataProbeBounded(t *testing.T) {
	t.Parallel()
	dsn := sharedPostgresDSN(t)
	store := openSharedNativeMetadataTest(t, dsn)
	metadata := store.SharedCatalog
	metadata.SetMaxOpenConns(1)
	held, err := metadata.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	bounded, cancel := context.WithTimeout(t.Context(), 75*time.Millisecond)
	started := time.Now()
	_, err = store.SourceID(bounded)
	cancel()
	if err == nil || time.Since(started) > time.Second {
		t.Fatal("metadata health probe did not honor its caller deadline")
	}
	if store.Registry != nil {
		t.Fatal("metadata health retained an authoritative local registry")
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.SourceID(t.Context()); err != nil {
		t.Fatal("healthy shared metadata was not reachable")
	}
	if err := metadata.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.SourceID(t.Context()); err == nil {
		t.Fatal("closed metadata pool was reported healthy from local identity")
	}
}
