package porcupine

import (
	"context"
	"database/sql"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestNativePostgresMetadataTimeoutPolicy(t *testing.T) {
	t.Parallel()
	base := "postgres://operator:test-secret@localhost/lake?sslmode=require&connect_timeout=0&keepalives=0&tcp_user_timeout=0"
	parsed, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	q := parsed.Query()
	q.Set("options", `-c application_name=agent\ with\ spaces --statement-timeout=0 -clock_timeout=0 -c search_path=public`)
	parsed.RawQuery = q.Encode()
	protected, err := nativePostgresMetadataURL(parsed.String(), 10)
	if err != nil {
		t.Fatal(err)
	}
	normalized, err := url.Parse(protected)
	if err != nil {
		t.Fatal(err)
	}
	got := normalized.Query()
	for key, want := range map[string]string{"connect_timeout": "10", "keepalives": "1", "keepalives_idle": "10", "keepalives_interval": "1", "keepalives_count": "3", "tcp_user_timeout": "10000", "sslmode": "require"} {
		if got.Get(key) != want {
			t.Fatalf("native metadata setting %s was not enforced", key)
		}
	}
	args, err := splitPostgresOptions(got.Get("options"))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"-c", "application_name=agent with spaces", "-c", "search_path=public", "-c", "statement_timeout=10000", "-c", "lock_timeout=5000"}
	if !reflect.DeepEqual(args, want) {
		t.Fatalf("native startup option boundaries changed: %v", args)
	}
	if parsed.Query().Get("connect_timeout") != "0" {
		t.Fatal("mutated operator URI settings")
	}
	rawOptions := `-c application_name=agent\ -c\ statement_timeout=0`
	options, err := boundedPostgresOptions(rawOptions, 2)
	if err != nil {
		t.Fatal(err)
	}
	args, err = splitPostgresOptions(options)
	if err != nil {
		t.Fatal(err)
	}
	if args[1] != "application_name=agent -c statement_timeout=0" || args[len(args)-3] != "statement_timeout=2000" {
		t.Fatal("an escaped option value swallowed enforced settings")
	}
}

func TestNativePostgresMetadataRejectsAmbiguousOptions(t *testing.T) {
	t.Parallel()
	base := "postgres://operator:test-secret@localhost/lake"
	for _, raw := range []string{
		base + "?host=first,second", base + "?hostaddr=127.0.0.1%2C127.0.0.2", base + "?port=5432,5433",
		"postgres://first,second/lake", base + "?host=first&host=second", base + "?options=-c+statement_timeout%3D0&options=-c+lock_timeout%3D0",
		base + "?options=-c", base + "?options=-d+5", base + "?options=-c+%3Dvalue", base + "?options=-c+application_name%3Dbad%5C",
	} {
		_, err := nativePostgresMetadataURL(raw, 10)
		if err == nil {
			t.Fatal("accepted ambiguous native PostgreSQL URL")
		}
		if strings.Contains(err.Error(), "test-secret") {
			t.Fatal("native metadata validation exposed a credential")
		}
	}
	for _, seconds := range []int{-1, 0, 1, 61} {
		if _, err := nativePostgresMetadataURL(base, seconds); err == nil {
			t.Fatal("accepted invalid metadata timeout", seconds)
		}
	}
	config := DuckLakeConfig{PostgresURL: base, SharedSourceID: newID(), S3: &DuckLakeS3Config{}}
	if err := validateSharedNativeConfig(&config); err != nil || config.MetadataTimeoutSeconds != 10 {
		t.Fatal("native metadata default was not normalized", err)
	}
	if err := validateSharedNativeConfig(&DuckLakeConfig{MetadataTimeoutSeconds: 10}); err == nil {
		t.Fatal("local mode accepted a remote metadata setting")
	}
}

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
	store := duckLakeStore{config: DuckLakeConfig{PostgresURL: dsn, MetadataTimeoutSeconds: 2}}
	if err := store.configureSharedPostgres(ctx, native); err != nil {
		t.Fatal(err)
	}
	attach := "ATTACH 'ducklake:postgres:' AS lake (META_SECRET 'pc_analytics_postgres',METADATA_SCHEMA 'pc_timeout',DATA_PATH " + lakeLiteral(t.TempDir()) + ",DATA_INLINING_ROW_LIMIT 0)"
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
	store := openLakeStorageTest(t, t.TempDir(), 1)
	metadata, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	store.sharedCatalog = metadata
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
	if err := store.registry.PingContext(t.Context()); err != nil {
		t.Fatal("local registry was not healthy during metadata failure")
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
