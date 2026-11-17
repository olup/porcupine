package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type fixtureObservation struct {
	Address string
	DataDir string
}

func TestNativePostgresLifecycle(t *testing.T) {
	if os.Getenv("PORCUPINE_TEST_NATIVE_POSTGRES") != "1" {
		t.Skip("run make shared-postgres-fixture-test")
	}
	for _, mode := range []string{"success", "failure", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			marker := filepath.Join(t.TempDir(), "ready.json")
			t.Setenv("PORCUPINE_POSTGRES_CHILD_MODE", mode)
			t.Setenv("PORCUPINE_POSTGRES_CHILD_MARKER", marker)
			executable, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			if mode == "cancel" {
				go func() {
					ticker := time.NewTicker(20 * time.Millisecond)
					defer ticker.Stop()
					for {
						if _, err := os.Stat(marker); err == nil {
							cancel()
							return
						}
						select {
						case <-ctx.Done():
							return
						case <-ticker.C:
						}
					}
				}()
			}
			err = run(ctx, []string{executable, "-test.run=^TestPostgresFixtureChild$"})
			if mode == "success" && err != nil {
				t.Fatal(err)
			}
			if mode == "failure" {
				var exit *exec.ExitError
				if !errors.As(err, &exit) || exit.ExitCode() != 7 {
					t.Fatalf("child failure lost: %v", err)
				}
			}
			if mode == "cancel" && (err == nil || ctx.Err() != context.Canceled) {
				t.Fatalf("child not canceled: %v, %v", err, ctx.Err())
			}
			data, err := os.ReadFile(marker)
			if err != nil {
				t.Fatalf("child did not verify PostgreSQL: %v", err)
			}
			var observed fixtureObservation
			if err := json.Unmarshal(data, &observed); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(observed.DataDir); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("temporary PostgreSQL data remains: %v", err)
			}
			connection, err := net.DialTimeout("tcp", observed.Address, time.Second)
			if err == nil {
				connection.Close()
				t.Fatal("PostgreSQL is still listening after fixture cleanup")
			}
		})
	}
}

func TestPostgresFixtureChild(t *testing.T) {
	mode := os.Getenv("PORCUPINE_POSTGRES_CHILD_MODE")
	if mode == "" {
		t.Skip("fixture subprocess only")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	dsn, err := url.Parse(os.Getenv("PORCUPINE_TEST_POSTGRES_DSN"))
	if err != nil || dsn.Hostname() != "127.0.0.1" || dsn.Port() == "0" || dsn.Port() == "" {
		t.Fatal("fixture must use a dynamic loopback endpoint")
	}
	db, err := sql.Open("pgx", dsn.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var version int
	var fsync, commit, pageWrites, listen, dataDir string
	err = db.QueryRowContext(ctx, `SELECT current_setting('server_version_num')::int,
		current_setting('fsync'), current_setting('synchronous_commit'),
		current_setting('full_page_writes'), current_setting('listen_addresses'),
		current_setting('data_directory')`).Scan(&version, &fsync, &commit, &pageWrites, &listen, &dataDir)
	if err != nil {
		t.Fatal(err)
	}
	if version/10000 != 17 || fsync != "on" || commit != "on" || pageWrites != "on" || listen != "127.0.0.1" {
		t.Fatalf("unexpected fixture configuration: %d %s %s %s %s", version, fsync, commit, pageWrites, listen)
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE fixture_test (value int); INSERT INTO fixture_test VALUES (42)`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(fixtureObservation{Address: dsn.Host, DataDir: dataDir})
	if err != nil {
		t.Fatal(err)
	}
	marker := os.Getenv("PORCUPINE_POSTGRES_CHILD_MARKER")
	if err := os.WriteFile(marker+".tmp", data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(marker+".tmp", marker); err != nil {
		t.Fatal(err)
	}
	switch mode {
	case "success":
		return
	case "failure":
		os.Exit(7)
	case "cancel":
		for {
			time.Sleep(time.Second)
		}
	default:
		t.Fatal("unknown child mode")
	}
}
