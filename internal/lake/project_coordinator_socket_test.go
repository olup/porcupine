package lake

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/testfixtures/postgresfixture"
)

func TestSharedPostgresCoordinatorScopesSocketSettings(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db, err := sql.Open("pgx", postgresfixture.DSN(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(3)
	db.SetMaxIdleConns(1)
	if _, err := db.ExecContext(ctx, ProjectCoordinatorSchemaV1); err != nil {
		t.Fatal(err)
	}
	config := DefaultProjectCoordinatorConfig()
	config.MaxConcurrent = 2
	coordinator, err := NewPostgresProjectCoordinator(db, config)
	if err != nil {
		t.Fatal(err)
	}
	var linuxTCP bool
	if err := db.QueryRowContext(ctx, "SELECT inet_client_addr() IS NOT NULL AND split_part(version(),',',1) LIKE '%linux%'").Scan(&linuxTCP); err != nil {
		t.Fatal(err)
	}
	checkout := func() *sql.Conn {
		t.Helper()
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		return conn
	}
	readSocket := func(conn *sql.Conn) (int, [4]int) {
		t.Helper()
		var pid int
		var values [4]int
		err := conn.QueryRowContext(ctx, `SELECT pg_backend_pid(),current_setting('tcp_keepalives_idle'),
 current_setting('tcp_keepalives_interval'),current_setting('tcp_keepalives_count'),current_setting('tcp_user_timeout')`).
			Scan(&pid, &values[0], &values[1], &values[2], &values[3])
		if err != nil {
			t.Fatal(err)
		}
		return pid, values
	}
	prepare := func(userTimeout int) (int, [4]int) {
		t.Helper()
		conn := checkout()
		defer conn.Close()
		if _, err := conn.ExecContext(ctx, `SET tcp_keepalives_idle=120;
 SET tcp_keepalives_interval=3; SET tcp_keepalives_count=7`); err != nil {
			t.Fatal(err)
		}
		if linuxTCP {
			if _, err := conn.ExecContext(ctx, "SELECT set_config('tcp_user_timeout',$1::int::text,false)", userTimeout); err != nil {
				t.Fatal(err)
			}
		}
		return readSocket(conn)
	}

	ownerPID, ownerSettings := prepare(0)
	key := ProjectWriteKey{SourceID: "source", ProjectID: "socket-settings"}
	owner, err := coordinator.Acquire(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Release() })
	want := ownerSettings
	for i, limit := range [4]int{15, 5, 3, 30000} {
		if want[i] > 0 {
			want[i] = min(want[i], limit)
		}
	}
	if linuxTCP && ownerSettings[3] == 0 {
		want[3] = 30000
	}
	if pid, current := readSocket(owner.conn); pid != ownerPID || current != want {
		t.Fatalf("owner backend %d has socket settings %v, want %d with %v", pid, current, ownerPID, want)
	}

	busyPID, busySettings := prepare(80000)
	if _, err := coordinator.TryAcquire(ctx, key); !errors.Is(err, ErrProjectWriteBusy) {
		t.Fatalf("contended acquisition returned %v", err)
	}
	busyConn := checkout()
	defer busyConn.Close()
	if pid, current := readSocket(busyConn); pid != busyPID || current != busySettings {
		t.Fatalf("contended backend %d has socket settings %v, want %d with %v", pid, current, busyPID, busySettings)
	}

	if err := owner.Release(); err != nil {
		t.Fatal(err)
	}
	if pid, current := readSocket(checkout()); pid != ownerPID || current != ownerSettings {
		t.Fatalf("released backend %d has socket settings %v, want %d with %v", pid, current, ownerPID, ownerSettings)
	}
}

func TestSharedPostgresCoordinatorDiscardsBackendAfterFailedEpoch(t *testing.T) {
	t.Parallel()
	db, coordinator := nativeFenceControl(t)
	db.SetMaxIdleConns(1)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, "ALTER SEQUENCE shared_native_write_epoch CACHE 2"); err != nil {
		t.Fatal(err)
	}
	var pid int
	if err := db.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&pid); err != nil {
		t.Fatal(err)
	}
	key := ProjectWriteKey{SourceID: "source", ProjectID: "invalid-epoch"}
	if owner, err := coordinator.Acquire(ctx, key); !errors.Is(err, ErrProjectWriteSessionLost) {
		if owner != nil {
			_ = owner.Release()
		}
		t.Fatalf("invalid epoch sequence returned %v", err)
	}
	waitForProjectBackendExit(t, ctx, db, pid)

	if _, err := db.ExecContext(ctx, "ALTER SEQUENCE shared_native_write_epoch CACHE 1"); err != nil {
		t.Fatal(err)
	}
	successor, err := coordinator.TryAcquire(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = successor.Release() })
	if successor.epoch != 1 {
		t.Fatalf("failed acquisition advanced the epoch, got %d", successor.epoch)
	}
}

func waitForProjectBackendExit(t *testing.T, ctx context.Context, db *sql.DB, pid int) {
	t.Helper()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		var exists bool
		if err := db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1)", pid).Scan(&exists); err != nil {
			t.Fatalf("check for PostgreSQL backend %d: %v", pid, err)
		}
		if !exists {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("discarded PostgreSQL backend %d still exists: %v", pid, ctx.Err())
		case <-tick.C:
		}
	}
}
