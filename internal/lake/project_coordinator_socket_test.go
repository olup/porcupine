package lake

import (
	"database/sql"
	"errors"
	"testing"

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
	prepare := func() (int, [4]int) {
		t.Helper()
		conn := checkout()
		defer conn.Close()
		if _, err := conn.ExecContext(ctx, `SET tcp_keepalives_idle=120;
 SET tcp_keepalives_interval=3; SET tcp_keepalives_count=7; SET tcp_user_timeout=80000`); err != nil {
			t.Fatal(err)
		}
		return readSocket(conn)
	}

	ownerPID, ownerSettings := prepare()
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
	if pid, current := readSocket(owner.conn); pid != ownerPID || current != want {
		t.Fatalf("owner backend %d has socket settings %v, want %d with %v", pid, current, ownerPID, want)
	}

	busyPID, busySettings := prepare()
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
