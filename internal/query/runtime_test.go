package query

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func TestFinishReservationPreservesFirstResult(t *testing.T) {
	t.Parallel()
	control, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = control.Close() })
	_, err = control.ExecContext(t.Context(), `
CREATE TABLE tenants (id TEXT, hot_query_max_concurrent INT, hot_query_timeout_ms INT, hot_query_max_rows INT, hot_query_max_result_bytes INT);
CREATE TABLE projects (id TEXT, tenant_id TEXT);
CREATE TABLE project_deletions (project_id TEXT);
INSERT INTO tenants VALUES ('tenant', 2, 60000, 1000, 1048576);
INSERT INTO projects VALUES ('project', 'tenant');`)
	if err != nil {
		t.Fatal(err)
	}
	limits, err := Normalize(&InteractiveQueryConfig{TimeoutMS: 60000})
	if err != nil {
		t.Fatal(err)
	}
	runtime := New(limits, time.Now, nil)
	runtime.Bind(control, "", 0)
	native, _, reservation, err := runtime.admit(t.Context(), "project")
	if err != nil {
		t.Fatal(err)
	}
	if err := reservation.finish(); err != nil {
		t.Fatal(err)
	}
	if native.Err() != context.Canceled || runtime.UsedSlots() != 0 {
		t.Fatalf("finish left context error %v and %d reserved slots", native.Err(), runtime.UsedSlots())
	}
	if err := reservation.finish(); err != nil {
		t.Fatalf("second finish changed the result to %v", err)
	}
}
