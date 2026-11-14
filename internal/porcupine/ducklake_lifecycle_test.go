//go:build ducklake

package porcupine

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"
)

func TestDuckLakeMaintenanceLeavesIdleProjectsEvictable(t *testing.T) {
	s := openLakeStorageTest(t, t.TempDir(), 2)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for i, name := range []string{"a", "b"} {
		if err := s.Apply(ctx, []eventProjection{lakeStorageProjection(t, name, name, int64(i+1))}); err != nil {
			t.Fatal(err)
		}
	}
	a, b := s.projects["a"], s.projects["b"]
	if err := a.maintenance.Lock(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.maintenance.Lock(ctx); err != nil {
		a.maintenance.Unlock()
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- s.Maintain(ctx) }()
	var once sync.Once
	finish := func() {
		once.Do(func() {
			a.maintenance.Unlock()
			b.maintenance.Unlock()
			if err := <-done; err != nil {
				t.Errorf("maintenance failed after allowing the cold read: %v", err)
			}
		})
	}
	defer finish()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		if err := s.mu.Lock(ctx); err != nil {
			t.Fatal(err)
		}
		active := len(a.leases) + len(b.leases)
		s.mu.Unlock()
		if active > 0 {
			break
		}
		select {
		case <-tick.C:
		case <-deadline.C:
			t.Fatal("maintenance never acquired a resident project")
		}
	}
	result, err := s.Query(ctx, "cold", "SELECT count(*) AS total FROM events", nil)
	if err != nil {
		t.Fatalf("cold-project query failed while idle resident projects were available: status=%d error=%v", queryStatus(err), err)
	}
	if len(result.Data) != 1 || string(result.Data[0]["total"]) != `"0"` {
		t.Fatalf("unexpected cold-project result: %+v", result)
	}
	if len(s.projects) != 2 {
		t.Fatalf("resident project budget grew to %d", len(s.projects))
	}
	evicted := "a"
	if s.projects[evicted] != nil {
		evicted = "b"
	}
	if err := s.DeleteProject(ctx, evicted); err != nil {
		t.Fatal(err)
	}
	finish()
	if _, err := os.Stat(s.projectDirectory(evicted)); !os.IsNotExist(err) {
		t.Fatalf("maintenance recreated a deleted or evicted project: %v", err)
	}
}

func TestDuckLakeProjectAdmissionIsResourceLimit(t *testing.T) {
	s := openLakeStorageTest(t, t.TempDir(), 1)
	ctx := context.Background()
	_, release, err := s.leaseProject(ctx, "hot")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	for name, operation := range map[string]func() error{
		"query":   func() error { _, err := s.Query(ctx, "cold", "SELECT count(*) FROM events", nil); return err },
		"catalog": func() error { _, err := s.Catalog(ctx, EventCatalogInput{ID: "cold"}); return err },
		"export":  func() error { _, err := s.ValidateExport(ctx, "cold", "SELECT * FROM events", nil, false); return err },
	} {
		t.Run(name, func(t *testing.T) {
			err := operation()
			var detail *managementError
			if queryStatus(err) != 429 || !errors.As(err, &detail) || detail.Code != "active_project_limit" || detail.Hint == "" {
				t.Fatalf("busy project budget reported storage failure: status=%d error=%v", queryStatus(err), err)
			}
		})
	}
}

func TestDuckLakeProjectsReopenDuringMaintenance(t *testing.T) {
	s := openLakeStorageTest(t, t.TempDir(), 2)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	names := []string{"a", "b", "c", "d", "e"}
	for i, name := range names {
		if err := s.Apply(ctx, []eventProjection{lakeStorageProjection(t, name, name, int64(i+1))}); err != nil {
			t.Fatal(err)
		}
	}
	maintenance, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-maintenance.Done():
				done <- nil
				return
			case <-tick.C:
				if err := s.Maintain(maintenance); err != nil && maintenance.Err() == nil {
					done <- err
					return
				}
			}
		}
	}()
	defer func() {
		stop()
		if err := <-done; err != nil {
			t.Errorf("concurrent maintenance failed: %v", err)
		}
	}()
	for round := 0; round < 4; round++ {
		for _, name := range names {
			result, err := s.Query(ctx, name, "SELECT uuid FROM events", nil)
			if err != nil {
				t.Fatalf("round %d project %s failed on first query: %v", round, name, err)
			}
			if len(result.Data) != 1 || string(result.Data[0]["uuid"]) != `"`+name+`"` {
				t.Fatalf("reopening mixed or lost project %s data: %+v", name, result)
			}
		}
	}
}

func TestDuckLakeProjectErrorsKeepStorageFailuresPrivate(t *testing.T) {
	for _, failure := range []error{
		errors.New("private catalog path"),
		&duckdb.Error{Type: duckdb.ErrorTypeBinder, Msg: "private catalog column"},
	} {
		err := duckLakeProjectError(context.Background(), failure)
		if queryStatus(err) != 503 || strings.Contains(err.Error(), "private") {
			t.Fatalf("catalog opening error misclassified or exposed: %v", err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := duckLakeProjectError(ctx, context.Canceled); queryStatus(err) != 429 {
		t.Fatalf("canceled project admission is not a resource limit: %v", err)
	}
}
