package lake

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"
	"github.com/olup/porcupine/internal/apierr"
)

func TestDuckLakeMaintenanceLeavesIdleProjectsEvictable(t *testing.T) {
	t.Parallel()
	s := openLakeStorageTest(t, t.TempDir(), 2)
	ctx := t.Context()
	const operationTimeout = 10 * time.Second
	for i, name := range []string{"a", "b"} {
		seed, cancel := context.WithTimeout(ctx, operationTimeout)
		err := s.Apply(seed, []EventProjection{lakeStorageProjection(t, name, name, int64(i+1))})
		cancel()
		if err != nil {
			t.Fatal(err)
		}
	}
	a, b := s.Projects["a"], s.Projects["b"]
	if err := a.maintenance.Lock(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.maintenance.Lock(ctx); err != nil {
		a.maintenance.Unlock()
		t.Fatal(err)
	}
	maintenance, stopMaintenance := context.WithCancel(ctx)
	defer stopMaintenance()
	done := make(chan error, 1)
	go func() { done <- s.Maintain(maintenance) }()
	var once sync.Once
	finish := func() {
		once.Do(func() {
			// The lock wait is intentional. Give native maintenance its own
			// bounded execution window after the cold query and deletion.
			deadline := time.AfterFunc(operationTimeout, stopMaintenance)
			defer deadline.Stop()
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
		if err := s.Mu.Lock(ctx); err != nil {
			t.Fatal(err)
		}
		active := len(a.leases) + len(b.leases)
		s.Mu.Unlock()
		if active > 0 {
			break
		}
		select {
		case <-tick.C:
		case <-deadline.C:
			t.Fatal("maintenance never acquired a resident project")
		}
	}
	result, err := s.Query(ctx, "cold", "SELECT count(*) AS total FROM events", nil, DefaultQueryExecutionLimits())
	if err != nil {
		t.Fatalf("cold-project query failed while idle resident projects were available: status=%d error=%v", queryStatus(err), err)
	}
	if len(result.Data) != 1 || string(result.Data[0]["total"]) != `"0"` {
		t.Fatalf("unexpected cold-project result: %+v", result)
	}
	if len(s.Projects) != 2 {
		t.Fatalf("resident project budget grew to %d", len(s.Projects))
	}
	evicted := "a"
	if s.Projects[evicted] != nil {
		evicted = "b"
	}
	deletion, cancel := context.WithTimeout(ctx, operationTimeout)
	err = s.DeleteProject(deletion, evicted)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	finish()
	if _, err := os.Stat(s.ProjectDirectory(evicted)); !os.IsNotExist(err) {
		t.Fatalf("maintenance recreated a deleted or evicted project: %v", err)
	}
}

func TestDuckLakeProjectAdmissionIsResourceLimit(t *testing.T) {
	t.Parallel()
	s := openLakeStorageTest(t, t.TempDir(), 1)
	ctx := context.Background()
	_, release, err := s.LeaseProject(ctx, "hot")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	for name, operation := range map[string]func() error{
		"query": func() error {
			_, err := s.Query(ctx, "cold", "SELECT count(*) FROM events", nil, DefaultQueryExecutionLimits())
			return err
		},
		"catalog": func() error { _, err := s.Catalog(ctx, EventCatalogInput{ID: "cold"}); return err },
		"export":  func() error { _, err := s.ValidateExport(ctx, "cold", "SELECT * FROM events", nil, false); return err },
	} {
		t.Run(name, func(t *testing.T) {
			err := operation()
			var detail *apierr.Error
			if queryStatus(err) != 429 || !errors.As(err, &detail) || detail.Code != "active_project_limit" || detail.Hint == "" {
				t.Fatalf("busy project budget reported storage failure: status=%d error=%v", queryStatus(err), err)
			}
		})
	}
}

func TestDuckLakeProjectsReopenDuringMaintenance(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	s := openLakeStorageTest(t, t.TempDir(), 2)
	names := []string{"a", "b", "c", "d", "e"}
	const rounds = 4
	limits := DefaultQueryExecutionLimits()
	// Budget the complete fixture separately from each query's unchanged limit.
	// A fixed 30-second total imposed an incidental throughput requirement on
	// twenty cold opens competing with the full race suite on smaller runners.
	operations := len(names)*(rounds+1) + 1
	ctx, cancel := context.WithTimeout(t.Context(), time.Duration(operations)*limits.Timeout)
	defer cancel()
	for i, name := range names {
		if err := s.Apply(ctx, []EventProjection{lakeStorageProjection(t, name, name, int64(i+1))}); err != nil {
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
	for round := 0; round < rounds; round++ {
		for _, name := range names {
			result, err := s.Query(ctx, name, "SELECT uuid FROM events", nil, limits)
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
	t.Parallel()
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
