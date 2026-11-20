package porcupine

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	eventapi "github.com/olup/porcupine/internal/event"
	lakeapi "github.com/olup/porcupine/internal/lake"
)

func openLakeStorageTest(t *testing.T, directory string, maxOpen int) *lakeapi.DuckLakeStore {
	t.Helper()
	s, err := lakeapi.NewDuckLake(lakeapi.DuckLakeConfig{Directory: directory, MemoryLimitMB: 128, Threads: 1, MaxOpenProjects: maxOpen})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}

func openSharedNativeMetadataTest(t *testing.T, dsn string) *lakeapi.DuckLakeStore {
	t.Helper()
	// Health exercises real PG registry authority without requiring analytical
	// objects. Any accidental object request fails this metadata-only fixture.
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("metadata health fixture accessed analytical objects")
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(endpoint.Close)
	store, err := lakeapi.NewDuckLake(lakeapi.DuckLakeConfig{
		Directory: t.TempDir(), PostgresURL: dsn, SharedSourceID: eventapi.NewID(),
		MemoryLimitMB: 128, Threads: 1, MaxOpenProjects: 1, DisableCleanup: true,
		S3: &lakeapi.DuckLakeS3Config{Bucket: "metadata-probe", Prefix: "native", Region: "us-east-1", Endpoint: endpoint.URL, UsePathStyle: true, AccessKeyID: "fixture", SecretAccessKey: "fixture"},
	})
	if err != nil {
		t.Fatal(err)
	}
	s := store
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}

func lakeStorageProjection(t *testing.T, project, uuid string, id int64) lakeapi.EventProjection {
	t.Helper()
	e := eventapi.Event{ID: id, ProjectID: project, UUID: uuid, Event: "page_viewed", DistinctID: "anonymous", Properties: json.RawMessage(`{"page":"/home"}`), Timestamp: "2026-11-13T10:00:00Z", ReceivedAt: "2026-11-13T10:00:01Z"}
	snapshot, err := lakeapi.OriginalSnapshot(e)
	if err != nil {
		t.Fatal(err)
	}
	return lakeapi.EventProjection{Event: e, OriginalSnapshot: snapshot}
}

func lakeStorageCount(t *testing.T, s *lakeapi.DuckLakeStore, project, table string) int {
	t.Helper()
	p, release, err := s.LeaseProject(context.Background(), project)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	var count int
	if err := p.DB.QueryRow("SELECT count(*) FROM " + table).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

type secretErrorDriver struct{ reached *bool }

func (d secretErrorDriver) Open(string) (driver.Conn, error) {
	return &secretErrorConnection{reached: d.reached}, nil
}

type secretErrorConnection struct{ reached *bool }

func (c *secretErrorConnection) Prepare(string) (driver.Stmt, error) {
	return nil, errors.ErrUnsupported
}

func (c *secretErrorConnection) Close() error { return nil }

func (c *secretErrorConnection) Begin() (driver.Tx, error) { return nil, errors.ErrUnsupported }

func (c *secretErrorConnection) ExecContext(_ context.Context, statement string, _ []driver.NamedValue) (driver.Result, error) {
	if strings.HasPrefix(statement, "CREATE SECRET") {
		*c.reached = true
		return nil, fmt.Errorf("native bind failure containing private statement: %s", statement)
	}
	return driver.RowsAffected(0), nil
}
