package porcupine

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	eventapi "github.com/olup/porcupine/internal/event"
	lakeapi "github.com/olup/porcupine/internal/lake"
)

func openLakeStorageTest(t *testing.T, directory string, maxOpen int) *lakeapi.DuckLakeStore {
	t.Helper()
	lake, err := lakeapi.NewDuckLake(lakeapi.DuckLakeConfig{Directory: directory, MemoryLimitMB: 128, Threads: 1, MaxOpenProjects: maxOpen})
	if err != nil {
		t.Fatal(err)
	}
	s := lake.(*lakeapi.DuckLakeStore)
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
