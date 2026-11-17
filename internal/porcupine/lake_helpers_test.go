package porcupine

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func openLakeStorageTest(t *testing.T, directory string, maxOpen int) *duckLakeStore {
	t.Helper()
	lake, err := newDuckLake(DuckLakeConfig{Directory: directory, MemoryLimitMB: 128, Threads: 1, MaxOpenProjects: maxOpen})
	if err != nil {
		t.Fatal(err)
	}
	s := lake.(*duckLakeStore)
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}

func lakeStorageProjection(t *testing.T, project, uuid string, id int64) eventProjection {
	t.Helper()
	e := Event{ID: id, ProjectID: project, UUID: uuid, Event: "page_viewed", DistinctID: "anonymous", Properties: json.RawMessage(`{"page":"/home"}`), Timestamp: "2026-11-13T10:00:00Z", ReceivedAt: "2026-11-13T10:00:01Z"}
	snapshot, err := originalSnapshot(e)
	if err != nil {
		t.Fatal(err)
	}
	return eventProjection{Event: e, OriginalSnapshot: snapshot}
}

func lakeStorageCount(t *testing.T, s *duckLakeStore, project, table string) int {
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
