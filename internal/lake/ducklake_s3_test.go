package lake

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestDuckLakeOpeningReservationsDoNotHoldGlobalMutex(t *testing.T) {
	t.Parallel()
	store := openLakeStorageTest(t, t.TempDir(), 2)
	ctx := context.Background()
	_, release, err := store.LeaseProject(ctx, "hot")
	if err != nil {
		t.Fatal(err)
	}
	release()
	// Block only the cold project's extension/engine opening using its reserved
	// slot. A hot project must remain usable while the reservation is present.
	pending := &projectOpening{done: make(chan struct{})}
	if err := store.Mu.Lock(ctx); err != nil {
		t.Fatal(err)
	}
	store.opening["cold"] = pending
	store.Mu.Unlock()
	if _, err := store.Query(ctx, "hot", "SELECT count(*) AS total FROM events", nil, DefaultQueryExecutionLimits()); err != nil {
		t.Fatal("opening reservation blocked the hot project", err)
	}
	deadline, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	if _, err := store.StageCheckpoint(deadline, func(context.Context, string) error { return nil }); !errors.Is(err, context.DeadlineExceeded) {
		cancel()
		t.Fatal("checkpoint bypassed pending catalog opening", err)
	}
	cancel()
	if err := store.Close(); err == nil {
		t.Fatal("active opening released the writer lifetime lock")
	}
	if err := store.DeleteProject(ctx, "cold"); err == nil {
		t.Fatal("deletion raced with an opening")
	}
	if err := store.Mu.Lock(ctx); err != nil {
		t.Fatal(err)
	}
	delete(store.opening, "cold")
	pending.err = errors.New("injected opening failure")
	close(pending.done)
	store.Mu.Unlock()
}

type secretErrorDriver struct{ reached *bool }

func (d secretErrorDriver) Open(string) (driver.Conn, error) {
	return &secretErrorConnection{reached: d.reached}, nil
}

type secretErrorConnection struct{ reached *bool }

func (c *secretErrorConnection) Prepare(string) (driver.Stmt, error) {
	return nil, errors.ErrUnsupported
}
func (c *secretErrorConnection) Close() error              { return nil }
func (c *secretErrorConnection) Begin() (driver.Tx, error) { return nil, errors.ErrUnsupported }
func (c *secretErrorConnection) ExecContext(_ context.Context, statement string, _ []driver.NamedValue) (driver.Result, error) {
	if strings.HasPrefix(statement, "CREATE SECRET") {
		*c.reached = true
		return nil, fmt.Errorf("native bind failure containing private statement: %s", statement)
	}
	return driver.RowsAffected(0), nil
}
