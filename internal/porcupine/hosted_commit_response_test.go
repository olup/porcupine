package porcupine

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"sync/atomic"

	"github.com/jackc/pgx/v5/stdlib"
	eventapi "github.com/olup/porcupine/internal/event"
)

// The real PG transaction commits; only its response is lost to the publisher.
// Other transactions, including reservation and checkpoint rereads, are ordinary.
type hostedLostCommitDriver struct{ lose *atomic.Bool }

func (d hostedLostCommitDriver) Open(dsn string) (driver.Conn, error) {
	c, e := stdlib.GetDefaultDriver().Open(dsn)
	if e != nil {
		return nil, e
	}
	return &hostedLostCommitConn{Conn: c, lose: d.lose}, nil
}

type hostedLostCommitConn struct {
	driver.Conn
	lose       *atomic.Bool
	publishing bool
}

func (c *hostedLostCommitConn) Begin() (driver.Tx, error) {
	c.publishing = false
	t, e := c.Conn.Begin()
	if e != nil {
		return nil, e
	}
	return hostedLostCommitTx{Tx: t, c: c}, nil
}
func (c *hostedLostCommitConn) BeginTx(ctx context.Context, o driver.TxOptions) (driver.Tx, error) {
	c.publishing = false
	t, e := c.Conn.(driver.ConnBeginTx).BeginTx(ctx, o)
	if e != nil {
		return nil, e
	}
	return hostedLostCommitTx{Tx: t, c: c}, nil
}
func (c *hostedLostCommitConn) ExecContext(ctx context.Context, q string, a []driver.NamedValue) (driver.Result, error) {
	if strings.HasPrefix(q, "UPDATE pipeline_effect_streams SET sequence=") {
		c.publishing = true
	}
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, q, a)
}
func (c *hostedLostCommitConn) QueryContext(ctx context.Context, q string, a []driver.NamedValue) (driver.Rows, error) {
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, q, a)
}

type hostedLostCommitTx struct {
	driver.Tx
	c *hostedLostCommitConn
}

func (t hostedLostCommitTx) Commit() error {
	if e := t.Tx.Commit(); e != nil {
		return e
	}
	if t.c.publishing && t.c.lose.CompareAndSwap(true, false) {
		return errors.New("injected lost actual PG commit response")
	}
	return nil
}
func installLostHostedCommit(dbURL string, p *hostedEffectPublisher, lose *atomic.Bool) (*sql.DB, error) {
	name := "hosted_lost_commit_" + eventapi.NewID()
	// Every fixture gets a separate registry entry without storing its DSN there.
	sql.Register(name, hostedLostCommitDriver{lose: lose})
	db, err := sql.Open(name, dbURL)
	if err != nil {
		return nil, err
	}
	meter := *p.usage
	meter.DB = db
	p.control = db
	p.usage = &meter
	return db, nil
}

// This bridge delegates every native statement to the already-open real engine.
// It injects failures only at the returned native commit/ACK boundary.
type hostedNativeBoundaryDriver struct {
	db                  *sql.DB
	loseCommit, failACK *atomic.Bool
}

func (d hostedNativeBoundaryDriver) Open(string) (driver.Conn, error) {
	c, e := d.db.Conn(context.Background())
	if e != nil {
		return nil, e
	}
	return &hostedNativeBoundaryConn{conn: c, loseCommit: d.loseCommit, failACK: d.failACK}, nil
}

type hostedNativeBoundaryConn struct {
	conn                *sql.Conn
	tx                  *sql.Tx
	loseCommit, failACK *atomic.Bool
	proof               bool
}

func (c *hostedNativeBoundaryConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("boundary fixture requires direct native statements")
}
func (c *hostedNativeBoundaryConn) Close() error { return c.conn.Close() }
func (c *hostedNativeBoundaryConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}
func (c *hostedNativeBoundaryConn) BeginTx(ctx context.Context, o driver.TxOptions) (driver.Tx, error) {
	tx, e := c.conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.IsolationLevel(o.Isolation), ReadOnly: o.ReadOnly})
	if e != nil {
		return nil, e
	}
	c.tx = tx
	c.proof = false
	return hostedNativeBoundaryTx{Tx: tx, c: c}, nil
}
func nativeBoundaryArgs(a []driver.NamedValue) []any {
	v := make([]any, len(a))
	for i, x := range a {
		v[i] = x.Value
	}
	return v
}
func (c *hostedNativeBoundaryConn) ExecContext(ctx context.Context, q string, a []driver.NamedValue) (driver.Result, error) {
	if strings.HasPrefix(q, "UPDATE lake.hosted_write_guard SET generation=generation+1,ack_effect_seq") && c.failACK.CompareAndSwap(true, false) {
		return nil, errors.New("injected real native ACK failure")
	}
	if strings.HasPrefix(q, "INSERT INTO lake.live_commit_proofs (") {
		c.proof = true
	}
	if c.tx != nil {
		return c.tx.ExecContext(ctx, q, nativeBoundaryArgs(a)...)
	}
	return c.conn.ExecContext(ctx, q, nativeBoundaryArgs(a)...)
}
func (c *hostedNativeBoundaryConn) QueryContext(ctx context.Context, q string, a []driver.NamedValue) (driver.Rows, error) {
	var r *sql.Rows
	var e error
	if c.tx != nil {
		r, e = c.tx.QueryContext(ctx, q, nativeBoundaryArgs(a)...)
	} else {
		r, e = c.conn.QueryContext(ctx, q, nativeBoundaryArgs(a)...)
	}
	if e != nil {
		return nil, e
	}
	cols, e := r.Columns()
	if e != nil {
		r.Close()
		return nil, e
	}
	return hostedNativeBoundaryRows{r: r, columns: cols}, nil
}

type hostedNativeBoundaryRows struct {
	r       *sql.Rows
	columns []string
}

func (r hostedNativeBoundaryRows) Columns() []string { return r.columns }
func (r hostedNativeBoundaryRows) Close() error      { return r.r.Close() }
func (r hostedNativeBoundaryRows) Next(v []driver.Value) error {
	if !r.r.Next() {
		if e := r.r.Err(); e != nil {
			return e
		}
		return io.EOF
	}
	values := make([]any, len(v))
	for i := range v {
		values[i] = &v[i]
	}
	return r.r.Scan(values...)
}

type hostedNativeBoundaryTx struct {
	*sql.Tx
	c *hostedNativeBoundaryConn
}

func (t hostedNativeBoundaryTx) Commit() error {
	e := t.Tx.Commit()
	t.c.tx = nil
	if e != nil {
		return e
	}
	if t.c.proof && t.c.loseCommit.CompareAndSwap(true, false) {
		return errors.New("injected lost real native commit response")
	}
	return nil
}
func (t hostedNativeBoundaryTx) Rollback() error { e := t.Tx.Rollback(); t.c.tx = nil; return e }
func installHostedNativeBoundary(db *sql.DB, lose, ack *atomic.Bool) (*sql.DB, error) {
	name := "hosted_native_boundary_" + eventapi.NewID()
	sql.Register(name, hostedNativeBoundaryDriver{db: db, loseCommit: lose, failACK: ack})
	return sql.Open(name, "")
}
