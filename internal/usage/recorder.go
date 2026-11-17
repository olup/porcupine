package usage

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"
)

var columns = []string{"live_events", "import_events", "query_count", "query_micros", "export_bytes", "pipeline_bytes", "webhook_success"}

type Recorder struct {
	DB       *sql.DB
	SourceID string
	Clock    func() time.Time
	mu       sync.Mutex
	observe  sync.Mutex
	active   map[string]*Execution
}

type Execution struct {
	recorder *Recorder
	id       string
	started  time.Time
	mu       sync.Mutex
	ended    bool
	elapsed  int64
}

func NewID() string {
	var raw [18]byte
	if _, err := rand.Read(raw[:]); err != nil {
		panic(err)
	}
	return base64.RawURLEncoding.EncodeToString(raw[:])
}
func NewRecorder(db *sql.DB, source string, clock func() time.Time) *Recorder {
	return &Recorder{DB: db, SourceID: source, Clock: clock, active: map[string]*Execution{}}
}

// AddTx joins an existing domain transaction. It stores one cumulative update,
// never one row or trigger for each event. The authoritative journal project
// anchor supplies the tenant.
func (r *Recorder) AddTx(ctx context.Context, tx *sql.Tx, project string, at time.Time, delta Counters, unresolved int64) error {
	return r.addTx(ctx, tx, "", project, at, delta, unresolved)
}

var (
	domainPosting = postingSQL(false, false)
	scopedPosting = postingSQL(true, false)
	domainFinish  = postingSQL(false, true)
	scopedFinish  = postingSQL(true, true)
)

// These statements depend only on the metric dictionary, not on a request.
func postingSQL(scoped, finish bool) string {
	set := []string{"revision=usage_rollups.revision+1", "unresolved_queries=usage_rollups.unresolved_queries+excluded.unresolved_queries", "last_observed_at=CASE WHEN excluded.last_observed_at>usage_rollups.last_observed_at THEN excluded.last_observed_at ELSE usage_rollups.last_observed_at END"}
	guards := []string{"usage_rollups.revision<9223372036854775807", "usage_rollups.unresolved_queries<=9223372036854775807-excluded.unresolved_queries"}
	for _, c := range columns {
		set = append(set, c+"=usage_rollups."+c+"+excluded."+c)
		guards = append(guards, "usage_rollups."+c+"<=9223372036854775807-excluded."+c)
	}
	// Ordinary domain postings resolve the journal anchor; queries carry their
	// already authorized scope so reads need no local project metadata.
	selector := `SELECT $1,tenant_id,id,$2,1,$3,$4,$5,$6,$7,$8,$9,$10,$11 FROM projects WHERE id=$12`
	if scoped {
		selector = `VALUES($1,$13,$12,$2,1,$3,$4,$5,$6,$7,$8,$9,$10,$11)`
	}
	// The INSERT path must satisfy its nonnegative CHECK even for a finishing
	// execution. Apply its gauge separately while preserving the same transaction.
	if finish {
		set[1] = "unresolved_queries=usage_rollups.unresolved_queries-1"
		guards[1] = "usage_rollups.unresolved_queries>=1"
	}
	return `INSERT INTO usage_rollups(source_id,tenant_id,project_id,day_start,revision,` + strings.Join(columns, ",") + `,unresolved_queries,last_observed_at) ` + selector + ` ON CONFLICT(source_id,tenant_id,project_id,day_start) DO UPDATE SET ` + strings.Join(set, ",") + ` WHERE ` + strings.Join(guards, " AND ")
}

func (r *Recorder) addTx(ctx context.Context, tx *sql.Tx, tenant, project string, at time.Time, delta Counters, unresolved int64) error {
	if delta == (Counters{}) && unresolved == 0 {
		return nil
	}
	if !delta.Valid() || unresolved < -1 || unresolved > 1 {
		return errors.New("invalid usage delta")
	}
	observedAt := ""
	if delta.Queries > 0 || delta.QueryMicros > 0 || unresolved != 0 {
		observedAt = Stamp(r.Clock())
	}
	args := make([]any, 0, 13)
	args = append(args, r.SourceID, Day(at), delta.Live, delta.Import, delta.Queries, delta.QueryMicros, delta.ExportBytes, delta.PipelineBytes, delta.Webhooks, max(int64(0), unresolved), observedAt, project)
	query := domainPosting
	if unresolved < 0 {
		query = domainFinish
	}
	if tenant != "" {
		args = append(args, tenant)
		query = scopedPosting
		if unresolved < 0 {
			query = scopedFinish
		}
	}
	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return errors.New("usage project unavailable or counter exhausted")
	}
	return nil
}

// StartExecution commits the admitted count before any backend call. A crash
// before its final observation leaves an unresolved record and a duration floor.
func (r *Recorder) StartExecution(ctx context.Context, tenant, project, operation string) (*Execution, error) {
	if operation != "sql" && operation != "catalog" && operation != "profile" {
		return nil, errors.New("invalid usage operation")
	}
	at := r.Clock().UTC()
	id := NewID()
	tx, err := r.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if !validID(tenant) || !validID(project) {
		return nil, errors.New("invalid usage execution scope")
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO usage_executions(id,tenant_id,project_id,day_start,operation,last_observed_at) VALUES($1,$2,$3,$4,$5,$6)`, id, tenant, project, Day(at), operation, Stamp(at))
	if err != nil {
		return nil, err
	}
	n, _ := result.RowsAffected()
	if n != 1 {
		return nil, errors.New("usage project unavailable")
	}
	if err = r.addTx(ctx, tx, tenant, project, at, Counters{Queries: 1}, 1); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	e := &Execution{recorder: r, id: id}
	r.mu.Lock()
	r.active[id] = e
	r.mu.Unlock()
	return e, nil
}

// Begin starts native wall time immediately before dispatch, independently of
// the durable admitted count. An admitted attempt can crash before this point.
func (e *Execution) Begin() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.started.IsZero() && !e.ended {
		e.started = time.Now()
	}
}

// Finish is cheap and must run immediately after the native method returns.
// Persistence belongs to the shared observation loop, never the permit lifetime.
func (e *Execution) Finish() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.ended {
		if !e.started.IsZero() {
			e.elapsed = max(int64(0), time.Since(e.started).Microseconds())
		}
		e.ended = true
	}
}
func (r *Recorder) Observe(ctx context.Context) error {
	r.observe.Lock()
	defer r.observe.Unlock()
	r.mu.Lock()
	active := make([]*Execution, 0, len(r.active))
	for _, e := range r.active {
		active = append(active, e)
	}
	r.mu.Unlock()
	for _, e := range active {
		if err := r.observeExecution(ctx, e); err != nil {
			return err
		}
	}
	return nil
}
func (r *Recorder) observeExecution(ctx context.Context, e *Execution) error {
	e.mu.Lock()
	elapsed := e.elapsed
	ended := e.ended
	if !ended && !e.started.IsZero() {
		elapsed = max(int64(0), time.Since(e.started).Microseconds())
	}
	e.mu.Unlock()
	tx, err := r.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var tenant, project, day string
	var previous, closed int64
	if err = tx.QueryRowContext(ctx, `SELECT tenant_id,project_id,day_start,elapsed_us,closed FROM usage_executions WHERE id=$1`, e.id).Scan(&tenant, &project, &day, &previous, &closed); errors.Is(err, sql.ErrNoRows) {
		r.mu.Lock()
		delete(r.active, e.id)
		r.mu.Unlock()
		return nil
	} else if err != nil {
		return err
	}
	if closed == 1 {
		r.mu.Lock()
		delete(r.active, e.id)
		r.mu.Unlock()
		return nil
	}
	if elapsed < previous {
		return ErrConflict
	}
	at, err := time.Parse(time.RFC3339, day)
	if err != nil {
		return err
	}
	unresolved := int64(0)
	if ended {
		unresolved = -1
	}
	if err = r.addTx(ctx, tx, tenant, project, at, Counters{QueryMicros: elapsed - previous}, unresolved); err != nil {
		return err
	}
	state := 0
	if ended {
		state = 1
	}
	if _, err = tx.ExecContext(ctx, `UPDATE usage_executions SET elapsed_us=$1,closed=$2,last_observed_at=$3 WHERE id=$4`, elapsed, state, Stamp(r.Clock()), e.id); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	if ended {
		r.mu.Lock()
		delete(r.active, e.id)
		r.mu.Unlock()
	}
	return nil
}
func (r *Recorder) Recover(ctx context.Context) error {
	_, err := r.DB.ExecContext(ctx, `UPDATE usage_executions SET lost=1 WHERE closed=0`)
	return err
}

func (r *Recorder) Pending(ctx context.Context) ([]SourceTotals, error) {
	rows, err := r.DB.QueryContext(ctx, `SELECT source_id,tenant_id,project_id,day_start,revision,`+strings.Join(columns, ",")+`,unresolved_queries,last_observed_at FROM usage_rollups WHERE revision>published_revision ORDER BY day_start,tenant_id,project_id LIMIT 1000`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []SourceTotals{}
	for rows.Next() {
		var v SourceTotals
		if err = rows.Scan(&v.SourceID, &v.TenantID, &v.ProjectID, &v.Day, &v.Revision, &v.Counters.Live, &v.Counters.Import, &v.Counters.Queries, &v.Counters.QueryMicros, &v.Counters.ExportBytes, &v.Counters.PipelineBytes, &v.Counters.Webhooks, &v.Unresolved, &v.LastObserved); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
func (r *Recorder) Acknowledge(ctx context.Context, totals []SourceTotals) error {
	tx, err := r.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, v := range totals {
		if _, err = tx.ExecContext(ctx, `UPDATE usage_rollups SET published_revision=CASE WHEN published_revision<$1 THEN $1 ELSE published_revision END WHERE source_id=$2 AND tenant_id=$3 AND project_id=$4 AND day_start=$5`, v.Revision, v.SourceID, v.TenantID, v.ProjectID, v.Day); err != nil {
			return err
		}
	}
	return tx.Commit()
}
func checkedUnresolved(a, b int64) (int64, error) {
	if b < 0 || a > math.MaxInt64-b {
		return 0, fmt.Errorf("usage query count overflow")
	}
	return a + b, nil
}
