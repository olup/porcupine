package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"time"

	eventapi "github.com/olup/porcupine/internal/event"
)

// Worker incarnation leases bound the lifetime of activity tickets. The owner
// writes reclaim_after, so reclaimers need no knowledge of its fence timing.
// A NULL incarnation marks a ticket from an older build; it never expires.
const hostedIncarnationSchemaV1 = `CREATE TABLE hosted_worker_incarnations (
incarnation_id TEXT PRIMARY KEY,
started_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
expires_at TIMESTAMPTZ NOT NULL,
reclaim_after TIMESTAMPTZ NOT NULL,
CHECK (reclaim_after > expires_at));
ALTER TABLE hosted_project_activity ADD COLUMN incarnation_id TEXT
REFERENCES hosted_worker_incarnations(incarnation_id) ON DELETE CASCADE;
CREATE INDEX hosted_project_activity_incarnation ON hosted_project_activity(incarnation_id);`

var errHostedIncarnationLost = errors.New("hosted worker lease is not current")

// Grace lets requests already sent before cancellation drain.
type hostedIncarnationTiming struct {
	lease, renew, grace time.Duration
}

var defaultHostedIncarnationTiming = hostedIncarnationTiming{lease: 30 * time.Second, renew: 5 * time.Second, grace: 45 * time.Second}

const hostedWriteTimeout = 30 * time.Second

// Only the keeper changes the lease. Operations capture its cancellation
// context so replacing an expired lease cannot revive work from that lease.
type hostedIncarnation struct {
	control  *sql.DB
	timing   hostedIncarnationTiming
	mu       sync.Mutex
	id       string
	deadline time.Time
	lease    context.Context
	fence    context.CancelFunc
	stop     context.CancelFunc
	done     chan struct{}
}

func startHostedIncarnation(ctx context.Context, control *sql.DB, timing hostedIncarnationTiming) (*hostedIncarnation, error) {
	if control == nil || timing.renew <= 0 || timing.lease <= timing.renew || timing.grace <= 0 {
		return nil, errors.New("hosted worker lease requires control storage and a renewal shorter than its lease")
	}
	i := &hostedIncarnation{control: control, timing: timing}
	if err := i.grant(ctx); err != nil {
		return nil, err
	}
	keeper, stop := context.WithCancel(context.Background())
	i.stop, i.done = stop, make(chan struct{})
	go i.keep(keeper)
	return i, nil
}

func (i *hostedIncarnation) grant(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, hostedActivityControlTimeout)
	defer cancel()
	if err := reclaimExpiredHostedIncarnations(ctx, i.control); err != nil {
		return err
	}
	id := eventapi.NewID()
	// Start the local lease before sending the statement, conservatively.
	sent := time.Now()
	if _, err := i.control.ExecContext(ctx, `INSERT INTO hosted_worker_incarnations(incarnation_id,expires_at,reclaim_after)
VALUES($1,clock_timestamp()+$2::bigint*interval '1 millisecond',clock_timestamp()+$3::bigint*interval '1 millisecond')`,
		id, i.timing.lease.Milliseconds(), (i.timing.lease + i.timing.grace).Milliseconds()); err != nil {
		return err
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	i.id, i.deadline = id, sent.Add(i.timing.lease)
	i.lease, i.fence = context.WithCancel(context.Background())
	return nil
}

func (i *hostedIncarnation) keep(ctx context.Context) {
	defer close(i.done)
	defer func() { i.fence() }()
	for {
		wait := i.timing.renew
		if i.lease.Err() == nil {
			wait = min(wait, time.Until(i.deadline))
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		if i.lease.Err() != nil {
			_ = i.grant(ctx)
		} else if !time.Now().Before(i.deadline) || !i.renew(ctx, i.deadline) {
			i.fence()
		}
	}
}

// renew reports false only when PostgreSQL no longer holds a current lease.
// It returns by the local deadline, so a stalled statement cannot delay fencing.
func (i *hostedIncarnation) renew(ctx context.Context, deadline time.Time) bool {
	work, cancel := context.WithTimeout(ctx, min(time.Until(deadline), hostedActivityControlTimeout))
	defer cancel()
	sent := time.Now()
	result, err := i.control.ExecContext(work, `UPDATE hosted_worker_incarnations
SET expires_at=clock_timestamp()+$2::bigint*interval '1 millisecond',reclaim_after=clock_timestamp()+$3::bigint*interval '1 millisecond'
WHERE incarnation_id=$1 AND expires_at>clock_timestamp()`, i.id, i.timing.lease.Milliseconds(), (i.timing.lease + i.timing.grace).Milliseconds())
	if err != nil {
		return true
	}
	if n, err := result.RowsAffected(); err != nil || n != 1 {
		return err != nil
	}
	i.mu.Lock()
	i.deadline = sent.Add(i.timing.lease)
	i.mu.Unlock()
	return true
}

func (i *hostedIncarnation) currentLocked() bool {
	return i.lease.Err() == nil && time.Now().Before(i.deadline)
}

// Nil is used by fixtures that register unleased tickets.
func (i *hostedIncarnation) current() (string, error) {
	if i == nil {
		return "", nil
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if !i.currentLocked() {
		return "", errHostedIncarnationLost
	}
	return i.id, nil
}

func (i *hostedIncarnation) cancelOnLoss(cancel context.CancelFunc) func() {
	if i == nil {
		return cancel
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if !i.currentLocked() {
		cancel()
		return cancel
	}
	stop := context.AfterFunc(i.lease, cancel)
	return func() { stop(); cancel() }
}

func (i *hostedIncarnation) writeContext(parent context.Context) (context.Context, func()) {
	ctx, cancel := context.WithTimeout(parent, hostedWriteTimeout)
	return ctx, i.cancelOnLoss(cancel)
}

func (i *hostedIncarnation) beginOutputWork(parent context.Context, _ string) (context.Context, func(), bool) {
	ctx, cancel := context.WithCancel(parent)
	finish := i.cancelOnLoss(cancel)
	if ctx.Err() != nil {
		finish()
		return ctx, nil, false
	}
	return ctx, finish, true
}

func (i *hostedIncarnation) close() {
	i.stop()
	<-i.done
	ctx, cancel := context.WithTimeout(context.Background(), hostedActivityControlTimeout)
	defer cancel()
	_, _ = i.control.ExecContext(ctx, `DELETE FROM hosted_worker_incarnations
WHERE incarnation_id=$1 AND NOT EXISTS(SELECT 1 FROM hosted_project_activity WHERE incarnation_id=$1)`, i.id)
}

// Reclaim only after the lease and cancellation grace have ended. The row
// lock waits for any registration still using the incarnation, then cascades
// the deletion to its tickets.
func reclaimExpiredHostedIncarnations(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `DELETE FROM hosted_worker_incarnations WHERE reclaim_after<clock_timestamp()`)
	return err
}
