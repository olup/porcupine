package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"os"
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

// hostedIncarnationExitCode asks the supervisor to replace the worker.
const hostedIncarnationExitCode = 75

// The grace gives requests already sent when the worker stops time to drain.
type hostedIncarnationTiming struct {
	lease, renew, grace time.Duration
}

var defaultHostedIncarnationTiming = hostedIncarnationTiming{lease: 30 * time.Second, renew: 5 * time.Second, grace: 45 * time.Second}

type hostedActivityKey struct{ project, ticket string }

// hostedIncarnation owns one renewable PostgreSQL lease per process lifetime.
// The local deadline is measured from before each successful grant or
// renewal statement, so it never passes after the PostgreSQL expiry. At that
// deadline the process stops if any write-capable activity has not returned.
// A nil incarnation registers unleased tickets for unit fixtures.
type hostedIncarnation struct {
	control  *sql.DB
	timing   hostedIncarnationTiming
	failStop func()
	mu       sync.Mutex
	id       string
	deadline time.Time
	lost     bool
	held     map[hostedActivityKey]struct{}
	stop     context.CancelFunc
	done     chan struct{}
}

func exitOnHostedLeaseLoss() { os.Exit(hostedIncarnationExitCode) }

func startHostedIncarnation(ctx context.Context, control *sql.DB, timing hostedIncarnationTiming, failStop func()) (*hostedIncarnation, error) {
	if control == nil || failStop == nil || timing.renew <= 0 || timing.lease <= timing.renew || timing.grace <= 0 {
		return nil, errors.New("hosted worker lease requires control storage, a fail-stop action and a renewal shorter than its lease")
	}
	i := &hostedIncarnation{control: control, timing: timing, failStop: failStop, held: make(map[hostedActivityKey]struct{})}
	// Remove dead incarnations left by earlier processes before joining.
	if err := reclaimExpiredHostedIncarnations(ctx, control); err != nil {
		return nil, err
	}
	if err := i.grant(ctx); err != nil {
		return nil, err
	}
	keeper, stop := context.WithCancel(context.Background())
	i.stop, i.done = stop, make(chan struct{})
	go i.keep(keeper)
	return i, nil
}

func (i *hostedIncarnation) grant(ctx context.Context) error {
	id := eventapi.NewID()
	sent := time.Now()
	if _, err := i.control.ExecContext(ctx, `INSERT INTO hosted_worker_incarnations(incarnation_id,expires_at,reclaim_after)
VALUES($1,clock_timestamp()+$2::bigint*interval '1 millisecond',clock_timestamp()+$3::bigint*interval '1 millisecond')`,
		id, i.timing.lease.Milliseconds(), (i.timing.lease + i.timing.grace).Milliseconds()); err != nil {
		return err
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	i.id, i.deadline, i.lost = id, sent.Add(i.timing.lease), false
	return nil
}

func (i *hostedIncarnation) keep(ctx context.Context) {
	defer close(i.done)
	for {
		i.mu.Lock()
		lost, deadline := i.lost, i.deadline
		i.mu.Unlock()
		wait := i.timing.renew
		if !lost {
			wait = min(wait, time.Until(deadline))
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		switch {
		case lost:
			// Only reached with nothing outstanding; rejoin under a new identity.
			work, cancel := context.WithTimeout(ctx, sharedQueryControlTimeout)
			_ = i.grant(work)
			cancel()
		case !time.Now().Before(deadline):
			if !i.fence() {
				return
			}
		default:
			if !i.renew(ctx, deadline) && !i.fence() {
				return
			}
		}
	}
}

// renew reports false only when PostgreSQL no longer holds a current lease.
// It returns by the local deadline, so a stalled statement cannot delay fencing.
func (i *hostedIncarnation) renew(ctx context.Context, deadline time.Time) bool {
	i.mu.Lock()
	id := i.id
	i.mu.Unlock()
	work, cancel := context.WithTimeout(ctx, min(time.Until(deadline), sharedQueryControlTimeout))
	defer cancel()
	sent := time.Now()
	result, err := i.control.ExecContext(work, `UPDATE hosted_worker_incarnations
SET expires_at=clock_timestamp()+$2::bigint*interval '1 millisecond',reclaim_after=clock_timestamp()+$3::bigint*interval '1 millisecond'
WHERE incarnation_id=$1 AND expires_at>clock_timestamp()`, id, i.timing.lease.Milliseconds(), (i.timing.lease + i.timing.grace).Milliseconds())
	if err != nil {
		return true
	}
	if n, err := result.RowsAffected(); err != nil || n != 1 {
		return err != nil
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.id == id && !i.lost {
		i.deadline = sent.Add(i.timing.lease)
	}
	return true
}

// fence ends this incarnation. It reports whether the keeper may continue.
func (i *hostedIncarnation) fence() bool {
	i.mu.Lock()
	i.lost = true
	outstanding := len(i.held)
	i.mu.Unlock()
	if outstanding == 0 {
		return true
	}
	// A returned call no longer counts, even with an unacknowledged ticket.
	// Anything still running could otherwise write after reclamation.
	i.failStop()
	return false
}

// reserve counts a write-capable ticket before its registration commits. The
// fence takes the same lock so expiry cannot miss an activity being registered.
func (i *hostedIncarnation) reserve(project, ticket string, mayWrite bool) (string, error) {
	if i == nil {
		return "", nil
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.lost || !time.Now().Before(i.deadline) {
		return "", errHostedIncarnationLost
	}
	if mayWrite {
		i.held[hostedActivityKey{project, ticket}] = struct{}{}
	}
	return i.id, nil
}

// release records actual return. It does not acknowledge the ticket.
func (i *hostedIncarnation) release(project, ticket string) {
	if i == nil {
		return
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	delete(i.held, hostedActivityKey{project, ticket})
}

// close runs after the native store and every executor have stopped. A
// ticket still held then belongs to a call that cannot run, so reclamation
// after the lease is safe; only an empty incarnation is removed at once.
func (i *hostedIncarnation) close() {
	i.stop()
	<-i.done
	i.mu.Lock()
	id, outstanding := i.id, len(i.held)
	i.mu.Unlock()
	if outstanding != 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), sharedQueryControlTimeout)
	defer cancel()
	_, _ = i.control.ExecContext(ctx, `DELETE FROM hosted_worker_incarnations
WHERE incarnation_id=$1 AND NOT EXISTS(SELECT 1 FROM hosted_project_activity WHERE incarnation_id=$1)`, id)
}

// reclaimExpiredHostedIncarnations removes incarnations whose owner has
// stopped by its own promise, cascading to their tickets. Its row lock
// waits for a registration still referencing the incarnation, then removes
// that ticket too.
func reclaimExpiredHostedIncarnations(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `DELETE FROM hosted_worker_incarnations WHERE reclaim_after<clock_timestamp()`)
	return err
}
