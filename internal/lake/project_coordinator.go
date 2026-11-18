package lake

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"sync"
	"time"

	"github.com/olup/porcupine/internal/event"
)

// ProjectCoordinatorSchemaV1 is appended by the operational control migrator.
// Epochs must be ordered across physical sessions, without sequence caching.
const ProjectCoordinatorSchemaV1 = `CREATE SEQUENCE shared_native_write_epoch AS BIGINT INCREMENT BY 1 MINVALUE 1 NO MAXVALUE START WITH 1 CACHE 1 NO CYCLE`

var ErrProjectWriteBusy = errors.New("project write coordinator is busy")
var ErrProjectWriteSessionLost = errors.New("project write session lost")
var ErrProjectWriteFenceStale = errors.New("project write fence is stale or belongs to another stream")
var ErrProjectWriteSealed = errors.New("project stream is sealed")

type ProjectWriteKey struct{ SourceID, ProjectID string }
type ProjectWriteFence struct {
	Key           ProjectWriteKey
	StreamID      string
	Epoch         int64
	AcquisitionID string
}
type ProjectCoordinatorConfig struct {
	MaxConcurrent               int
	AcquireTimeout              time.Duration
	ProbeInterval, ProbeTimeout time.Duration
}

func DefaultProjectCoordinatorConfig() ProjectCoordinatorConfig {
	return ProjectCoordinatorConfig{MaxConcurrent: 4, AcquireTimeout: 5 * time.Second, ProbeInterval: time.Second, ProbeTimeout: 2 * time.Second}
}

// PostgresProjectCoordinator requires a direct PostgreSQL endpoint or session
// pooling. Transaction pooling is unsupported: sql.Conn alone does not pin a
// physical backend behind a transaction pooler. PID and lock checks detect a
// switched backend, but cannot infer a proxy's pooling policy from its URL.
type PostgresProjectCoordinator struct {
	control *sql.DB
	config  ProjectCoordinatorConfig
	slots   chan struct{}
}

func NewPostgresProjectCoordinator(control *sql.DB, c ProjectCoordinatorConfig) (*PostgresProjectCoordinator, error) {
	if control == nil || c.MaxConcurrent <= 0 || c.AcquireTimeout <= 0 || c.ProbeInterval <= 0 || c.ProbeTimeout <= 0 {
		return nil, errors.New("project coordinator requires control storage and positive connection, acquisition and probe limits")
	}
	if max := control.Stats().MaxOpenConnections; max > 0 && c.MaxConcurrent >= max {
		return nil, errors.New("project coordinator must leave control connections available for other operations")
	}
	return &PostgresProjectCoordinator{control: control, config: c, slots: make(chan struct{}, c.MaxConcurrent)}, nil
}

type ProjectWriteSession struct {
	owner        *PostgresProjectCoordinator
	conn         *sql.Conn
	key          ProjectWriteKey
	lock         int64
	pid          int
	epoch        int64
	acquisition  string
	ctx          context.Context
	cancel       context.CancelCauseFunc
	done         chan struct{}
	stopProbe    chan struct{}
	operations   sync.Mutex
	callsMu      sync.Mutex
	nativeCalls  sync.WaitGroup
	closing      bool
	release      sync.Once
	releaseError error
}

func projectAdvisoryKey(key ProjectWriteKey) int64 {
	digest := sha256.Sum256([]byte("porcupine-project-write-v1\x00" + key.SourceID + "\x00" + key.ProjectID))
	return int64(binary.BigEndian.Uint64(digest[:8]))
}
func (c *PostgresProjectCoordinator) Acquire(ctx context.Context, key ProjectWriteKey) (*ProjectWriteSession, error) {
	return c.acquire(ctx, key, false)
}

// TryAcquire lets background work yield to an occupied project or local slot.
func (c *PostgresProjectCoordinator) TryAcquire(ctx context.Context, key ProjectWriteKey) (*ProjectWriteSession, error) {
	return c.acquire(ctx, key, true)
}

func (c *PostgresProjectCoordinator) acquire(parent context.Context, key ProjectWriteKey, try bool) (*ProjectWriteSession, error) {
	if key.SourceID == "" || key.ProjectID == "" {
		return nil, errors.New("project write scope is required")
	}
	if err := parent.Err(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(parent, c.config.AcquireTimeout)
	defer cancel()
	acquireError := func(fallback error) error {
		if err := parent.Err(); err != nil {
			return err
		}
		if ctx.Err() != nil {
			return ErrProjectWriteBusy
		}
		return fallback
	}
	if try {
		select {
		case c.slots <- struct{}{}:
		default:
			return nil, ErrProjectWriteBusy
		}
	} else {
		select {
		case c.slots <- struct{}{}:
		case <-ctx.Done():
			return nil, acquireError(ErrProjectWriteBusy)
		}
	}
	conn, err := c.control.Conn(ctx)
	if err != nil {
		<-c.slots
		return nil, acquireError(errors.New("project coordinator control connection unavailable"))
	}
	fail := func() (*ProjectWriteSession, error) {
		// A canceled acquisition can race with a granted session lock. Close
		// the physical backend rather than returning a possible owner to the pool.
		discardProjectConnection(conn)
		<-c.slots
		return nil, acquireError(ErrProjectWriteSessionLost)
	}
	session := &ProjectWriteSession{owner: c, conn: conn, key: key, lock: projectAdvisoryKey(key), done: make(chan struct{}), stopProbe: make(chan struct{}), acquisition: event.NewID()}
	// Capture PID in the acquisition statement, before any second statement can
	// be routed elsewhere by an unsupported transaction-pooling proxy.
	if try {
		var acquired bool
		if err := conn.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1),pg_backend_pid()", session.lock).Scan(&acquired, &session.pid); err != nil {
			return fail()
		}
		if !acquired {
			_ = conn.Close()
			<-c.slots
			return nil, ErrProjectWriteBusy
		}
	} else {
		var ignored any
		if err := conn.QueryRowContext(ctx, "SELECT pg_advisory_lock($1),pg_backend_pid()", session.lock).Scan(&ignored, &session.pid); err != nil {
			return fail()
		}
	}
	if err := session.checkPhysicalSession(ctx); err != nil {
		return fail()
	}
	var increment, cache int64
	var cycle bool
	if err := conn.QueryRowContext(ctx, `SELECT increment_by,cache_size,cycle FROM pg_sequences WHERE schemaname=current_schema() AND sequencename='shared_native_write_epoch'`).Scan(&increment, &cache, &cycle); err != nil || increment != 1 || cache != 1 || cycle {
		return fail()
	}
	// The sequence is read on the same physical session that owns the lock.
	if err := conn.QueryRowContext(ctx, `SELECT CASE WHEN pg_backend_pid()=$1 THEN nextval('shared_native_write_epoch') END`, session.pid).Scan(&session.epoch); err != nil || session.epoch <= 0 {
		return fail()
	}
	if ctx.Err() != nil {
		return fail()
	}
	session.ctx, session.cancel = context.WithCancelCause(parent)
	go session.probe()
	return session, nil
}
func (s *ProjectWriteSession) Context() context.Context { return s.ctx }
func (s *ProjectWriteSession) Fence(streamID string) ProjectWriteFence {
	return ProjectWriteFence{Key: s.key, StreamID: streamID, Epoch: s.epoch, AcquisitionID: s.acquisition}
}

// RunNative joins the actual native callback before Release unlocks the physical
// control session. Cancellation or lease expiry alone never counts as a return.
// Keep the entire native operation, including cold open and retries, inside it.
func (s *ProjectWriteSession) RunNative(ctx context.Context, call func(context.Context) error) error {
	if call == nil {
		return ErrProjectWriteFenceStale
	}
	s.callsMu.Lock()
	if s.closing || s.ctx.Err() != nil || ctx.Err() != nil {
		s.callsMu.Unlock()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return ErrProjectWriteSessionLost
	}
	s.nativeCalls.Add(1)
	s.callsMu.Unlock()
	defer s.nativeCalls.Done()
	nativeCtx, cancel := context.WithCancelCause(ctx)
	stop := context.AfterFunc(s.ctx, func() { cancel(ErrProjectWriteSessionLost) })
	defer func() { stop(); cancel(context.Canceled) }()
	s.operations.Lock()
	bounded, finish := context.WithTimeout(nativeCtx, s.owner.config.ProbeTimeout)
	err := s.checkPhysicalSession(bounded)
	finish()
	s.operations.Unlock()
	if err != nil {
		s.cancel(ErrProjectWriteSessionLost)
		return err
	}
	return call(nativeCtx)
}

func (s *ProjectWriteSession) checkPhysicalSession(ctx context.Context) error {
	var owned bool
	high, low := int64(uint64(s.lock)>>32), int64(uint64(s.lock)&0xffffffff)
	err := s.conn.QueryRowContext(ctx, `SELECT pg_backend_pid()=$1 AND EXISTS(
 SELECT 1 FROM pg_locks WHERE locktype='advisory' AND pid=pg_backend_pid() AND granted
 AND database=(SELECT oid FROM pg_database WHERE datname=current_database())
 AND classid::bigint=$2 AND objid::bigint=$3 AND objsubid=1)`, s.pid, high, low).Scan(&owned)
	if err != nil || !owned {
		return ErrProjectWriteSessionLost
	}
	return nil
}
func (s *ProjectWriteSession) probe() {
	defer close(s.done)
	timer := time.NewTicker(s.owner.config.ProbeInterval)
	defer timer.Stop()
	for {
		select {
		case <-s.stopProbe:
			return
		case <-s.ctx.Done():
			return
		case <-timer.C:
			s.operations.Lock()
			select {
			case <-s.stopProbe:
				s.operations.Unlock()
				return
			default:
			}
			ctx, cancel := context.WithTimeout(s.ctx, s.owner.config.ProbeTimeout)
			err := s.checkPhysicalSession(ctx)
			cancel()
			s.operations.Unlock()
			if err != nil {
				s.cancel(ErrProjectWriteSessionLost)
				return
			}
		}
	}
}

func (s *ProjectWriteSession) Release() error {
	s.release.Do(func() {
		// Cancelling an active pgx query can discard its physical connection.
		// Stop scheduling probes and join the bounded current probe before
		// cancelling operation contexts, so a healthy lock can still be released.
		s.callsMu.Lock()
		s.closing = true
		close(s.stopProbe)
		s.callsMu.Unlock()
		<-s.done
		s.nativeCalls.Wait()
		s.operations.Lock()
		defer s.operations.Unlock()
		s.cancel(ErrProjectWriteSessionLost)
		cleanup, cancel := context.WithTimeout(context.Background(), s.owner.config.ProbeTimeout)
		defer cancel()
		var unlocked bool
		if s.checkPhysicalSession(cleanup) != nil || s.conn.QueryRowContext(cleanup, "SELECT pg_advisory_unlock($1)", s.lock).Scan(&unlocked) != nil || !unlocked {
			s.releaseError = ErrProjectWriteSessionLost
			discardProjectConnection(s.conn)
		} else {
			s.releaseError = s.conn.Close()
		}
		<-s.owner.slots
	})
	return s.releaseError
}
func discardProjectConnection(conn *sql.Conn) {
	_ = conn.Raw(func(any) error { return driver.ErrBadConn })
	_ = conn.Close()
}
