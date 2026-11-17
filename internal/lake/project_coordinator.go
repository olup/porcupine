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
	ProbeInterval, ProbeTimeout time.Duration
}

func DefaultProjectCoordinatorConfig() ProjectCoordinatorConfig {
	return ProjectCoordinatorConfig{MaxConcurrent: 4, ProbeInterval: time.Second, ProbeTimeout: 2 * time.Second}
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
	if control == nil || c.MaxConcurrent <= 0 || c.ProbeInterval <= 0 || c.ProbeTimeout <= 0 {
		return nil, errors.New("project coordinator requires control storage and positive connection/probe limits")
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
	release      sync.Once
	releaseError error
}

func projectAdvisoryKey(key ProjectWriteKey) int64 {
	digest := sha256.Sum256([]byte("porcupine-project-write-v1\x00" + key.SourceID + "\x00" + key.ProjectID))
	return int64(binary.BigEndian.Uint64(digest[:8]))
}
func (c *PostgresProjectCoordinator) Acquire(ctx context.Context, key ProjectWriteKey) (*ProjectWriteSession, error) {
	if key.SourceID == "" || key.ProjectID == "" {
		return nil, errors.New("project write scope is required")
	}
	select {
	case c.slots <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	conn, err := c.control.Conn(ctx)
	if err != nil {
		<-c.slots
		return nil, errors.New("project coordinator control connection unavailable")
	}
	fail := func() (*ProjectWriteSession, error) {
		discardProjectConnection(conn)
		<-c.slots
		return nil, ErrProjectWriteSessionLost
	}
	session := &ProjectWriteSession{owner: c, conn: conn, key: key, lock: projectAdvisoryKey(key), done: make(chan struct{}), stopProbe: make(chan struct{}), acquisition: event.NewID()}
	// Capture PID in the acquisition statement, before any second statement can
	// be routed elsewhere by an unsupported transaction-pooling proxy.
	var ignored any
	if err := conn.QueryRowContext(ctx, "SELECT pg_advisory_lock($1),pg_backend_pid()", session.lock).Scan(&ignored, &session.pid); err != nil {
		return fail()
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
	session.ctx, session.cancel = context.WithCancelCause(ctx)
	go session.probe()
	return session, nil
}
func (s *ProjectWriteSession) Context() context.Context { return s.ctx }
func (s *ProjectWriteSession) Fence(streamID string) ProjectWriteFence {
	return ProjectWriteFence{Key: s.key, StreamID: streamID, Epoch: s.epoch, AcquisitionID: s.acquisition}
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
		close(s.stopProbe)
		<-s.done
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
