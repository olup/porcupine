package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"time"

	"github.com/olup/porcupine/internal/usage"
)

type usageRuntime struct {
	recorder *usage.Recorder
	store    *usage.Store
	config   usage.Config
	cancel   context.CancelFunc
	done     chan struct{}
	flush    contextMutex
	clock    func() time.Time
	sampler  parquetUsageSampler
}

func usageMetadata(ctx context.Context, db *sql.DB, key, value string) (string, error) {
	if _, err := db.ExecContext(ctx, `INSERT INTO metadata(key,value) VALUES($1,$2) ON CONFLICT(key) DO NOTHING`, key, value); err != nil {
		return "", err
	}
	var current string
	err := db.QueryRowContext(ctx, `SELECT value FROM metadata WHERE key=$1`, key).Scan(&current)
	return current, err
}
func (s *Service) initializeUsage(ctx context.Context, c *usage.Config) error {
	config, err := usage.Normalize(c)
	if err != nil {
		return err
	}
	source, err := usageMetadata(ctx, s.db, "usage_source_id", usage.NewID())
	if err != nil {
		return err
	}
	if _, err = usageMetadata(ctx, s.control, "usage_epoch", usage.NewID()); err != nil {
		return err
	}
	if _, err = usageMetadata(ctx, s.control, "usage_available_since", usage.Stamp(s.clock())); err != nil {
		return err
	}
	clock := s.clock
	recorder := usage.NewRecorder(s.db, source, clock)
	if err = recorder.Recover(ctx); err != nil {
		return err
	}
	sampler, _ := s.lake.(parquetUsageSampler)
	s.usage = &usageRuntime{recorder: recorder, store: usage.NewStore(s.control, s.shared != nil, config, clock), config: config, clock: clock, sampler: sampler}
	s.eventRules.usage = recorder
	return nil
}
func (u *usageRuntime) synchronize(ctx context.Context) error {
	if err := u.flush.Lock(ctx); err != nil {
		return err
	}
	defer u.flush.Unlock()
	if err := u.recorder.Observe(ctx); err != nil {
		return err
	}
	totals, err := u.recorder.Pending(ctx)
	if err != nil {
		return err
	}
	if len(totals) == 0 {
		return nil
	}
	if err = u.store.PublishSourceTotals(ctx, totals); err != nil {
		return err
	}
	return u.recorder.Acknowledge(ctx, totals)
}
func (s *Service) synchronizeUsage(ctx context.Context) error {
	var err error
	if s.hostedQueries != nil {
		err = errors.Join(s.hostedQueries.observeReturns(ctx, s.control), s.hostedQueries.recorder.Observe(ctx))
	}
	if s.usage.recorder != nil {
		err = errors.Join(err, s.usage.synchronize(ctx))
	}
	return err
}

func (s *Service) startUsage() {
	u := s.usage
	if u == nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	u.cancel = cancel
	u.done = make(chan struct{})
	go func() {
		defer close(u.done)
		ticker := time.NewTicker(usage.ObservationInterval)
		defer ticker.Stop()
		nextSample := time.Time{}
		for {
			work, stop := context.WithTimeout(ctx, 30*time.Second)
			_ = s.synchronizeUsage(work)
			if !u.clock().Before(nextSample) {
				_ = s.sampleUsageStorage(work)
				_ = u.store.Prune(work)
				if u.recorder != nil {
					_ = u.recorder.Prune(work, u.config.RetentionDays)
				}
				if s.hostedQueries != nil {
					_ = s.hostedQueries.recorder.PruneExecutions(work, u.config.RetentionDays)
				}
				nextSample = u.clock().Add(usage.SampleInterval)
			}
			stop()
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}
func (s *Service) stopUsage() {
	u := s.usage
	if u == nil {
		return
	}
	if u.cancel != nil {
		u.cancel()
		<-u.done
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = s.synchronizeUsage(ctx)
}

// The durable start is committed before dispatch. Finish records elapsed native
// scope in memory before releasing its permit; the common observation worker
// persists cumulative time. A crash leaves the durable unresolved lower bound.
type usageQueryScope struct {
	execution *usage.Execution
	finish    func() error
	once      sync.Once
	cause     error
}

func (q *usageQueryScope) Begin() {
	if q.execution != nil {
		q.execution.Begin()
	}
}
func (q *usageQueryScope) Finish() error {
	q.once.Do(func() {
		if q.execution != nil {
			q.execution.Finish()
		}
		q.cause = q.finish()
	})
	return q.cause
}

func (s *Service) beginUsageQuery(ctx context.Context, project, operation string) (context.Context, InteractiveQueryConfig, *usageQueryScope, error) {
	native, limits, finish, err := s.beginInteractiveQuery(ctx, project)
	if err != nil {
		return nil, InteractiveQueryConfig{}, nil, err
	}
	if s.usage == nil {
		return native, limits, &usageQueryScope{finish: finish}, nil
	}
	// Analytical reads can execute on a non-owner with no local project anchor.
	// Freeze the authoritative control scope rather than manufacturing an anchor.
	tenant, err := s.projectTenant(native, project)
	if err != nil {
		if stopped := finish(); stopped != nil {
			err = interactiveQueryResultError(native, stopped, nil)
		}
		return nil, InteractiveQueryConfig{}, nil, err
	}
	recorder := s.usage.recorder
	if s.hostedQueries != nil {
		recorder = s.hostedQueries.recorder
	}
	execution, err := recorder.StartExecution(native, tenant.ID, project, operation)
	if err != nil {
		if stopped := finish(); stopped != nil {
			return nil, InteractiveQueryConfig{}, nil, interactiveQueryResultError(native, stopped, nil)
		}
		return nil, InteractiveQueryConfig{}, nil, apiError(503, "usage admission unavailable; query was not executed")
	}
	return native, limits, &usageQueryScope{execution: execution, finish: finish}, nil
}

type parquetUsageSampler interface {
	CurrentParquetBytes(context.Context, string) (int64, error)
}

func (s *Service) sampleUsageStorage(ctx context.Context) error {
	sampler := s.usage.sampler
	if sampler == nil || s.hostedQueries != nil {
		return nil
	}
	// Only locally anchored projects are sampled. A remote owner is never opened
	// through discovery or manufactured to fill a missing measurement.
	after := ""
	for {
		rows, err := s.db.QueryContext(ctx, `SELECT id,tenant_id FROM projects WHERE id>$1 ORDER BY id LIMIT 100`, after)
		if err != nil {
			return err
		}
		type project struct{ id, tenant string }
		batch := []project{}
		for rows.Next() {
			var p project
			if err = rows.Scan(&p.id, &p.tenant); err != nil {
				rows.Close()
				return err
			}
			batch = append(batch, p)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			return nil
		}
		for _, p := range batch {
			after = p.id
			if err = s.requireSharedProjectOwner(ctx, p.id); err != nil {
				continue
			}
			bytes, e := sampler.CurrentParquetBytes(ctx, p.id)
			if e != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				continue
			}
			at := s.usage.clock()
			if e = s.usage.store.RecordStorageSample(ctx, usage.StorageSample{TenantID: p.tenant, ProjectID: p.id, Day: usage.Day(at), ObservedAt: usage.Stamp(at), Bytes: bytes}); e != nil {
				return e
			}
		}
	}
}
