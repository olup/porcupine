package query

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"maps"
	"slices"
	"sort"
	"sync"

	"time"

	"github.com/olup/porcupine/internal/apierr"
)

type QueryExecutionFailure struct {
	ProjectID string `json:"project_id"`
	Reason    string `json:"reason" enum:"native_query_unresponsive,query_permit_uncertain"`
}

type QueryExecutionStatus struct {
	Active            int                     `json:"active" doc:"Interactive scopes that have not returned, including canceled native calls. Their worker slots remain occupied."`
	UncertainPermits  int                     `json:"uncertain_permits" doc:"Active scopes whose latest PostgreSQL renewal failed or whose permit was lost. An expired permit may already have been reassigned."`
	Draining          bool                    `json:"draining" doc:"New shared analytical work is refused while this worker shuts down."`
	UnhealthyProjects []QueryExecutionFailure `json:"unhealthy_projects" doc:"Worker-local query quarantine, retained until worker restart. These projects receive no new interactive SQL, catalog, or profile executions on this worker."`
}

type sharedExecution struct {
	project   string
	cancel    context.CancelFunc
	timer     *time.Timer
	uncertain bool
	stopErr   error
}

var errPermitUncertain = errors.New("shared query permit is uncertain")

// The keeper belongs to actual execution scopes, not HTTP request contexts.
// Expiry recovers crashed workers but cannot fence an uninterruptible native
// call on a live worker that lost PostgreSQL connectivity.
type keeper struct {
	mu        sync.Mutex
	active    map[string]*sharedExecution
	unhealthy map[string]string
	draining  bool
	running   bool
	wake      chan struct{}
	logging   bool
}

func (m *keeper) allowLocked(project string) error {
	if m.draining {
		return apierr.Detailed(503, "analytics_worker_draining", "analytics worker is shutting down", "Retry on another available worker.", "")
	}
	if m.unhealthy[project] != "" {
		return apierr.Detailed(503, "analytics_engine_unhealthy", "project analytics engine is unhealthy on this worker", "The operator must restart the worker. Other healthy projects can continue while capacity remains.", "")
	}
	return nil
}

func (m *keeper) allow(project string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.allowLocked(project)
}

func (m *keeper) start(p *permit, cancel context.CancelFunc, unhealthyAt time.Time, control *sql.DB, worker string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.allowLocked(p.project); err != nil {
		return err
	}
	if m.active == nil {
		m.active = make(map[string]*sharedExecution)
		m.unhealthy = make(map[string]string)
		m.wake = make(chan struct{}, 1)
	}
	e := &sharedExecution{project: p.project, cancel: cancel}
	m.active[p.id] = e
	e.timer = time.AfterFunc(max(0, time.Until(unhealthyAt)), func() {
		m.quarantine(p.id, "native_query_unresponsive", false)
	})
	if !m.running {
		m.running = true
		go m.keepAlive(control, worker)
	}
	return nil
}

func (m *keeper) returned(ctx context.Context, p *permit, hosted *hostedQueries) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if hosted != nil {
		hosted.noteReturn(p)
	}
	stopped := ctx.Err()
	if e := m.active[p.id]; e != nil {
		if errors.Is(e.stopErr, errPermitUncertain) || stopped == nil {
			stopped = e.stopErr
		}
		delete(m.active, p.id)
		e.timer.Stop()
		select {
		case m.wake <- struct{}{}:
		default:
		}
	}
	return stopped
}

func (m *keeper) keepAlive(control *sql.DB, worker string) {
	ticker := time.NewTicker(renewInterval)
	defer ticker.Stop()
	for {
		m.mu.Lock()
		if len(m.active) == 0 {
			m.running = false
			m.mu.Unlock()
			return
		}
		m.mu.Unlock()
		select {
		case <-m.wake:
		case <-ticker.C:
			m.renew(control, worker)
		}
	}
}

func (m *keeper) renew(control *sql.DB, worker string) {
	m.mu.Lock()
	ids := slices.Collect(maps.Keys(m.active))
	m.mu.Unlock()
	if len(ids) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), controlTimeout)
	defer cancel()
	rows, err := control.QueryContext(ctx, `UPDATE shared_query_permits SET expires_at=clock_timestamp()+$2::bigint*interval '1 millisecond'
WHERE worker_id=$1 AND expires_at>clock_timestamp() AND permit_id=ANY($3::text[]) RETURNING permit_id`, worker, permitLifetime.Milliseconds(), ids)
	renewed := make(map[string]bool, len(ids))
	if err == nil {
		for rows.Next() {
			var id string
			if err = rows.Scan(&id); err != nil {
				break
			}
			renewed[id] = true
		}
		err = errors.Join(err, rows.Err(), rows.Close())
	}
	for _, id := range ids {
		if err != nil || !renewed[id] {
			m.quarantine(id, "query_permit_uncertain", true)
		} else {
			m.mu.Lock()
			if e := m.active[id]; e != nil {
				e.uncertain = false
			}
			m.mu.Unlock()
		}
	}
}

func (m *keeper) quarantine(id, reason string, uncertain bool) {
	m.mu.Lock()
	e := m.active[id]
	if e == nil {
		m.mu.Unlock()
		return
	}
	if uncertain {
		e.uncertain = true
	}
	firstFailure := m.unhealthy[e.project] == ""
	if firstFailure {
		m.unhealthy[e.project] = reason
	}
	var cancels []context.CancelFunc
	for _, scope := range m.active {
		if scope.project == e.project {
			if uncertain {
				scope.stopErr = errPermitUncertain
			} else if scope.stopErr == nil {
				scope.stopErr = context.Canceled
			}
			cancels = append(cancels, scope.cancel)
		}
	}
	log := firstFailure && !m.logging
	if log {
		m.logging = true
	}
	project := e.project
	m.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
	if log {
		// At most one blocked logger per manager; state and renewal never wait on it.
		go func() {
			slog.Error("shared analytics execution is unhealthy; worker remains running", "project_id", project, "reason", reason)
			m.mu.Lock()
			m.logging = false
			m.mu.Unlock()
		}()
	}
}

func (m *keeper) status() QueryExecutionStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	status := QueryExecutionStatus{Active: len(m.active), Draining: m.draining, UnhealthyProjects: []QueryExecutionFailure{}}
	for _, e := range m.active {
		if e.uncertain {
			status.UncertainPermits++
		}
	}
	for project, reason := range m.unhealthy {
		status.UnhealthyProjects = append(status.UnhealthyProjects, QueryExecutionFailure{ProjectID: project, Reason: reason})
	}
	sort.Slice(status.UnhealthyProjects, func(i, j int) bool {
		return status.UnhealthyProjects[i].ProjectID < status.UnhealthyProjects[j].ProjectID
	})
	return status
}

func (s QueryExecutionStatus) Unavailable() bool {
	return s.Draining || s.UncertainPermits != 0 || len(s.UnhealthyProjects) != 0
}

func (r *Runtime) Status() QueryExecutionStatus {
	return r.keeper.status()
}

// Drain cancels the shared executions while retaining their keeper and permits.
func (r *Runtime) Drain() {
	if r.worker == "" {
		return
	}
	m := &r.keeper
	m.mu.Lock()
	m.draining = true
	cancels := make([]context.CancelFunc, 0, len(m.active))
	for _, e := range m.active {
		if e.stopErr == nil {
			e.stopErr = context.Canceled
		}
		cancels = append(cancels, e.cancel)
	}
	m.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
}
