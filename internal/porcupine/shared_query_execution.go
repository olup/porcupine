package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"
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

type sharedQueryExecution struct {
	permit    *sharedQueryPermit
	project   string
	cancel    context.CancelFunc
	timer     *time.Timer
	uncertain bool
}

// The keeper belongs to actual execution scopes, not HTTP request contexts.
// Expiry recovers crashed workers but cannot fence an uninterruptible native
// call on a live worker that lost PostgreSQL connectivity.
type sharedQueryExecutions struct {
	mu        sync.Mutex
	active    map[string]*sharedQueryExecution
	unhealthy map[string]string
	draining  bool
	running   bool
	wake      chan struct{}
	logging   bool
}

func (m *sharedQueryExecutions) allowLocked(project string) error {
	if m.draining {
		return analyticsError(503, "analytics_worker_draining", "analytics worker is shutting down", "Retry on another available worker.", "")
	}
	if m.unhealthy[project] != "" {
		return analyticsError(503, "analytics_engine_unhealthy", "project analytics engine is unhealthy on this worker", "The operator must restart the worker. Other healthy projects can continue while capacity remains.", "")
	}
	return nil
}

func (m *sharedQueryExecutions) allow(project string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.allowLocked(project)
}

func (m *sharedQueryExecutions) start(permit *sharedQueryPermit, project string, cancel context.CancelFunc, unhealthyAt time.Time) (func(), error) {
	m.mu.Lock()
	if err := m.allowLocked(project); err != nil {
		m.mu.Unlock()
		return nil, err
	}
	if m.active == nil {
		m.active = make(map[string]*sharedQueryExecution)
		m.unhealthy = make(map[string]string)
		m.wake = make(chan struct{}, 1)
	}
	e := &sharedQueryExecution{permit: permit, project: project, cancel: cancel}
	m.active[permit.id] = e
	e.timer = time.AfterFunc(max(0, time.Until(unhealthyAt)), func() {
		m.quarantine(permit.id, "native_query_unresponsive", false)
	})
	if !m.running {
		m.running = true
		go m.keepAlive(permit.control)
	}
	m.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			m.mu.Lock()
			delete(m.active, permit.id)
			e.timer.Stop()
			m.mu.Unlock()
			cancel()
			select {
			case m.wake <- struct{}{}:
			default:
			}
			permit.release()
		})
	}, nil
}

func (m *sharedQueryExecutions) keepAlive(control *sql.DB) {
	ticker := time.NewTicker(sharedQueryRenewInterval)
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
			m.renew(control)
		}
	}
}

func (m *sharedQueryExecutions) renew(control *sql.DB) {
	m.mu.Lock()
	permits := make([]*sharedQueryPermit, 0, len(m.active))
	for _, e := range m.active {
		permits = append(permits, e.permit)
	}
	m.mu.Unlock()
	if len(permits) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), sharedQueryControlTimeout)
	defer cancel()
	args := []any{permits[0].worker, sharedQueryPermitLifetime.Milliseconds()}
	markers := make([]string, len(permits))
	for i, permit := range permits {
		markers[i] = fmt.Sprintf("$%d", len(args)+1)
		args = append(args, permit.id)
	}
	rows, err := control.QueryContext(ctx, `UPDATE shared_query_permits SET expires_at=clock_timestamp()+$2::bigint*interval '1 millisecond'
WHERE worker_id=$1 AND expires_at>clock_timestamp() AND permit_id IN (`+strings.Join(markers, ",")+`) RETURNING permit_id`, args...)
	renewed := make(map[string]bool, len(permits))
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
	for _, permit := range permits {
		if err != nil || !renewed[permit.id] {
			m.quarantine(permit.id, "query_permit_uncertain", true)
		} else {
			m.mu.Lock()
			if e := m.active[permit.id]; e != nil {
				e.uncertain = false
			}
			m.mu.Unlock()
		}
	}
}

func (m *sharedQueryExecutions) quarantine(id, reason string, uncertain bool) {
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

func (m *sharedQueryExecutions) status() QueryExecutionStatus {
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

func (s QueryExecutionStatus) unavailable() bool {
	return s.Draining || s.UncertainPermits != 0 || len(s.UnhealthyProjects) != 0
}

// BeginQueryDrain cancels shared analytical work while retaining its permit
// keeper until the actual call returns. Local execution behavior is unchanged.
func (s *Service) BeginQueryDrain() {
	if s.shared == nil {
		return
	}
	s.sharedQueries.mu.Lock()
	s.sharedQueries.draining = true
	cancels := make([]context.CancelFunc, 0, len(s.sharedQueries.active))
	for _, e := range s.sharedQueries.active {
		cancels = append(cancels, e.cancel)
	}
	s.sharedQueries.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
}
