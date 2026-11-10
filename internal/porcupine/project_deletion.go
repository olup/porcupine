package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"sync"
	"time"
)

// Requests and workers for a project register until all their external writes
// and local acknowledgments finish. A deletion cancels just that project's
// operations and drains them before it checkpoints the cleanup inventory.
type projectActivity struct {
	mu       sync.Mutex
	projects map[string]*activeProject
}

type activeProject struct {
	next     uint64
	work     map[uint64]context.CancelFunc
	draining chan struct{}
}

func (a *projectActivity) begin(ctx context.Context, id string) (context.Context, func(), bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.projects == nil {
		a.projects = make(map[string]*activeProject)
	}
	p := a.projects[id]
	if p == nil {
		p = &activeProject{work: make(map[uint64]context.CancelFunc)}
		a.projects[id] = p
	}
	if p.draining != nil {
		return ctx, nil, false
	}
	child, cancel := context.WithCancel(ctx)
	p.next++
	lease := p.next
	p.work[lease] = cancel
	return child, func() {
		cancel()
		a.mu.Lock()
		defer a.mu.Unlock()
		delete(p.work, lease)
		if len(p.work) == 0 {
			if p.draining != nil {
				close(p.draining)
			} else {
				delete(a.projects, id)
			}
		}
	}, true
}

// Only the serialized deletion path can drain or release a project.
func (a *projectActivity) drain(ctx context.Context, id string) (func(), error) {
	a.mu.Lock()
	if a.projects == nil {
		a.projects = make(map[string]*activeProject)
	}
	p := a.projects[id]
	if p == nil {
		p = &activeProject{}
		a.projects[id] = p
	}
	p.draining = make(chan struct{})
	done := p.draining
	for _, cancel := range p.work {
		cancel()
	}
	if len(p.work) == 0 {
		close(done)
	}
	a.mu.Unlock()
	release := func() {
		a.mu.Lock()
		defer a.mu.Unlock()
		p.draining = nil
		if len(p.work) == 0 {
			delete(a.projects, id)
		}
	}
	select {
	case <-done:
		return release, nil
	case <-ctx.Done():
		release()
		return nil, ctx.Err()
	}
}

func (s *Service) stageProjectDeletion(ctx context.Context, id string) error {
	release, err := s.activity.drain(ctx, id)
	if err != nil {
		return err
	}
	defer release()
	// Project grants span multiple projects, and delivery batches can do so
	// too. Drain those short operations before deleting their SQLite inputs.
	s.catalogMu.Lock()
	defer s.catalogMu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var one int
	if err = tx.QueryRowContext(ctx, `SELECT 1 FROM projects WHERE id=?`, id).Scan(&one); errors.Is(err, sql.ErrNoRows) {
		return apiError(404, "project not found")
	} else if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO project_deletions(project_id,completed) VALUES(?,0)`, id); err != nil {
		return err
	}
	if err = s.stageProjectExportCleanup(ctx, tx, id); err != nil {
		return err
	}
	if err = purgeProjectSQLite(ctx, tx, id); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Service) deleteProject(ctx context.Context, id string) error {
	if err := s.deletionMu.Lock(ctx); err != nil {
		return apiError(503, "project deletion interrupted; retry DELETE")
	}
	defer s.deletionMu.Unlock()
	ctx, release, err := s.projectionContext(ctx)
	if err != nil {
		return apiError(503, "project deletion unavailable; retry DELETE")
	}
	defer release()
	var completed bool
	err = s.db.QueryRowContext(ctx, `SELECT completed FROM project_deletions WHERE project_id=?`, id).Scan(&completed)
	if errors.Is(err, sql.ErrNoRows) {
		err = s.stageProjectDeletion(ctx, id)
	}
	if err == nil && !completed {
		err = s.purgeProjectExports(ctx, id)
		if err == nil {
			err = s.deleteProjectClickHouse(ctx, id)
		}
		if err == nil {
			_, err = s.db.ExecContext(ctx, `UPDATE project_deletions SET completed=1 WHERE project_id=?`, id)
		}
	}
	if err == nil {
		return nil
	}
	var known *managementError
	if errors.As(err, &known) {
		return known
	}
	slog.Warn("project deletion incomplete", "project_id", id, "error", err)
	return apiError(503, "project deletion incomplete; retry DELETE with the same project ID")
}

// Resume interrupted cleanup after restart, even if the calling agent is gone.
func (s *Service) startDeletionRecovery() {
	ctx, cancel := context.WithCancel(context.Background())
	s.deletionCancel = cancel
	s.deletionDone = make(chan struct{})
	go func() {
		defer close(s.deletionDone)
		for ctx.Err() == nil {
			s.resumeProjectDeletions(ctx)
			t := time.NewTimer(time.Minute)
			select {
			case <-ctx.Done():
				t.Stop()
				return
			case <-t.C:
			}
		}
	}()
}

func (s *Service) resumeProjectDeletions(ctx context.Context) {
	rows, err := s.db.QueryContext(ctx, `SELECT project_id FROM project_deletions WHERE completed=0 ORDER BY rowid`)
	if err != nil {
		return
	}
	var ids []string
	for rows.Next() {
		var id string
		if rows.Scan(&id) != nil {
			break
		}
		ids = append(ids, id)
	}
	rows.Close()
	for _, id := range ids {
		if ctx.Err() != nil {
			return
		}
		_ = s.deleteProject(ctx, id)
	}
}
