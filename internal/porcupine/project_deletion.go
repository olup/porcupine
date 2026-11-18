package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/olup/porcupine/internal/apierr"
	"github.com/olup/porcupine/internal/auth"
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
	if err = tx.QueryRowContext(ctx, `SELECT 1 FROM projects WHERE id=$1`, id).Scan(&one); errors.Is(err, sql.ErrNoRows) {
		return apierr.New(404, "project not found")
	} else if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO project_deletions(project_id,completed) VALUES($1,0)`, id); err != nil {
		return err
	}
	err = purgeProjectLocalRecords(ctx, tx, id)
	if err != nil {
		return err
	}
	return tx.Commit()
}

type projectCleanupContextKey struct{}

func (s *Service) deleteProject(ctx context.Context, id string) error {
	if s.sealHostedProject != nil {
		return s.deleteHostedProject(ctx, id)
	}
	if err := s.deletionMu.Lock(ctx); err != nil {
		return apierr.New(503, "project deletion interrupted; retry DELETE")
	}
	defer s.deletionMu.Unlock()
	// Cancel local operations before waiting for the SQLite connection. A request
	// materializing a project anchor may itself be waiting for that connection.
	finishActivity, err := s.activity.drain(ctx, id)
	if err != nil {
		return apierr.New(503, "project deletion is waiting for local work; retry DELETE")
	}
	defer finishActivity()
	ctx = context.WithValue(ctx, projectCleanupContextKey{}, true)
	var alreadyCompleted bool
	if err := s.control.QueryRowContext(ctx, `SELECT completed FROM project_deletions WHERE project_id=$1`, id).Scan(&alreadyCompleted); err == nil && alreadyCompleted {
		return nil
	}
	if err := s.beginControlProjectDeletion(ctx, id); err != nil {
		return err
	}
	var staged bool
	if err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM project_deletions WHERE project_id=$1)`, id).Scan(&staged); err != nil {
		return apierr.New(503, "local deletion state unavailable")
	}
	if !staged {
		if err := s.ensureProjectLocal(ctx, id, auth.Principal{}); err != nil {
			return err
		}
	}
	ctx, release, err := s.projectionContext(ctx)
	if err != nil {
		return apierr.New(503, "project deletion unavailable; retry DELETE")
	}
	defer release()
	var completed bool
	err = s.db.QueryRowContext(ctx, `SELECT completed FROM project_deletions WHERE project_id=$1`, id).Scan(&completed)
	if errors.Is(err, sql.ErrNoRows) {
		err = s.pipelines.CleanupProject(ctx, id)
		if err == nil {
			err = s.stageProjectDeletion(ctx, id)
		}
	}
	if err == nil && !completed {
		err = s.exports.CleanupProject(ctx, id)
		if err == nil {
			err = s.deleteProjectArchive(ctx, id)
		}
		if err == nil {
			_, err = s.db.ExecContext(ctx, `UPDATE project_deletions SET completed=1 WHERE project_id=$1`, id)
		}
	}
	if err == nil {
		if err := s.completeControlProjectDeletion(ctx, id); err != nil {
			return apierr.New(503, "project metadata cleanup incomplete; retry DELETE")
		}
		return nil
	}
	var known *apierr.Error
	if errors.As(err, &known) {
		return known
	}
	slog.Warn("project deletion incomplete", "project_id", id)
	return apierr.New(503, "project deletion incomplete; retry DELETE with the same project ID")
}

// Resume interrupted cleanup after restart, even if the calling agent is gone.
func (s *Service) startDeletionRecovery() {
	ctx, cancel := context.WithCancel(context.Background())
	s.deletionCancel = cancel
	s.deletionDone = make(chan struct{})
	s.deletionWake = make(chan struct{}, 1)
	go func() {
		defer close(s.deletionDone)
		for ctx.Err() == nil {
			s.resumeProjectDeletions(ctx)
			s.resumeTenantDeletions(ctx)
			t := time.NewTimer(time.Minute)
			select {
			case <-ctx.Done():
				t.Stop()
				return
			case <-t.C:
			case <-s.deletionWake:
				t.Stop()
			}
		}
	}()
}

func (s *Service) resumeProjectDeletions(ctx context.Context) {
	if s.sealHostedProject != nil {
		s.resumeHostedProjectSeals(ctx)
		if s.hosted != nil {
			s.resumeHostedProjectCleanup(ctx)
		}
		return
	}
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
	rows, err = s.control.QueryContext(ctx, `SELECT project_id FROM project_deletions WHERE completed=0`)
	if err == nil {
		known := map[string]bool{}
		for _, id := range ids {
			known[id] = true
		}
		for rows.Next() {
			var id string
			if rows.Scan(&id) != nil {
				break
			}
			if !known[id] {
				ids = append(ids, id)
				known[id] = true
			}
		}
		rows.Close()
	}
	for _, id := range ids {
		if ctx.Err() != nil {
			return
		}
		_ = s.deleteProject(ctx, id)
	}
}
