package porcupine

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestProjectDeletionDrainsOnlyTarget(t *testing.T) {
	s := service(t, 20)
	id, _ := project(t, s)
	other, _ := project(t, s)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	first, finish, _ := s.activity.begin(ctx, id)
	second, finishOther, _ := s.activity.begin(ctx, other)
	defer finishOther()
	deleted := make(chan error, 1)
	go func() { deleted <- s.deleteProject(ctx, id) }()
	select {
	case <-first.Done():
	case <-ctx.Done():
		finish()
		t.Fatal("deletion did not cancel the active project worker")
	}
	_, late, admitted := s.activity.begin(ctx, id)
	if admitted {
		late()
		finish()
		t.Fatal("new work admitted while draining")
	}
	if second.Err() != nil {
		finish()
		t.Fatal("unrelated project worker canceled")
	}
	select {
	case err := <-deleted:
		finish()
		t.Fatalf("deleted before worker drained: %v", err)
	default:
	}
	finish()
	select {
	case err := <-deleted:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("deletion never finished")
	}
}

func TestProjectDeletionRecoveryAfterRestart(t *testing.T) {
	c := Config{DBPath: filepath.Join(t.TempDir(), "resume.db"), BootstrapToken: adminToken}
	s, err := Open(c)
	if err != nil {
		t.Fatal(err)
	}
	id, key := project(t, s)
	other, otherKey := project(t, s)
	// Simulate storage that was configured earlier and is unavailable now.
	if _, err = s.db.Exec(`INSERT INTO metadata(key,value) VALUES('query_master_key','disconnected')`); err != nil {
		s.Close()
		t.Fatal(err)
	}
	request(t, s, "DELETE", "/api/v1/projects/"+id, adminToken, "", 503)
	projects := request(t, s, "GET", "/api/v1/projects", adminToken, "", 200)["projects"].([]any)
	if len(projects) != 1 || projects[0].(map[string]any)["id"] != other {
		s.Close()
		t.Fatal("incorrect projects after deferred purge")
	}
	request(t, s, "POST", "/capture/", "", captureBody(key, "deleted"), 401)
	request(t, s, "POST", "/capture/", "", captureBody(otherKey, "survivor"), 200)
	var completed bool
	if err = s.db.QueryRow(`SELECT completed FROM project_deletions WHERE project_id=?`, id).Scan(&completed); err != nil || completed {
		s.Close()
		t.Fatalf("missing pending cleanup: %v", err)
	}
	// These fixtures have no ClickHouse rows. Restore their initial configuration
	// before reopening and verify that restart alone finishes the pending intent.
	if _, err = s.db.Exec(`DELETE FROM metadata WHERE key='query_master_key'`); err != nil {
		s.Close()
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	deadline := time.Now().Add(5 * time.Second)
	for !completed && time.Now().Before(deadline) {
		if err = s.db.QueryRow(`SELECT completed FROM project_deletions WHERE project_id=?`, id).Scan(&completed); err != nil {
			t.Fatal(err)
		}
		if !completed {
			time.Sleep(10 * time.Millisecond)
		}
	}
	if !completed {
		t.Fatal("pending deletion did not resume on startup")
	}
	request(t, s, "DELETE", "/api/v1/projects/"+id, adminToken, "", 204)
}

func TestInterruptedProjectDrainReopensAdmission(t *testing.T) {
	var a projectActivity
	ctx, cancel := context.WithCancel(context.Background())
	_, finish, _ := a.begin(ctx, "p")
	cancel()
	if _, err := a.drain(ctx, "p"); !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected drain error %v", err)
	}
	finish()
	_, end, ok := a.begin(context.Background(), "p")
	if !ok {
		t.Fatal("interrupted drain left project blocked")
	}
	end()
}
