package porcupine

import (
	"context"
	"testing"
)

func TestExternalPreparedJobAnchorSurvivesOtherTenantDeletion(t *testing.T) {
	t.Parallel()
	f := newExternalAuthFixture(t)
	s := f.s
	enableTestExports(t, s, 10)
	operator := f.operator(t)
	tenantProject := func(name string) (string, string) {
		tenant := request(t, s, "POST", "/api/v1/tenants", operator, externalBody(t, map[string]any{"name": name}), 201)["tenant"].(map[string]any)["id"].(string)
		p := request(t, s, "POST", "/api/v1/projects", operator, externalBody(t, map[string]any{"name": "project", "tenant_id": tenant}), 201)["project"].(map[string]any)["id"].(string)
		return tenant, p
	}
	a, _ := tenantProject("A")
	b, pb := tenantProject("B")
	token := f.token(t, "new-B-subject", b, "events:read", s.resources.apiResource, pb)
	actor, err := s.authenticateResource(t.Context(), token, s.resources.apiResource)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.WithValue(t.Context(), actorContextKey{}, actor))
	defer cancel()
	prepared, resume, done := make(chan error, 1), make(chan struct{}), make(chan error, 1)
	go func() {
		err := s.ensureProjectLocal(ctx, pb, actor)
		prepared <- err
		if err != nil {
			return
		}
		select {
		case <-resume:
		case <-ctx.Done():
			done <- ctx.Err()
			return
		}
		_, err = s.createEventExport(ctx, exportInput(pb, "tenant-deletion-race"))
		done <- err
	}()
	if err := <-prepared; err != nil {
		t.Fatal(err)
	}
	// Pause at the real boundary between journal preparation and job insertion.
	// Cleanup of A must not remove B's prepared but not-yet-referenced anchor.
	request(t, s, "DELETE", "/api/v1/tenants/"+a, operator, "", 202)
	waitTenantDeletion(t, s, a, "logically_deleted")
	close(resume)
	if err := <-done; err != nil {
		t.Fatal("other tenant cleanup broke prepared job insertion", err)
	}
	var jobs, links int
	if err := s.db.QueryRow(`SELECT count(*) FROM export_jobs WHERE project_id=$1 AND actor_id=$2`, pb, actor.ID).Scan(&jobs); err != nil || jobs != 1 {
		t.Fatal("B job did not retain its subject", jobs, err)
	}
	if err := s.db.QueryRow(`SELECT count(*) FROM journal_actor_tenants WHERE actor_id=$1 AND tenant_id=$2`, actor.ID, b).Scan(&links); err != nil || links != 1 {
		t.Fatal("B preparation lost its tenant link", links, err)
	}
}
