package porcupine

import (
	"testing"
	"time"
)

func TestSharedControlProjectDeletionRetainsTenantForRetries(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var s *Service
			if backend == "postgres" {
				s = sharedDeletionFixture(t)
				s.eventRules = &eventRules{clock: time.Now}
			} else {
				s = service(t, 20)
			}
			ctx := t.Context()
			tenant, err := s.insertTenant(ctx, "Deletion owner", s.defaultTenantBudgets())
			if err != nil {
				t.Fatal(err)
			}
			id := newID()
			if _, err := s.control.ExecContext(ctx, `INSERT INTO projects(id,tenant_id,name,created_at,ingestion_mode) VALUES($1,$2,'Target','now','mixed')`, id, tenant.ID); err != nil {
				t.Fatal(err)
			}
			check := func(wantComplete bool) {
				t.Helper()
				var owner string
				var complete bool
				if err := s.control.QueryRowContext(ctx, `SELECT tenant_id,completed FROM project_deletions WHERE project_id=$1`, id).Scan(&owner, &complete); err != nil {
					t.Fatal(err)
				}
				if owner != tenant.ID || complete != wantComplete {
					t.Fatalf("lost deletion ownership: %q complete=%t", owner, complete)
				}
			}
			if err := s.beginControlProjectDeletion(ctx, id); err != nil {
				t.Fatal(err)
			}
			check(false)
			if err := s.beginControlProjectDeletion(ctx, id); err != nil {
				t.Fatal(err)
			}
			check(false)
			if err := s.completeControlProjectDeletion(ctx, id); err != nil {
				t.Fatal(err)
			}
			check(true)
			var live int
			if err := s.control.QueryRowContext(ctx, `SELECT count(*) FROM projects WHERE id=$1`, id).Scan(&live); err != nil || live != 0 {
				t.Fatal("live project retained after completion", err)
			}
			if err := s.beginControlProjectDeletion(ctx, id); err != nil {
				t.Fatal("retry lost deletion metadata", err)
			}
			if err := s.completeControlProjectDeletion(ctx, id); err != nil {
				t.Fatal("completion retry lost deletion metadata", err)
			}
			check(true)
		})
	}
}

func TestSharedControlProjectDeletionDoesNotReassignTenantTombstone(t *testing.T) {
	s := service(t, 20)
	id, _ := project(t, s)
	other := tenantTestCreate(t, s, "Other team")
	if _, err := s.control.Exec(`INSERT INTO project_deletions(project_id,tenant_id,completed) VALUES($1,$2,0)`, id, other); err != nil {
		t.Fatal(err)
	}
	if err := s.beginControlProjectDeletion(t.Context(), id); err == nil {
		t.Fatal("conflicting ownership was silently reassigned")
	}
	var owner string
	if err := s.control.QueryRow(`SELECT tenant_id FROM project_deletions WHERE project_id=$1`, id).Scan(&owner); err != nil || owner != other {
		t.Fatal("failed intent changed existing ownership", err)
	}
}
