package porcupine

import (
	"context"
	"strings"
	"testing"
)

func TestHumanGrantTransactionsRejectDeletionAfterValidation(t *testing.T) {
	t.Parallel()
	j := newEmailJourney(t)
	s := j.service
	operator := accountTestToken(t, s)
	actor, err := s.authenticateResource(t.Context(), operator, s.oauth.apiResource)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.WithValue(t.Context(), actorContextKey{}, actor)
	a := request(t, s, "POST", "/api/v1/tenants", operator, `{"name":"A"}`, 201)["tenant"].(map[string]any)["id"].(string)
	b := request(t, s, "POST", "/api/v1/tenants", operator, `{"name":"B"}`, 201)["tenant"].(map[string]any)["id"].(string)
	project := func(tenant string) string {
		return request(t, s, "POST", "/api/v1/projects", operator, `{"name":"Project","tenant_id":"`+tenant+`"}`, 201)["project"].(map[string]any)["id"].(string)
	}
	pa, pb := project(a), project(b)
	human := request(t, s, "POST", "/api/v1/humans", operator, tenantTestBody(t, map[string]any{"email": "safe-b@example.test", "projects": []string{pb}, "permissions": []string{"events:read", "management:read"}}), 201)["id"].(string)
	<-j.notices
	app := j.register(t, operator, "safe-b", []string{"events:read", "management:read"}, []string{pb})
	bToken := j.browserLogin(t, app, "safe-b@example.test", "/api/v1", pb, "", 200)
	// Pause exactly between request validation and the durable write boundary.
	spec, err := s.emailGrantSpec(ctx, nil, []string{pa, pb}, []string{"events:read", "management:read"})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.beginTenantDeletion(ctx, a); err != nil {
		t.Fatal(err)
	}
	invitation := spec
	invitation.ID, invitation.Email, invitation.CreatedAt = "stale-invitation", "late-a@example.test", now()
	if _, err := s.inviteHumanGrant(ctx, invitation); err == nil {
		t.Fatal("stale validated invitation committed after the deletion fence")
	}
	if _, err := s.updateHumanGrant(ctx, human, spec); err == nil {
		t.Fatal("stale validated grant update committed after the deletion fence")
	}
	select {
	case notice := <-j.notices:
		t.Fatal("stale invitation sent a notice", notice)
	default:
	}
	var grants string
	if err := s.control.QueryRow(`SELECT project_grants FROM email_humans WHERE principal_id=$1`, human).Scan(&grants); err != nil || strings.Contains(grants, pa) {
		t.Fatal("stale A grant was written", grants, err)
	}
	var invitations int
	if err := s.control.QueryRow(`SELECT count(*) FROM email_humans WHERE email='late-a@example.test'`).Scan(&invitations); err != nil || invitations != 0 {
		t.Fatal("stale invited account survived", invitations, err)
	}
	request(t, s, "GET", "/api/v1/projects/"+pb+"/schema", bToken.AccessToken, "", 200)
	request(t, s, "PUT", "/api/v1/tenants/"+a+"/budgets", operator, tenantTestBody(t, s.defaultTenantBudgets()), 404)
	request(t, s, "GET", "/api/v1/tenants/"+a+"/deletion", operator, "", 200)
}
