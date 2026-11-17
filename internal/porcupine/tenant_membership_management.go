package porcupine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"slices"
	"time"

	"github.com/danielgtaylor/huma/v2"

	"github.com/olup/porcupine/internal/auth"
	eventapi "github.com/olup/porcupine/internal/event"
)

type TenantMemberInviteInput struct {
	TenantID string `path:"tenant_id" maxLength:"128"`
	Body     struct {
		Email string `json:"email" minLength:"3" maxLength:"254"`
		TenantMembershipSpec
	}
}
type TenantMemberUpdateInput struct {
	TenantID string `path:"tenant_id" maxLength:"128"`
	HumanID  string `path:"human_id" maxLength:"128"`
	Body     TenantMembershipSpec
}
type TenantMemberIDInput struct {
	TenantID string `path:"tenant_id" maxLength:"128"`
	HumanID  string `path:"human_id" maxLength:"128"`
}
type TenantMemberListInput struct {
	TenantID string `path:"tenant_id" maxLength:"128"`
	Limit    int    `query:"limit" default:"20" minimum:"1" maximum:"100"`
	Cursor   string `query:"cursor" maxLength:"128"`
}
type TenantMemberOutput struct{ Body TenantMembership }
type TenantMemberListOutput struct {
	Body struct {
		Members    []TenantMembership `json:"members"`
		NextCursor string             `json:"next_cursor,omitempty"`
	}
}
type TenantProjectRolesOutput struct {
	Body struct {
		Roles map[string][]string `json:"roles"`
	}
}

func (s *Service) registerTenantMembers(api huma.API, op func(string, string, string, string, auth.OperationAccess, int) huma.Operation) {
	access := auth.OperationAccess{Permission: "tenant:admin", TenantParameter: "tenant_id", TenantAdmin: true}
	if s.oauth != nil && s.oauth.config.Email != nil {
		base := "/api/v1/tenants/{tenant_id}/members"
		huma.Register(api, op("invite-tenant-member", "POST", base, "Invite an email human to this tenant as administrator or project-scoped member; sends a notice, not a login code", access, 201), s.inviteTenantMember)
		huma.Register(api, op("list-tenant-members", "GET", base, "List this tenant's memberships without exposing other teams or global human grants", access, 200), s.listTenantMembers)
		huma.Register(api, op("update-tenant-member", "PUT", base+"/{human_id}", "Replace this tenant membership and invalidate affected tool connections; protect the last tenant administrator", access, 200), s.updateTenantMember)
		huma.Register(api, op("revoke-tenant-member", "DELETE", base+"/{human_id}", "Revoke only this tenant membership and affected human tools; preserve other tenants and independent team service accounts", access, 204), s.revokeTenantMember)
	}
	huma.Register(api, op("get-tenant-project-roles", "GET", "/api/v1/tenants/{tenant_id}/project-roles", "Discover fixed project role presets used when inviting tenant members", access, 200), s.getTenantProjectRoles)
}

func (s *Service) getTenantProjectRoles(context.Context, *TenantIDInput) (*TenantProjectRolesOutput, error) {
	out := &TenantProjectRolesOutput{}
	out.Body.Roles = map[string][]string{}
	for _, role := range []string{"reader", "contributor", "manager"} {
		permissions, _ := projectRolePermissions(role)
		out.Body.Roles[role] = sortedGrants(permissions)
	}
	return out, nil
}

func (s *Service) beginTenantAdministration(ctx context.Context, tenantID string) (*sql.Tx, error) {
	tx, err := s.control.BeginTx(ctx, nil)
	if err != nil {
		return nil, apiError(503, "tenant membership storage unavailable")
	}
	if _, err = tx.ExecContext(ctx, `UPDATE metadata SET value=value WHERE key='initialized'`); err != nil {
		tx.Rollback()
		return nil, apiError(503, "tenant membership storage unavailable")
	}
	if err := s.requireTenantAdminCurrent(ctx, tx, principal(ctx), tenantID); err != nil {
		tx.Rollback()
		return nil, err
	}
	var exists bool
	if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM tenants WHERE id=$1)`, tenantID).Scan(&exists); err != nil {
		tx.Rollback()
		return nil, apiError(503, "tenant storage unavailable")
	}
	if !exists {
		tx.Rollback()
		return nil, apiError(404, "tenant not found")
	}
	return tx, nil
}

func sameMembership(a, b TenantMembership) bool {
	return a.Role == b.Role && a.ProjectScope == b.ProjectScope && slices.Equal(a.Permissions, b.Permissions) && slices.EqualFunc(a.Grants, b.Grants, func(a, b TenantProjectGrant) bool {
		return a.ProjectID == b.ProjectID && slices.Equal(a.Permissions, b.Permissions)
	})
}

func (s *Service) inviteTenantMember(ctx context.Context, input *TenantMemberInviteInput) (*TenantMemberOutput, error) {
	email, err := auth.NormalizeEmail(input.Body.Email)
	if err != nil {
		return nil, apiError(400, "invalid email address")
	}
	tx, err := s.beginTenantAdministration(ctx, input.TenantID)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	spec, err := canonicalMembership(ctx, tx, input.TenantID, input.Body.TenantMembershipSpec)
	if err != nil {
		return nil, err
	}
	var humanID string
	var revoked *string
	err = tx.QueryRowContext(ctx, `SELECT h.principal_id,k.revoked_at FROM email_humans h JOIN access_keys k ON k.id=h.principal_id WHERE h.email=$1`, email).Scan(&humanID, &revoked)
	if errors.Is(err, sql.ErrNoRows) {
		human := EmailHuman{ID: eventapi.NewID(), Email: email, CreatedAt: now(), Projects: []string{}, Permissions: []string{}, Grants: []HumanProjectGrant{}}
		if err = insertEmailHuman(ctx, tx, human); err != nil {
			return nil, apiError(503, "human account storage unavailable")
		}
		humanID = human.ID
	} else if err != nil {
		return nil, apiError(503, "human account storage unavailable")
	} else if revoked != nil {
		return nil, apiError(409, "human account is disabled; an instance administrator must restore access")
	}
	spec.HumanID, spec.Email = humanID, email
	existing, err := scanTenantMembership(tx.QueryRowContext(ctx, selectTenantMembers+` WHERE m.tenant_id=$1 AND m.human_id=$2`, input.TenantID, humanID))
	if err == nil {
		if existing.RevokedAt != nil || !sameMembership(existing, spec) {
			return nil, apiError(409, "membership already exists with different grants or is revoked; use update-tenant-member")
		}
		spec = existing
	} else if !errors.Is(err, sql.ErrNoRows) {
		return nil, apiError(503, "tenant membership storage unavailable")
	} else {
		spec.CreatedAt, spec.UpdatedAt = now(), now()
		permissions, _ := json.Marshal(spec.Permissions)
		grants, _ := json.Marshal(spec.Grants)
		if _, err = tx.ExecContext(ctx, `INSERT INTO tenant_memberships(tenant_id,human_id,role,project_scope,permissions,project_grants,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, spec.TenantID, spec.HumanID, spec.Role, spec.ProjectScope, string(permissions), string(grants), spec.CreatedAt, spec.UpdatedAt); err != nil {
			return nil, apiError(503, "tenant membership storage unavailable")
		}
	}
	attempt := ""
	send := s.oauth.config.Email.SendInvitation
	if send != nil {
		var sent bool
		var retry int64
		if err := tx.QueryRowContext(ctx, `SELECT notice_sent,notice_retry_after_ms FROM tenant_memberships WHERE tenant_id=$1 AND human_id=$2`, spec.TenantID, spec.HumanID).Scan(&sent, &retry); err != nil {
			return nil, apiError(503, "invitation status unavailable")
		}
		if !sent {
			if s.clock().UnixMilli() < retry {
				return nil, apiError(429, "invitation delivery in progress or cooling down; retry the same request")
			}
			attempt = eventapi.NewID()
			if _, err := tx.ExecContext(ctx, `UPDATE tenant_memberships SET notice_attempt=$1,notice_retry_after_ms=$2 WHERE tenant_id=$3 AND human_id=$4`, attempt, s.clock().Add(time.Minute).UnixMilli(), spec.TenantID, spec.HumanID); err != nil {
				return nil, apiError(503, "invitation status unavailable")
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, apiError(503, "tenant membership storage unavailable")
	}
	if attempt != "" {
		sendCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		sendErr := send(sendCtx, email, false)
		cancel()
		persist, stop := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer stop()
		result, err := s.control.ExecContext(persist, `UPDATE tenant_memberships SET notice_sent=$1,notice_attempt='',notice_retry_after_ms=$2 WHERE tenant_id=$3 AND human_id=$4 AND notice_attempt=$5`, sendErr == nil, s.clock().Add(5*time.Second).UnixMilli(), spec.TenantID, spec.HumanID, attempt)
		if err != nil {
			return nil, apiError(503, "invitation status unavailable; retry the same request")
		}
		changed, err := result.RowsAffected()
		if err != nil || changed != 1 {
			return nil, apiError(503, "invitation status unavailable; retry the same request")
		}
		if sendErr != nil {
			return nil, apiError(503, "membership authorized; retry this request to deliver the invitation notice")
		}
	}
	return &TenantMemberOutput{Body: spec}, nil
}

func (s *Service) protectLastTenantAdmin(ctx context.Context, tx *sql.Tx, existing TenantMembership, nextAdmin bool) error {
	if existing.Role != "admin" || existing.RevokedAt != nil || nextAdmin {
		return nil
	}
	var targetActive bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM access_keys WHERE id=$1 AND revoked_at IS NULL)`, existing.HumanID).Scan(&targetActive); err != nil {
		return apiError(503, "human account storage unavailable")
	}
	if !targetActive {
		return nil
	}
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM tenant_memberships m JOIN access_keys k ON k.id=m.human_id WHERE m.tenant_id=$1 AND m.role='admin' AND m.revoked_at IS NULL AND k.revoked_at IS NULL`, existing.TenantID).Scan(&count); err != nil {
		return apiError(503, "tenant membership storage unavailable")
	}
	if count <= 1 {
		return apiError(409, "cannot remove or demote the last active tenant administrator")
	}
	return nil
}

func (s *Service) updateTenantMember(ctx context.Context, input *TenantMemberUpdateInput) (*TenantMemberOutput, error) {
	tx, err := s.beginTenantAdministration(ctx, input.TenantID)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	existing, err := scanTenantMembership(tx.QueryRowContext(ctx, selectTenantMembers+` WHERE m.tenant_id=$1 AND m.human_id=$2`, input.TenantID, input.HumanID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, apiError(404, "tenant member not found")
	}
	if err != nil {
		return nil, apiError(503, "tenant membership storage unavailable")
	}
	var active bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM access_keys WHERE id=$1 AND revoked_at IS NULL)`, input.HumanID).Scan(&active); err != nil {
		return nil, apiError(503, "human account storage unavailable")
	}
	if !active {
		return nil, apiError(409, "human account is disabled")
	}
	spec, err := canonicalMembership(ctx, tx, input.TenantID, input.Body)
	if err != nil {
		return nil, err
	}
	if err := s.protectLastTenantAdmin(ctx, tx, existing, spec.Role == "admin"); err != nil {
		return nil, err
	}
	spec.HumanID, spec.Email, spec.CreatedAt, spec.UpdatedAt = input.HumanID, existing.Email, existing.CreatedAt, now()
	permissions, _ := json.Marshal(spec.Permissions)
	grants, _ := json.Marshal(spec.Grants)
	if _, err := tx.ExecContext(ctx, `UPDATE tenant_memberships SET role=$1,project_scope=$2,permissions=$3,project_grants=$4,updated_at=$5,revoked_at=NULL WHERE tenant_id=$6 AND human_id=$7`, spec.Role, spec.ProjectScope, string(permissions), string(grants), spec.UpdatedAt, spec.TenantID, spec.HumanID); err != nil {
		return nil, apiError(503, "tenant membership storage unavailable")
	}
	if !sameMembership(existing, spec) || existing.RevokedAt != nil {
		if err := s.revokeTenantHumanTools(ctx, tx, spec.HumanID, spec.TenantID); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, apiError(503, "tenant membership storage unavailable")
	}
	return &TenantMemberOutput{Body: spec}, nil
}

func (s *Service) revokeTenantMember(ctx context.Context, input *TenantMemberIDInput) (*struct{}, error) {
	tx, err := s.beginTenantAdministration(ctx, input.TenantID)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	existing, err := scanTenantMembership(tx.QueryRowContext(ctx, selectTenantMembers+` WHERE m.tenant_id=$1 AND m.human_id=$2`, input.TenantID, input.HumanID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, apiError(404, "tenant member not found")
	}
	if err != nil {
		return nil, apiError(503, "tenant membership storage unavailable")
	}
	if err := s.protectLastTenantAdmin(ctx, tx, existing, false); err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE tenant_memberships SET revoked_at=coalesce(revoked_at,$1),updated_at=$1 WHERE tenant_id=$2 AND human_id=$3`, now(), input.TenantID, input.HumanID); err != nil {
		return nil, apiError(503, "tenant membership storage unavailable")
	}
	if err := s.revokeTenantHumanTools(ctx, tx, input.HumanID, input.TenantID); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, apiError(503, "tenant membership storage unavailable")
	}
	return nil, nil
}

func (s *Service) listTenantMembers(ctx context.Context, input *TenantMemberListInput) (*TenantMemberListOutput, error) {
	if input.Limit < 1 || input.Limit > 100 {
		return nil, apiError(400, "invalid membership pagination")
	}
	rows, err := s.control.QueryContext(ctx, selectTenantMembers+` WHERE m.tenant_id=$1 AND m.human_id>$2 ORDER BY m.human_id LIMIT $3`, input.TenantID, input.Cursor, input.Limit+1)
	if err != nil {
		return nil, apiError(503, "tenant membership storage unavailable")
	}
	defer rows.Close()
	out := &TenantMemberListOutput{}
	out.Body.Members = []TenantMembership{}
	for rows.Next() {
		m, err := scanTenantMembership(rows)
		if err != nil {
			return nil, apiError(503, "tenant membership storage unavailable")
		}
		if len(out.Body.Members) == input.Limit {
			out.Body.NextCursor = out.Body.Members[len(out.Body.Members)-1].HumanID
			break
		}
		out.Body.Members = append(out.Body.Members, m)
	}
	if err := rows.Err(); err != nil {
		return nil, apiError(503, "tenant membership storage unavailable")
	}
	return out, nil
}

func (s *Service) revokeTenantHumanTools(ctx context.Context, tx *sql.Tx, humanID, tenantID string) error {
	projects := map[string]bool{}
	rows, err := tx.QueryContext(ctx, `SELECT id FROM projects WHERE tenant_id=$1`, tenantID)
	if err != nil {
		return apiError(503, "project ownership unavailable")
	}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return apiError(503, "project ownership unavailable")
		}
		projects[id] = true
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return apiError(503, "project ownership unavailable")
	}
	rows, err = tx.QueryContext(ctx, `SELECT g.principal_id,k.projects,coalesce(t.tenant_id,'') FROM oauth_interactive_grants g JOIN access_keys k ON k.id=g.principal_id LEFT JOIN oauth_grant_tenants t ON t.grant_id=g.id WHERE g.human_id=$1 AND k.revoked_at IS NULL`, humanID)
	if err != nil {
		return apiError(503, "tool grant storage unavailable")
	}
	var revoke []string
	for rows.Next() {
		var id, encoded, tenant string
		if err := rows.Scan(&id, &encoded, &tenant); err != nil {
			rows.Close()
			return apiError(503, "tool grant storage unavailable")
		}
		var granted []string
		if json.Unmarshal([]byte(encoded), &granted) != nil {
			rows.Close()
			return apiError(503, "tool grant storage unavailable")
		}
		matches := tenant == tenantID
		for _, project := range granted {
			matches = matches || projects[project]
		}
		if matches {
			revoke = append(revoke, id)
		}
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return apiError(503, "tool grant storage unavailable")
	}
	for _, id := range revoke {
		if _, err := tx.ExecContext(ctx, `UPDATE access_keys SET revoked_at=$1 WHERE id=$2 AND revoked_at IS NULL`, now(), id); err != nil {
			return apiError(503, "tool grant storage unavailable")
		}
	}
	return nil
}
