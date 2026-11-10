package porcupine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/danielgtaylor/huma/v2"
)

type OAuthGrant struct {
	Permissions []string `json:"permissions"`
	Projects    []string `json:"projects"`
	ID          string   `json:"id" doc:"Consent grant and revocable access-key principal identifier"`
	HumanID     string   `json:"human_id" doc:"Human principal ID; revoking it through revoke-access-key revokes every consent grant."`
	ClientID    string   `json:"client_id"`
	Resource    string   `json:"resource" doc:"API or MCP resource authorized by this grant."`
	Issuer      string   `json:"issuer"`
	Subject     string   `json:"subject" doc:"Verified email address, OIDC subject or local invitation ID."`
	CreatedAtMS int64    `json:"created_at_ms"`
	ExpiresAtMS int64    `json:"expires_at_ms"`
	RevokedAt   *string  `json:"revoked_at" nullable:"true"`
}
type OAuthGrantsInput struct {
	Limit  int `query:"limit" default:"20" minimum:"1" maximum:"100"`
	Offset int `query:"offset" default:"0" minimum:"0"`
}
type OAuthGrants struct {
	Grants     []OAuthGrant `json:"grants"`
	Limit      int          `json:"limit"`
	Offset     int          `json:"offset"`
	NextOffset *int         `json:"next_offset,omitempty"`
}
type OAuthGrantsOutput struct{ Body OAuthGrants }
type OAuthGrantRevokeInput struct {
	ID string `path:"id"`
}

func (s *Service) registerInteractiveOAuthManagement(api huma.API, op func(string, string, string, string, operationAccess, int) huma.Operation) {
	s.registerLocalInvitations(api, op)
	access := operationAccess{Permission: "instance:admin"}
	huma.Register(api, op("list-oauth-grants", "GET", "/api/v1/oauth-grants", "Inspect interactive human consent grants and their revocation identities", access, 200), s.managementListOAuthGrants)
	huma.Register(api, op("revoke-oauth-grant", "DELETE", "/api/v1/oauth-grants/{id}", "Revoke a human consent grant, its tokens and background creator authority", access, 204), s.managementRevokeOAuthGrant)
}
func (s *Service) managementListOAuthGrants(ctx context.Context, input *OAuthGrantsInput) (*OAuthGrantsOutput, error) {
	if input.Limit < 1 || input.Limit > 100 || input.Offset < 0 {
		return nil, apiError(400, "invalid OAuth grant pagination")
	}
	rows, err := s.db.QueryContext(ctx, `SELECT g.id,g.human_id,g.client_id,g.resource,CASE WHEN e.principal_id IS NOT NULL THEN 'email' ELSE coalesce(h.issuer,'local-invitation') END,coalesce(e.email,h.subject,g.human_id),g.created_at_ms,g.expires_at_ms,k.revoked_at,k.permissions,k.projects
 FROM oauth_interactive_grants g JOIN access_keys k ON k.id=g.principal_id LEFT JOIN oidc_humans h ON h.principal_id=g.human_id LEFT JOIN email_humans e ON e.principal_id=g.human_id
 ORDER BY g.created_at_ms DESC,g.id LIMIT ? OFFSET ?`, input.Limit+1, input.Offset)
	if err != nil {
		return nil, apiError(503, "OAuth grant storage unavailable")
	}
	defer rows.Close()
	result := OAuthGrants{Grants: []OAuthGrant{}, Limit: input.Limit, Offset: input.Offset}
	for rows.Next() {
		var grant OAuthGrant
		var permissions, projects string
		if rows.Scan(&grant.ID, &grant.HumanID, &grant.ClientID, &grant.Resource, &grant.Issuer, &grant.Subject, &grant.CreatedAtMS, &grant.ExpiresAtMS, &grant.RevokedAt, &permissions, &projects) != nil {
			return nil, apiError(503, "OAuth grant storage unavailable")
		}
		if json.Unmarshal([]byte(permissions), &grant.Permissions) != nil || json.Unmarshal([]byte(projects), &grant.Projects) != nil {
			return nil, apiError(503, "OAuth grant storage unavailable")
		}
		result.Grants = append(result.Grants, grant)
	}
	if rows.Err() != nil {
		return nil, apiError(503, "OAuth grant storage unavailable")
	}
	if len(result.Grants) > input.Limit {
		next := input.Offset + input.Limit
		result.NextOffset = &next
		result.Grants = result.Grants[:input.Limit]
	}
	return &OAuthGrantsOutput{Body: result}, nil
}
func (s *Service) managementRevokeOAuthGrant(ctx context.Context, input *OAuthGrantRevokeInput) (*struct{}, error) {
	var id string
	err := s.db.QueryRowContext(ctx, `SELECT principal_id FROM oauth_interactive_grants WHERE id=?`, input.ID).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, apiError(404, "OAuth grant not found")
	}
	if err != nil {
		return nil, apiError(503, "OAuth grant storage unavailable")
	}
	return s.managementRevokeKey(ctx, &AccessKeyDeleteInput{ID: id})
}
