package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"github.com/danielgtaylor/huma/v2"

	"github.com/olup/porcupine/internal/auth"
)

type HumanIdentity struct {
	ID          string `json:"id" doc:"Stable identifier for this binding. A fresh relink creates a different ID."`
	Issuer      string `json:"issuer"`
	Subject     string `json:"subject"`
	LinkedEmail string `json:"linked_email"`
	CreatedAt   string `json:"created_at"`
}

type HumanIdentityListInput struct {
	ID     string `path:"id" maxLength:"128" doc:"Durable human account ID."`
	Limit  int    `query:"limit" default:"100" minimum:"1" maximum:"200"`
	Cursor string `query:"cursor" maxLength:"128" doc:"Continue after the binding ID returned as next_cursor."`
}

type HumanIdentityListOutput struct {
	Body struct {
		Identities []HumanIdentity `json:"identities"`
		NextCursor string          `json:"next_cursor,omitempty"`
	}
}

type HumanIdentityDeleteInput struct {
	ID         string `path:"id" maxLength:"128" doc:"Durable human account ID."`
	IdentityID string `path:"identity_id" maxLength:"128" doc:"The binding ID returned by list-human-identities."`
}

func (s *Service) registerOIDCIdentityManagement(api huma.API, op func(string, string, string, string, auth.OperationAccess, int) huma.Operation) {
	operator := auth.OperationAccess{Permission: "instance:admin"}
	huma.Register(api, op("list-human-identities", "GET", "/api/v1/humans/{id}/identities", "List upstream sign-in identity bindings for a human; deployment operator only", operator, 200), s.managementListHumanIdentities)
	operation := op("unlink-human-identity", "DELETE", "/api/v1/humans/{id}/identities/{identity_id}", "Unlink one upstream identity and revoke this human's interactive connections; deployment operator only", operator, 204)
	operation.Description = "Recovery operation for one global human account, across all its tenants. Atomically removes the selected binding, revokes all issued interactive grants and invalidates existing browser sign-in proofs and pending email codes. Keeps the human, memberships and team service credentials. Unknown human: 404. Absent binding: 204 without further revocation. Retrying an old binding ID cannot remove a fresh relink. A fresh verified invited sign-in may link again; revoke the human to prevent further sign-in. Requires an independent operator recovery credential."
	huma.Register(api, operation, s.managementUnlinkHumanIdentity)
}

func (s *Service) managementListHumanIdentities(ctx context.Context, input *HumanIdentityListInput) (*HumanIdentityListOutput, error) {
	if !principal(ctx).Admin() {
		return nil, apiError(403, "deployment operator required")
	}
	limit := input.Limit
	if limit == 0 {
		limit = 100
	}
	if limit < 1 || limit > 200 || len(input.Cursor) > 128 {
		return nil, apiError(400, "invalid identity pagination")
	}
	var human string
	if err := s.control.QueryRowContext(ctx, `SELECT principal_id FROM email_humans WHERE principal_id=$1`, input.ID).Scan(&human); errors.Is(err, sql.ErrNoRows) {
		return nil, apiError(404, "human not found")
	} else if err != nil {
		return nil, apiError(503, "identity storage unavailable")
	}
	rows, err := s.control.QueryContext(ctx, `SELECT id,issuer,subject,linked_email,created_at FROM oidc_identities WHERE principal_id=$1 AND id>$2 ORDER BY id LIMIT $3`, input.ID, input.Cursor, limit+1)
	if err != nil {
		return nil, apiError(503, "identity storage unavailable")
	}
	defer rows.Close()
	out := &HumanIdentityListOutput{}
	out.Body.Identities = []HumanIdentity{}
	for rows.Next() {
		var identity HumanIdentity
		if err := rows.Scan(&identity.ID, &identity.Issuer, &identity.Subject, &identity.LinkedEmail, &identity.CreatedAt); err != nil {
			return nil, apiError(503, "identity storage unavailable")
		}
		if len(out.Body.Identities) == limit {
			out.Body.NextCursor = out.Body.Identities[limit-1].ID
			break
		}
		out.Body.Identities = append(out.Body.Identities, identity)
	}
	if rows.Err() != nil {
		return nil, apiError(503, "identity storage unavailable")
	}
	return out, nil
}

func (s *Service) managementUnlinkHumanIdentity(ctx context.Context, input *HumanIdentityDeleteInput) (*struct{}, error) {
	if !principal(ctx).Admin() {
		return nil, apiError(403, "deployment operator required")
	}
	tx, err := s.control.BeginTx(ctx, nil)
	if err != nil {
		return nil, apiError(503, "identity storage unavailable")
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE metadata SET value=value WHERE key='initialized'`); err != nil {
		return nil, apiError(503, "identity storage unavailable")
	}
	current, err := s.loadCredentialActorFrom(ctx, tx, principal(ctx).ID)
	if err != nil || !current.Admin() {
		return nil, apiError(403, "current deployment operator required")
	}
	var human string
	if err := tx.QueryRowContext(ctx, `SELECT principal_id FROM email_humans WHERE principal_id=$1`, input.ID).Scan(&human); errors.Is(err, sql.ErrNoRows) {
		return nil, apiError(404, "human not found")
	} else if err != nil {
		return nil, apiError(503, "identity storage unavailable")
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM oidc_identities WHERE id=$1 AND principal_id=$2`, input.IdentityID, input.ID)
	if err != nil {
		return nil, apiError(503, "identity storage unavailable")
	}
	count, err := result.RowsAffected()
	if err != nil {
		return nil, apiError(503, "identity storage unavailable")
	}
	if count > 0 {
		if _, err := tx.ExecContext(ctx, `UPDATE access_keys SET revoked_at=coalesce(revoked_at,$2) WHERE id IN (SELECT principal_id FROM oauth_interactive_grants WHERE human_id=$1)`, input.ID, now()); err != nil {
			return nil, apiError(503, "identity storage unavailable")
		}
		if _, err := tx.ExecContext(ctx, `UPDATE email_humans SET login_version=login_version+1 WHERE principal_id=$1`, input.ID); err != nil {
			return nil, apiError(503, "identity storage unavailable")
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, apiError(503, "identity storage unavailable")
	}
	return nil, nil
}
