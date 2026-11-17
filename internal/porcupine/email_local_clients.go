package porcupine

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/olup/porcupine/internal/auth"
)

// Native tools on the user's device can start PKCE without a client secret.
// Use a separate registration for explicit administrative consent.
const localOAuthCallback = "http://127.0.0.1:43179/callback"

func (s *Service) initializeLocalEmailClients(ctx context.Context) error {
	if s.oauth == nil || s.oauth.config.Email == nil {
		return nil
	}
	tx, err := s.control.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := registerLocalEmailClient(ctx, tx, "porcupine-local", "Porcupine local project access", auth.ProjectPermissions()); err != nil {
		return err
	}
	if err := registerLocalEmailClient(ctx, tx, "porcupine-local-admin", "Porcupine local administration", []string{"instance:admin"}); err != nil {
		return err
	}
	if err := registerLocalEmailClient(ctx, tx, "porcupine-local-tenant-admin", "Porcupine local tenant administration", []string{tenantAdminScope}); err != nil {
		return err
	}
	return tx.Commit()
}

func registerLocalEmailClient(ctx context.Context, tx *sql.Tx, id, name string, permissions []string) error {
	scopes, err := json.Marshal(permissions)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO access_keys(id,name,token_hash,permissions,projects,created_at)
 VALUES($1,$2,$3,$4,'["*"]',$5) ON CONFLICT(id) DO NOTHING`, id, name, hash(oidcRandom()), string(scopes), now())
	if err != nil {
		return err
	}
	redirects, err := json.Marshal([]string{localOAuthCallback})
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO oauth_clients(client_id,secret_hash,grant_types,redirect_uris,token_endpoint_auth_method)
 VALUES($1,$2,'["authorization_code","refresh_token"]',$3,'none') ON CONFLICT(client_id) DO NOTHING`, id, hash(oidcRandom()), string(redirects))
	return err
}
