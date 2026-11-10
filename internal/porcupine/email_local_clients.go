package porcupine

import (
	"context"
	"database/sql"
	"encoding/json"
)

// Native tools on the user's device can start PKCE without a client secret.
// Use a separate registration for explicit administrative consent.
const localOAuthCallback = "http://127.0.0.1:43179/callback"

func (s *Service) initializeLocalEmailClients(ctx context.Context) error {
	if s.oauth == nil || s.oauth.config.Email == nil {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := registerLocalEmailClient(ctx, tx, "porcupine-local", "Porcupine local analytics", interactiveOAuthScopes); err != nil {
		return err
	}
	if s.oauth.config.Email.AdminEmail != "" {
		if err := registerLocalEmailClient(ctx, tx, "porcupine-local-admin", "Porcupine local administration", []string{"instance:admin"}); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func registerLocalEmailClient(ctx context.Context, tx *sql.Tx, id, name string, permissions []string) error {
	scopes, err := json.Marshal(permissions)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO access_keys(id,name,token_hash,permissions,projects,created_at)
 VALUES(?,?,?,?,'["*"]',?) ON CONFLICT(id) DO NOTHING`, id, name, hash(oidcRandom()), string(scopes), now())
	if err != nil {
		return err
	}
	redirects, err := json.Marshal([]string{localOAuthCallback})
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO oauth_clients(client_id,secret_hash,grant_types,redirect_uris,token_endpoint_auth_method)
 VALUES(?,?,'["authorization_code","refresh_token"]',?,'none') ON CONFLICT(client_id) DO NOTHING`, id, hash(oidcRandom()), string(redirects))
	return err
}
