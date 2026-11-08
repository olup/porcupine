package porcupine

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"

	"github.com/danielgtaylor/huma/v2"
)

// initializeOAuthClients preserves principal IDs shared by grants and async jobs.
// The bootstrap alias is pinned once and never rotates to a newer administrator.
func (s *Service) initializeOAuthClients(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS oauth_clients (
client_id TEXT PRIMARY KEY REFERENCES access_keys(id),
secret_hash TEXT NOT NULL
)`); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO metadata(key,value)
SELECT 'oauth_bootstrap_id',id FROM access_keys
WHERE name='bootstrap admin'
AND EXISTS (SELECT 1 FROM json_each(access_keys.permissions) WHERE value='instance:admin')
ORDER BY created_at,id LIMIT 1`); err != nil {
		return err
	}
	return tx.Commit()
}

type OAuthClient struct {
	ClientID    string   `json:"client_id" doc:"OAuth client and access-key principal identifier"`
	Name        string   `json:"name"`
	Permissions []string `json:"permissions"`
	Projects    []string `json:"projects"`
	CreatedAt   string   `json:"created_at"`
	RevokedAt   *string  `json:"revoked_at" nullable:"true"`
}

type OAuthClientCreateInput struct {
	Body struct {
		Name        string   `json:"name" minLength:"1" maxLength:"200"`
		Permissions []string `json:"permissions" minItems:"1" doc:"Permissions cannot exceed the caller's current grants"`
		Projects    []string `json:"projects" minItems:"1" doc:"Existing project IDs, or * for instance-wide scope; cannot exceed caller scope"`
	}
}

type OAuthClientCreated struct {
	ClientID     string   `json:"client_id"`
	ClientSecret string   `json:"client_secret" doc:"One-time OAuth token-exchange secret. It is never a bearer token or ingestion key."`
	Name         string   `json:"name"`
	Permissions  []string `json:"permissions"`
	Projects     []string `json:"projects"`
	CreatedAt    string   `json:"created_at"`
}

type OAuthClientCreateOutput struct {
	CacheControl string `header:"Cache-Control"`
	Body         OAuthClientCreated
}

type OAuthClientList struct {
	Enabled      bool          `json:"enabled"`
	OAuthClients []OAuthClient `json:"oauth_clients" doc:"Registered clients within the caller's complete project scope. The bootstrap alias is not a registered client."`
}

type OAuthClientListOutput struct{ Body OAuthClientList }

type OAuthClientDeleteInput struct {
	ID string `path:"id" doc:"Registered OAuth client ID. Revocation disables its principal and every token issued to it."`
}

func (s *Service) registerOAuthClients(api huma.API, op func(string, string, string, string, operationAccess, int) huma.Operation) {
	base := "/api/v1/oauth-clients"
	huma.Register(api, op("create-oauth-client", "POST", base, "Create a scoped machine OAuth client and return its secret once", operationAccess{Permission: "management:write"}, 201), s.managementCreateOAuthClient)
	huma.Register(api, op("list-oauth-clients", "GET", base, "List in-scope OAuth client metadata without secrets", operationAccess{Permission: "management:read"}, 200), s.managementListOAuthClients)
	huma.Register(api, op("revoke-oauth-client", "DELETE", base+"/{id}", "Revoke an OAuth principal and its issued tokens; the last administrator is protected", operationAccess{Permission: "instance:admin"}, 204), s.managementRevokeOAuthClient)
}

func (s *Service) managementCreateOAuthClient(ctx context.Context, input *OAuthClientCreateInput) (*OAuthClientCreateOutput, error) {
	if s.oauth == nil {
		return nil, apiError(503, "OAuth client credentials are not configured")
	}
	a, body := principal(ctx), input.Body
	if err := s.validateCredentialGrant(ctx, a, body.Name, body.Permissions, body.Projects); err != nil {
		return nil, err
	}
	var clientSecret, unknownBearer [32]byte
	if _, err := rand.Read(clientSecret[:]); err != nil {
		return nil, apiError(500, "cannot create OAuth client")
	}
	if _, err := rand.Read(unknownBearer[:]); err != nil {
		return nil, apiError(500, "cannot create OAuth client")
	}
	secret := base64.RawURLEncoding.EncodeToString(clientSecret[:])
	id, created := newID(), now()
	permissions, _ := json.Marshal(body.Permissions)
	projects, _ := json.Marshal(body.Projects)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, apiError(500, "storage error")
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `INSERT INTO access_keys(id,name,token_hash,permissions,projects,created_at) VALUES(?,?,?,?,?,?)`, id, body.Name, hash(base64.RawURLEncoding.EncodeToString(unknownBearer[:])), string(permissions), string(projects), created); err != nil {
		return nil, apiError(500, "storage error")
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO oauth_clients(client_id,secret_hash) VALUES(?,?)`, id, hash(secret)); err != nil {
		return nil, apiError(500, "storage error")
	}
	if err = tx.Commit(); err != nil {
		return nil, apiError(500, "storage error")
	}
	return &OAuthClientCreateOutput{CacheControl: "no-store", Body: OAuthClientCreated{ClientID: id, ClientSecret: secret, Name: body.Name, Permissions: body.Permissions, Projects: body.Projects, CreatedAt: created}}, nil
}

func (s *Service) managementListOAuthClients(ctx context.Context, _ *struct{}) (*OAuthClientListOutput, error) {
	a := principal(ctx)
	rows, err := s.db.QueryContext(ctx, `SELECT k.id,k.name,k.permissions,k.projects,k.created_at,k.revoked_at
FROM oauth_clients c JOIN access_keys k ON k.id=c.client_id ORDER BY k.created_at,k.id`)
	if err != nil {
		return nil, apiError(500, "storage error")
	}
	defer rows.Close()
	items := []OAuthClient{}
	for rows.Next() {
		var item OAuthClient
		var permissions, projects string
		if err := rows.Scan(&item.ClientID, &item.Name, &permissions, &projects, &item.CreatedAt, &item.RevokedAt); err != nil {
			return nil, apiError(500, "storage error")
		}
		if json.Unmarshal([]byte(permissions), &item.Permissions) != nil || json.Unmarshal([]byte(projects), &item.Projects) != nil {
			return nil, apiError(500, "storage error")
		}
		if a.containsProjects(item.Projects) {
			items = append(items, item)
		}
	}
	if rows.Err() != nil {
		return nil, apiError(500, "storage error")
	}
	return &OAuthClientListOutput{Body: OAuthClientList{Enabled: s.oauth != nil, OAuthClients: items}}, nil
}

func (s *Service) managementRevokeOAuthClient(ctx context.Context, input *OAuthClientDeleteInput) (*struct{}, error) {
	var id string
	err := s.db.QueryRowContext(ctx, `SELECT client_id FROM oauth_clients WHERE client_id=?`, input.ID).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, apiError(404, "OAuth client not found")
	}
	if err != nil {
		return nil, apiError(500, "storage error")
	}
	return s.managementRevokeKey(ctx, &AccessKeyDeleteInput{ID: id})
}
