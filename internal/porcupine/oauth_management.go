package porcupine

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"slices"

	"github.com/danielgtaylor/huma/v2"

	"github.com/olup/porcupine/internal/auth"
	eventapi "github.com/olup/porcupine/internal/event"
)

// initializeOAuthClients preserves principal IDs shared by grants and async jobs.
// The bootstrap alias is pinned once and never rotates to a newer administrator.
func (s *Service) initializeOAuthClients(ctx context.Context) error {
	if s.externalAuthEnabled() {
		return nil
	}
	tx, err := s.control.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT id,permissions FROM access_keys WHERE name='bootstrap admin' ORDER BY created_at,id`)
	if err != nil {
		return err
	}
	var bootstrap string
	for rows.Next() {
		var id, encoded string
		if err := rows.Scan(&id, &encoded); err != nil {
			rows.Close()
			return err
		}
		var permissions []string
		if json.Unmarshal([]byte(encoded), &permissions) != nil {
			rows.Close()
			return errors.New("invalid bootstrap permissions")
		}
		if slices.Contains(permissions, "instance:admin") {
			bootstrap = id
			break
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if bootstrap != "" {
		if _, err := tx.ExecContext(ctx, `INSERT INTO metadata(key,value) VALUES('oauth_bootstrap_id',$1) ON CONFLICT(key) DO NOTHING`, bootstrap); err != nil {
			return err
		}
	}
	return tx.Commit()
}

type OAuthClient struct {
	TenantID                string   `json:"tenant_id,omitempty" doc:"Tenant that owns this team machine account; omitted for deployment registrations."`
	ClientID                string   `json:"client_id" doc:"OAuth client and access-key principal identifier"`
	Name                    string   `json:"name"`
	SourceID                string   `json:"source_id,omitempty" doc:"Credential-bound logical producer for ingestion; blank uses protocol defaults."`
	Permissions             []string `json:"permissions"`
	Projects                []string `json:"projects"`
	CreatedAt               string   `json:"created_at"`
	RevokedAt               *string  `json:"revoked_at" nullable:"true"`
	GrantTypes              []string `json:"grant_types"`
	RedirectURIs            []string `json:"redirect_uris"`
	TokenEndpointAuthMethod string   `json:"token_endpoint_auth_method"`
}

type OAuthClientCreateInput struct {
	Body struct {
		Name                    string   `json:"name" minLength:"1" maxLength:"200"`
		SourceID                string   `json:"source_id,omitempty" maxLength:"128" pattern:"^([A-Za-z0-9][A-Za-z0-9._:-]{0,127})?$" doc:"Optional logical producer, requiring events:write and exactly one project. Immutable after creation; posthog is reserved for unbound ingestion."`
		Permissions             []string `json:"permissions" minItems:"1" doc:"Permissions cannot exceed the caller's current grants"`
		Projects                []string `json:"projects" minItems:"1" doc:"Existing project IDs, or * for instance-wide scope; cannot exceed caller scope"`
		GrantTypes              []string `json:"grant_types,omitempty" maxItems:"3" doc:"Defaults to client_credentials. authorization_code enables PKCE and rotating refresh tokens."`
		RedirectURIs            []string `json:"redirect_uris,omitempty" maxItems:"16" doc:"Exact pre-registered HTTPS redirects, or HTTP loopback redirects."`
		TokenEndpointAuthMethod string   `json:"token_endpoint_auth_method,omitempty" enum:"client_secret_basic,client_secret_post,none" doc:"none is allowed only for public interactive clients."`
	}
}

type OAuthClientCreated struct {
	TenantID                string   `json:"tenant_id,omitempty"`
	ClientID                string   `json:"client_id"`
	ClientSecret            string   `json:"client_secret,omitempty" doc:"One-time OAuth token-exchange secret. It is never a bearer token or ingestion key."`
	Name                    string   `json:"name"`
	SourceID                string   `json:"source_id,omitempty" doc:"Credential-bound logical producer for ingestion; blank uses protocol defaults."`
	Permissions             []string `json:"permissions"`
	Projects                []string `json:"projects"`
	CreatedAt               string   `json:"created_at"`
	GrantTypes              []string `json:"grant_types"`
	RedirectURIs            []string `json:"redirect_uris"`
	TokenEndpointAuthMethod string   `json:"token_endpoint_auth_method"`
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

func (s *Service) registerOAuthClients(api huma.API, op func(string, string, string, string, auth.OperationAccess, int) huma.Operation) {
	s.registerInteractiveOAuthManagement(api, op)
	base := "/api/v1/oauth-clients"
	huma.Register(api, op("create-oauth-client", "POST", base, "Create a scoped machine OAuth client and return its secret once", auth.OperationAccess{Permission: "management:write"}, 201), s.managementCreateOAuthClient)
	huma.Register(api, op("list-oauth-clients", "GET", base, "List in-scope OAuth client metadata without secrets", auth.OperationAccess{Permission: "management:read"}, 200), s.managementListOAuthClients)
	huma.Register(api, op("revoke-oauth-client", "DELETE", base+"/{id}", "Revoke an OAuth principal and its issued tokens; the last administrator is protected", auth.OperationAccess{Permission: "instance:admin"}, 204), s.managementRevokeOAuthClient)
}

func (s *Service) managementCreateOAuthClient(ctx context.Context, input *OAuthClientCreateInput) (*OAuthClientCreateOutput, error) {
	if s.externalAuthEnabled() && (len(input.Body.GrantTypes) > 0 && !slices.Equal(input.Body.GrantTypes, []string{"client_credentials"}) || len(input.Body.RedirectURIs) > 0) {
		return nil, apiError(400, "external mode supports native machine accounts only; register interactive applications at the external issuer")
	}
	if s.resources == nil {
		return nil, apiError(503, "OAuth client credentials are not configured")
	}
	a, body := principal(ctx), input.Body
	if !a.Admin() && len(a.TenantIDs) > 0 {
		return nil, apiError(403, "use tenant service-account operations for team credentials")
	}
	if !a.CanProvisionCredentials() {
		return nil, apiError(403, "this credential cannot create independent clients")
	}
	if err := s.validateCredentialGrant(ctx, a, body.Name, body.Permissions, body.Projects); err != nil {
		return nil, err
	}
	sourceID, err := delegatedSourceID(a, body.SourceID, body.Permissions, body.Projects)
	if err != nil {
		return nil, err
	}
	body.SourceID = sourceID
	grants, redirects, authMethod, err := auth.NormalizeOAuthClientRegistration(body.GrantTypes, body.RedirectURIs, body.TokenEndpointAuthMethod)
	if err != nil {
		return nil, apiError(400, "invalid OAuth client registration")
	}
	if slices.Contains(body.Permissions, "tenant:admin") && (!slices.Contains(grants, "authorization_code") || slices.Contains(grants, "client_credentials") || s.oauth == nil || s.oauth.config.Email == nil || !a.Admin() || !slices.Equal(body.Permissions, []string{"tenant:admin"}) || !slices.Equal(body.Projects, []string{"*"})) {
		return nil, apiError(400, "tenant administration requires a dedicated interactive email client")
	}
	if slices.Contains(grants, "authorization_code") {
		if sourceID != "" {
			return nil, apiError(400, "interactive clients cannot have a producer source")
		}
		if slices.Contains(body.Permissions, "instance:admin") && (s.oauth.config.Email == nil || !a.Admin() || len(body.Permissions) != 1 || !slices.Equal(body.Projects, []string{"*"}) || slices.Contains(grants, "client_credentials")) {
			return nil, apiError(400, "administrator login requires a dedicated interactive email client with only instance:admin across all projects")
		}
	}
	if slices.Contains(grants, "authorization_code") && !s.interactiveOAuthEnabled() {
		return nil, apiError(503, "interactive OAuth is not configured")
	}
	var clientSecret, unknownBearer [32]byte
	if _, err := rand.Read(clientSecret[:]); err != nil {
		return nil, apiError(500, "cannot create OAuth client")
	}
	if _, err := rand.Read(unknownBearer[:]); err != nil {
		return nil, apiError(500, "cannot create OAuth client")
	}
	secret := base64.RawURLEncoding.EncodeToString(clientSecret[:])
	if authMethod == "none" {
		secret = ""
	}
	id, created := eventapi.NewID(), now()
	permissions, _ := json.Marshal(body.Permissions)
	projects, _ := json.Marshal(body.Projects)
	tx, err := s.control.BeginTx(ctx, nil)
	if err != nil {
		return nil, apiError(500, "storage error")
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE metadata SET value=value WHERE key='initialized'`); err != nil {
		return nil, apiError(503, "credential storage unavailable")
	}
	if err := requireActiveProjectSet(ctx, tx, body.Projects); err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO access_keys(id,name,token_hash,permissions,projects,created_at,source_id) VALUES($1,$2,$3,$4,$5,$6,$7)`, id, body.Name, hash(base64.RawURLEncoding.EncodeToString(unknownBearer[:])), string(permissions), string(projects), created, body.SourceID); err != nil {
		return nil, apiError(500, "storage error")
	}
	grantJSON, _ := json.Marshal(grants)
	redirectJSON, _ := json.Marshal(redirects)
	if _, err = tx.ExecContext(ctx, `INSERT INTO oauth_clients(client_id,secret_hash,grant_types,redirect_uris,token_endpoint_auth_method) VALUES($1,$2,$3,$4,$5)`, id, hash(secret), string(grantJSON), string(redirectJSON), authMethod); err != nil {
		return nil, apiError(500, "storage error")
	}
	if err = tx.Commit(); err != nil {
		return nil, apiError(500, "storage error")
	}
	if s.externalAuthEnabled() {
		secret = ""
	}
	return &OAuthClientCreateOutput{CacheControl: "no-store", Body: OAuthClientCreated{ClientID: id, ClientSecret: secret, Name: body.Name, SourceID: body.SourceID, Permissions: body.Permissions, Projects: body.Projects, CreatedAt: created, GrantTypes: grants, RedirectURIs: redirects, TokenEndpointAuthMethod: authMethod}}, nil
}

func (s *Service) managementListOAuthClients(ctx context.Context, _ *struct{}) (*OAuthClientListOutput, error) {
	a := principal(ctx)
	if !a.Admin() && len(a.TenantIDs) > 0 {
		return nil, apiError(403, "use tenant service-account operations for team credentials")
	}
	rows, err := s.control.QueryContext(ctx, `SELECT k.id,k.name,k.permissions,k.projects,k.created_at,k.revoked_at,k.source_id,c.grant_types,c.redirect_uris,c.token_endpoint_auth_method,coalesce(t.tenant_id,'')
FROM oauth_clients c JOIN access_keys k ON k.id=c.client_id LEFT JOIN tenant_credentials t ON t.principal_id=k.id ORDER BY k.created_at,k.id`)
	if err != nil {
		return nil, apiError(500, "storage error")
	}
	defer rows.Close()
	items := []OAuthClient{}
	for rows.Next() {
		var item OAuthClient
		var permissions, projects, grants, redirects string
		if err := rows.Scan(&item.ClientID, &item.Name, &permissions, &projects, &item.CreatedAt, &item.RevokedAt, &item.SourceID, &grants, &redirects, &item.TokenEndpointAuthMethod, &item.TenantID); err != nil {
			return nil, apiError(500, "storage error")
		}
		if json.Unmarshal([]byte(permissions), &item.Permissions) != nil || json.Unmarshal([]byte(projects), &item.Projects) != nil {
			return nil, apiError(500, "storage error")
		}
		if json.Unmarshal([]byte(grants), &item.GrantTypes) != nil || json.Unmarshal([]byte(redirects), &item.RedirectURIs) != nil {
			return nil, apiError(500, "storage error")
		}
		if a.ContainsProjects(item.Projects) {
			items = append(items, item)
		}
	}
	if rows.Err() != nil {
		return nil, apiError(500, "storage error")
	}
	rows.Close()
	for i := range items {
		items[i].Projects, err = activeProjects(ctx, s.control, items[i].Projects)
		if err != nil {
			return nil, apiError(503, "project authority unavailable")
		}
	}
	return &OAuthClientListOutput{Body: OAuthClientList{Enabled: s.oauth != nil, OAuthClients: items}}, nil
}

func (s *Service) managementRevokeOAuthClient(ctx context.Context, input *OAuthClientDeleteInput) (*struct{}, error) {
	var id string
	err := s.control.QueryRowContext(ctx, `SELECT client_id FROM oauth_clients WHERE client_id=$1`, input.ID).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, apiError(404, "OAuth client not found")
	}
	if err != nil {
		return nil, apiError(500, "storage error")
	}
	return s.managementRevokeKey(ctx, &AccessKeyDeleteInput{ID: id})
}
