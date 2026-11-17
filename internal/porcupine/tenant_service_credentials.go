package porcupine

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"slices"
	"strings"

	"github.com/danielgtaylor/huma/v2"
)

type TenantServiceAccountSpec struct {
	Name        string   `json:"name" minLength:"1" maxLength:"200"`
	Permissions []string `json:"permissions" minItems:"1" maxItems:"32" doc:"Fixed project permissions; administration scopes are not available to machine accounts."`
	Projects    []string `json:"projects" minItems:"1" maxItems:"64" doc:"Explicit projects belonging to this tenant. No wildcard."`
	SourceID    string   `json:"source_id,omitempty" maxLength:"128" doc:"Optional producer source, requiring events:write and exactly one project. Immutable after creation."`
}

type TenantServiceAccountCreateInput struct {
	TenantID string `path:"tenant_id"`
	Body     TenantServiceAccountSpec
}

type TenantServiceAccountUpdateInput struct {
	TenantID string `path:"tenant_id"`
	ID       string `path:"id"`
	Body     TenantServiceAccountSpec
}

type TenantServiceAccountIDInput struct {
	TenantID string `path:"tenant_id"`
	ID       string `path:"id"`
}

type TenantServiceAccountListInput struct {
	TenantID string `path:"tenant_id"`
	Limit    int    `query:"limit" default:"20" minimum:"1" maximum:"100"`
	Offset   int    `query:"offset" default:"0" minimum:"0"`
}

type TenantServiceAccountListOutput struct {
	Body struct {
		ServiceAccounts []OAuthClient `json:"service_accounts"`
		NextOffset      *int          `json:"next_offset,omitempty"`
	}
}

type TenantServiceKeyCreateInput struct {
	TenantID string `path:"tenant_id"`
	ID       string `path:"id"`
	Body     ServiceKeySpec
}

type TenantServiceKeyListInput struct {
	TenantID string `path:"tenant_id"`
	ID       string `path:"id"`
	Limit    int    `query:"limit" default:"20" minimum:"1" maximum:"100"`
	Offset   int    `query:"offset" default:"0" minimum:"0"`
}

type TenantServiceKeyListOutput struct {
	Body struct {
		AccessKeys []AccessKey `json:"access_keys"`
		NextOffset *int        `json:"next_offset,omitempty"`
	}
}

type TenantServiceKeyIDInput struct {
	TenantID string `path:"tenant_id"`
	ID       string `path:"id"`
	KeyID    string `path:"key_id"`
}

func (s *Service) registerTenantServiceCredentials(api huma.API, op func(string, string, string, string, operationAccess, int) huma.Operation) {
	if s.oauth == nil {
		return
	}
	access := operationAccess{Permission: "tenant:admin", TenantParameter: "tenant_id", TenantAdmin: true}
	base := "/api/v1/tenants/{tenant_id}/service-accounts"
	huma.Register(api, op("create-tenant-service-account", "POST", base, "Create a team-owned machine account with fixed project permissions and a one-time secret", access, 201), s.createTenantServiceAccount)
	huma.Register(api, op("list-tenant-service-accounts", "GET", base, "List this tenant's team machine accounts without secrets", access, 200), s.listTenantServiceAccounts)
	huma.Register(api, op("update-tenant-service-account", "PUT", base+"/{id}", "Replace a team machine account's name and project grants; grant changes revoke its existing keys", access, 200), s.updateTenantServiceAccount)
	huma.Register(api, op("revoke-tenant-service-account", "DELETE", base+"/{id}", "Revoke this tenant's machine account and all its tokens and keys", access, 204), s.revokeTenantServiceAccount)
	huma.Register(api, op("create-tenant-service-api-key", "POST", base+"/{id}/api-keys", "Issue a separate API or MCP key within this tenant's machine account grants", access, 201), s.createTenantServiceKey)
	huma.Register(api, op("list-tenant-service-api-keys", "GET", base+"/{id}/api-keys", "List a team machine account's API and MCP key metadata without secrets", access, 200), s.listTenantServiceKeys)
	huma.Register(api, op("revoke-tenant-service-api-key", "DELETE", base+"/{id}/api-keys/{key_id}", "Revoke one key owned by this tenant's machine account", access, 204), s.revokeTenantServiceKey)
}

func constrainTenantCredentialActor(ctx context.Context, query oauthQueryer, a actor) (actor, error) {
	var tenant string
	err := query.QueryRowContext(ctx, `SELECT tenant_id FROM tenant_credentials WHERE principal_id=$1`, a.ID).Scan(&tenant)
	if errors.Is(err, sql.ErrNoRows) {
		return a, nil
	}
	if err != nil {
		return actor{}, err
	}
	if a.admin() || slices.Contains(a.Permissions, "tenant:admin") || len(a.Permissions) == 0 || len(a.Projects) == 0 || slices.Contains(a.Projects, "*") {
		return actor{}, errors.New("invalid tenant machine authority")
	}
	for _, permission := range a.Permissions {
		if !slices.Contains(projectPermissionNames, permission) {
			return actor{}, errors.New("invalid tenant machine permission")
		}
	}
	for _, project := range a.Projects {
		var owner string
		if err := query.QueryRowContext(ctx, `SELECT tenant_id FROM projects WHERE id=$1`, project).Scan(&owner); err != nil || owner != tenant {
			return actor{}, errors.New("tenant machine project is unavailable")
		}
	}
	a.tenantIDs = []string{tenant}
	return a, nil
}

func (s *Service) tenantMachineTransaction(ctx context.Context, tenant string) (*sql.Tx, error) {
	caller := principal(ctx)
	if !caller.canAdminTenant(tenant) {
		return nil, apiError(403, "tenant administrator required")
	}
	tx, err := s.control.BeginTx(ctx, nil)
	if err != nil {
		return nil, apiError(503, "team credential storage unavailable")
	}
	fail := func(err error) (*sql.Tx, error) {
		tx.Rollback()
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE metadata SET value=value WHERE key='initialized'`); err != nil {
		return fail(apiError(503, "team credential storage unavailable"))
	}
	if err := s.requireTenantAdminCurrent(ctx, tx, caller, tenant); err != nil {
		return fail(err)
	}
	var exists bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM tenants WHERE id=$1)`, tenant).Scan(&exists); err != nil {
		return fail(apiError(503, "tenant storage unavailable"))
	}
	if !exists {
		return fail(apiError(404, "tenant not found"))
	}
	return tx, nil
}

func (s *Service) requireTenantAdminCurrent(ctx context.Context, query oauthQueryer, caller actor, tenant string) error {
	if !caller.canAdminTenant(tenant) {
		return apiError(403, "tenant administrator required")
	}
	current, err := s.loadCredentialActorFrom(ctx, query, caller.ID)
	if err != nil || !current.canAdminTenant(tenant) {
		return apiError(403, "current tenant administration required")
	}
	return nil
}

func tenantMachineGrant(ctx context.Context, query oauthQueryer, tenant string, spec TenantServiceAccountSpec) (TenantServiceAccountSpec, error) {
	spec.Name = strings.TrimSpace(spec.Name)
	if spec.Name == "" || len(spec.Name) > 200 || len(spec.Permissions) == 0 || len(spec.Permissions) > 32 || len(spec.Projects) == 0 || len(spec.Projects) > 64 {
		return spec, apiError(400, "name, project permissions and explicit projects are required")
	}
	spec.Permissions, spec.Projects = sortedGrants(spec.Permissions), sortedGrants(spec.Projects)
	for _, permission := range spec.Permissions {
		if !slices.Contains(projectPermissionNames, permission) {
			return spec, apiError(400, "machine accounts require project permissions without administration scopes")
		}
	}
	for _, project := range spec.Projects {
		if project == "" || project == "*" {
			return spec, apiError(400, "tenant machines require explicit project IDs")
		}
		var owner string
		err := query.QueryRowContext(ctx, `SELECT tenant_id FROM projects WHERE id=$1`, project).Scan(&owner)
		if errors.Is(err, sql.ErrNoRows) || err == nil && owner != tenant {
			return spec, apiError(403, "project is outside this tenant")
		}
		if err != nil {
			return spec, apiError(503, "project ownership unavailable")
		}
	}
	if err := validateCredentialSource(spec.SourceID, spec.Permissions, spec.Projects); err != nil {
		return spec, err
	}
	return spec, nil
}

func tenantMachineOwned(ctx context.Context, query oauthQueryer, tenant, id string) error {
	var owner string
	err := query.QueryRowContext(ctx, `SELECT t.tenant_id FROM tenant_credentials t JOIN oauth_clients c ON c.client_id=t.principal_id WHERE t.principal_id=$1`, id).Scan(&owner)
	if errors.Is(err, sql.ErrNoRows) || err == nil && owner != tenant {
		return apiError(404, "team service account not found")
	}
	if err != nil {
		return apiError(503, "team credential storage unavailable")
	}
	return nil
}

func tenantMachineClient(ctx context.Context, query oauthQueryer, tenant, id string) (*oauthClient, error) {
	if err := tenantMachineOwned(ctx, query, tenant, id); err != nil {
		return nil, err
	}
	client, err := loadOAuthClient(ctx, query, id)
	if err != nil || !slices.Equal(client.GrantTypes, []string{"client_credentials"}) {
		return nil, apiError(404, "active team service account not found")
	}
	return client, nil
}

func (s *Service) createTenantServiceAccount(ctx context.Context, input *TenantServiceAccountCreateInput) (*OAuthClientCreateOutput, error) {
	tx, err := s.tenantMachineTransaction(ctx, input.TenantID)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	spec, err := tenantMachineGrant(ctx, tx, input.TenantID, input.Body)
	if err != nil {
		return nil, err
	}
	var secretBytes, bearerBytes [32]byte
	if _, err := rand.Read(secretBytes[:]); err != nil {
		return nil, apiError(503, "service account generation unavailable")
	}
	if _, err := rand.Read(bearerBytes[:]); err != nil {
		return nil, apiError(503, "service account generation unavailable")
	}
	id, created := newID(), now()
	secret := base64.RawURLEncoding.EncodeToString(secretBytes[:])
	permissions, _ := json.Marshal(spec.Permissions)
	projects, _ := json.Marshal(spec.Projects)
	if _, err := tx.ExecContext(ctx, `INSERT INTO access_keys(id,name,token_hash,permissions,projects,created_at,source_id) VALUES($1,$2,$3,$4,$5,$6,$7)`, id, spec.Name, hash(base64.RawURLEncoding.EncodeToString(bearerBytes[:])), string(permissions), string(projects), created, spec.SourceID); err != nil {
		return nil, apiError(503, "team credential storage unavailable")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO oauth_clients(client_id,secret_hash,grant_types,redirect_uris,token_endpoint_auth_method) VALUES($1,$2,'["client_credentials"]','[]','client_secret_basic')`, id, hash(secret)); err != nil {
		return nil, apiError(503, "team credential storage unavailable")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO tenant_credentials(principal_id,tenant_id) VALUES($1,$2)`, id, input.TenantID); err != nil {
		return nil, apiError(503, "team credential storage unavailable")
	}
	if err := tx.Commit(); err != nil {
		return nil, apiError(503, "team credential storage unavailable")
	}
	return &OAuthClientCreateOutput{CacheControl: "no-store", Body: OAuthClientCreated{TenantID: input.TenantID, ClientID: id, ClientSecret: secret, Name: spec.Name, SourceID: spec.SourceID, Permissions: spec.Permissions, Projects: spec.Projects, CreatedAt: created, GrantTypes: []string{"client_credentials"}, RedirectURIs: []string{}, TokenEndpointAuthMethod: "client_secret_basic"}}, nil
}

func (s *Service) listTenantServiceAccounts(ctx context.Context, input *TenantServiceAccountListInput) (*TenantServiceAccountListOutput, error) {
	if !principal(ctx).canAdminTenant(input.TenantID) {
		return nil, apiError(403, "tenant administrator required")
	}
	if input.Limit < 1 || input.Limit > 100 || input.Offset < 0 {
		return nil, apiError(400, "invalid pagination")
	}
	rows, err := s.control.QueryContext(ctx, `SELECT k.id,k.name,k.permissions,k.projects,k.created_at,k.revoked_at,k.source_id FROM tenant_credentials t JOIN oauth_clients c ON c.client_id=t.principal_id JOIN access_keys k ON k.id=t.principal_id WHERE t.tenant_id=$1 AND c.grant_types='["client_credentials"]' ORDER BY k.created_at,k.id LIMIT $2 OFFSET $3`, input.TenantID, input.Limit+1, input.Offset)
	if err != nil {
		return nil, apiError(503, "team credential storage unavailable")
	}
	defer rows.Close()
	out := &TenantServiceAccountListOutput{}
	out.Body.ServiceAccounts = []OAuthClient{}
	for rows.Next() {
		var item OAuthClient
		var permissions, projects string
		if err := rows.Scan(&item.ClientID, &item.Name, &permissions, &projects, &item.CreatedAt, &item.RevokedAt, &item.SourceID); err != nil {
			return nil, apiError(503, "team credential storage unavailable")
		}
		if json.Unmarshal([]byte(permissions), &item.Permissions) != nil || json.Unmarshal([]byte(projects), &item.Projects) != nil {
			return nil, apiError(503, "invalid team credential grants")
		}
		if len(out.Body.ServiceAccounts) == input.Limit {
			next := input.Offset + input.Limit
			out.Body.NextOffset = &next
			break
		}
		item.TenantID = input.TenantID
		item.GrantTypes, item.RedirectURIs, item.TokenEndpointAuthMethod = []string{"client_credentials"}, []string{}, "client_secret_basic"
		out.Body.ServiceAccounts = append(out.Body.ServiceAccounts, item)
	}
	if rows.Err() != nil {
		return nil, apiError(503, "team credential storage unavailable")
	}
	return out, nil
}

func (s *Service) updateTenantServiceAccount(ctx context.Context, input *TenantServiceAccountUpdateInput) (*OAuthClientCreateOutput, error) {
	tx, err := s.tenantMachineTransaction(ctx, input.TenantID)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	client, err := tenantMachineClient(ctx, tx, input.TenantID, input.ID)
	if err != nil {
		return nil, err
	}
	spec := input.Body
	if spec.SourceID == "" {
		spec.SourceID = client.SourceID
	}
	if spec.SourceID != client.SourceID {
		return nil, apiError(400, "producer source is immutable")
	}
	spec, err = tenantMachineGrant(ctx, tx, input.TenantID, spec)
	if err != nil {
		return nil, err
	}
	permissions, _ := json.Marshal(spec.Permissions)
	projects, _ := json.Marshal(spec.Projects)
	var created string
	if err := tx.QueryRowContext(ctx, `UPDATE access_keys SET name=$1,permissions=$2,projects=$3 WHERE id=$4 AND revoked_at IS NULL RETURNING created_at`, spec.Name, string(permissions), string(projects), input.ID).Scan(&created); err != nil {
		return nil, apiError(503, "team credential storage unavailable")
	}
	if err := tx.Commit(); err != nil {
		return nil, apiError(503, "team credential storage unavailable")
	}
	return &OAuthClientCreateOutput{CacheControl: "no-store", Body: OAuthClientCreated{TenantID: input.TenantID, ClientID: client.ID, Name: spec.Name, CreatedAt: created, SourceID: spec.SourceID, Permissions: spec.Permissions, Projects: spec.Projects, GrantTypes: client.GrantTypes, RedirectURIs: client.RedirectURIs, TokenEndpointAuthMethod: client.AuthMethod}}, nil
}

func (s *Service) revokeTenantServiceAccount(ctx context.Context, input *TenantServiceAccountIDInput) (*struct{}, error) {
	tx, err := s.tenantMachineTransaction(ctx, input.TenantID)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if err := tenantMachineOwned(ctx, tx, input.TenantID, input.ID); err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE access_keys SET revoked_at=$1 WHERE id=$2 AND revoked_at IS NULL`, now(), input.ID); err != nil {
		return nil, apiError(503, "team credential storage unavailable")
	}
	if err := tx.Commit(); err != nil {
		return nil, apiError(503, "team credential storage unavailable")
	}
	return nil, nil
}

func (s *Service) createTenantServiceKey(ctx context.Context, input *TenantServiceKeyCreateInput) (*AccessKeyCreateOutput, error) {
	tx, err := s.tenantMachineTransaction(ctx, input.TenantID)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	parent, err := tenantMachineClient(ctx, tx, input.TenantID, input.ID)
	if err != nil {
		return nil, err
	}
	body := input.Body
	if body.Audience != "api" && body.Audience != "mcp" {
		return nil, apiError(400, "invalid service key audience")
	}
	sourceID, err := delegatedSourceID(parent.actor, "", body.Permissions, body.Projects)
	if err != nil {
		return nil, err
	}
	spec, err := tenantMachineGrant(ctx, tx, input.TenantID, TenantServiceAccountSpec{Name: body.Name, Permissions: body.Permissions, Projects: body.Projects, SourceID: sourceID})
	if err != nil {
		return nil, err
	}
	for _, project := range spec.Projects {
		for _, permission := range spec.Permissions {
			if !authorize(parent.actor, permission, project) {
				return nil, apiError(403, "key grants exceed its service account")
			}
		}
	}
	secret, err := oauthRandom(serviceKeyPrefix)
	if err != nil {
		return nil, apiError(503, "service key generation unavailable")
	}
	id := newID()
	permissions, _ := json.Marshal(spec.Permissions)
	projects, _ := json.Marshal(spec.Projects)
	if _, err := tx.ExecContext(ctx, `INSERT INTO access_keys(id,name,token_hash,permissions,projects,source_id,created_at) VALUES($1,$2,$3,$4,$5,$6,$7)`, id, spec.Name, hash(secret), string(permissions), string(projects), sourceID, now()); err != nil {
		return nil, apiError(503, "team credential storage unavailable")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO service_api_keys(principal_id,client_id,audience) VALUES($1,$2,$3)`, id, parent.ID, body.Audience); err != nil {
		return nil, apiError(503, "team credential storage unavailable")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO tenant_credentials(principal_id,tenant_id) VALUES($1,$2)`, id, input.TenantID); err != nil {
		return nil, apiError(503, "team credential storage unavailable")
	}
	if err := tx.Commit(); err != nil {
		return nil, apiError(503, "team credential storage unavailable")
	}
	return &AccessKeyCreateOutput{Body: AccessKeyCreated{TenantID: input.TenantID, ID: id, Name: spec.Name, ServiceClientID: parent.ID, Audience: body.Audience, Permissions: spec.Permissions, Projects: spec.Projects, SourceID: sourceID, Token: secret}}, nil
}

func (s *Service) listTenantServiceKeys(ctx context.Context, input *TenantServiceKeyListInput) (*TenantServiceKeyListOutput, error) {
	if !principal(ctx).canAdminTenant(input.TenantID) {
		return nil, apiError(403, "tenant administrator required")
	}
	if input.Limit < 1 || input.Limit > 100 || input.Offset < 0 {
		return nil, apiError(400, "invalid pagination")
	}
	if err := tenantMachineOwned(ctx, s.control, input.TenantID, input.ID); err != nil {
		return nil, err
	}
	rows, err := s.control.QueryContext(ctx, `SELECT k.id,k.name,k.permissions,k.projects,k.created_at,k.revoked_at,k.source_id,b.audience FROM service_api_keys b JOIN access_keys k ON k.id=b.principal_id JOIN tenant_credentials t ON t.principal_id=k.id WHERE b.client_id=$1 AND t.tenant_id=$2 ORDER BY k.created_at,k.id LIMIT $3 OFFSET $4`, input.ID, input.TenantID, input.Limit+1, input.Offset)
	if err != nil {
		return nil, apiError(503, "team credential storage unavailable")
	}
	defer rows.Close()
	out := &TenantServiceKeyListOutput{}
	out.Body.AccessKeys = []AccessKey{}
	for rows.Next() {
		var key AccessKey
		var permissions, projects string
		if err := rows.Scan(&key.ID, &key.Name, &permissions, &projects, &key.CreatedAt, &key.RevokedAt, &key.SourceID, &key.Audience); err != nil {
			return nil, apiError(503, "team credential storage unavailable")
		}
		if len(out.Body.AccessKeys) == input.Limit {
			next := input.Offset + input.Limit
			out.Body.NextOffset = &next
			break
		}
		if json.Unmarshal([]byte(permissions), &key.Permissions) != nil || json.Unmarshal([]byte(projects), &key.Projects) != nil {
			return nil, apiError(503, "invalid service key grants")
		}
		key.TenantID = input.TenantID
		key.ServiceClientID = input.ID
		out.Body.AccessKeys = append(out.Body.AccessKeys, key)
	}
	if rows.Err() != nil {
		return nil, apiError(503, "team credential storage unavailable")
	}
	return out, nil
}

func (s *Service) revokeTenantServiceKey(ctx context.Context, input *TenantServiceKeyIDInput) (*struct{}, error) {
	tx, err := s.tenantMachineTransaction(ctx, input.TenantID)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if err := tenantMachineOwned(ctx, tx, input.TenantID, input.ID); err != nil {
		return nil, err
	}
	var keyExists bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM service_api_keys b JOIN tenant_credentials t ON t.principal_id=b.principal_id WHERE b.principal_id=$1 AND b.client_id=$2 AND t.tenant_id=$3)`, input.KeyID, input.ID, input.TenantID).Scan(&keyExists); err != nil {
		return nil, apiError(503, "team credential storage unavailable")
	}
	if !keyExists {
		return nil, apiError(404, "team service key not found")
	}
	_, err = tx.ExecContext(ctx, `UPDATE access_keys SET revoked_at=$1 WHERE id=$2 AND revoked_at IS NULL AND id IN (SELECT b.principal_id FROM service_api_keys b JOIN tenant_credentials t ON t.principal_id=b.principal_id WHERE b.client_id=$3 AND t.tenant_id=$4)`, now(), input.KeyID, input.ID, input.TenantID)
	if err != nil {
		return nil, apiError(503, "team credential storage unavailable")
	}
	if err := tx.Commit(); err != nil {
		return nil, apiError(503, "team credential storage unavailable")
	}
	return nil, nil
}
