package porcupine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"slices"
	"strings"

	"github.com/danielgtaylor/huma/v2"

	"github.com/olup/porcupine/internal/auth"
	eventapi "github.com/olup/porcupine/internal/event"
)

const serviceKeyPrefix = "pc_sk_"

type ServiceKeyCreateInput struct {
	ID   string `path:"id" doc:"Machine OAuth client ID that owns this key."`
	Body ServiceKeySpec
}

type ServiceKeySpec struct {
	Name        string   `json:"name" minLength:"1" maxLength:"200"`
	Audience    string   `json:"audience" enum:"api,mcp" doc:"Use the key with this resource only."`
	Permissions []string `json:"permissions" minItems:"1" maxItems:"32" doc:"Subset of the service account's permissions."`
	Projects    []string `json:"projects" minItems:"1" maxItems:"64" doc:"Subset of the service account's projects."`
}

func (s *Service) registerServiceKeys(api huma.API, op func(string, string, string, string, auth.OperationAccess, int) huma.Operation) {
	if s.resources == nil {
		return
	}
	huma.Register(api, op("create-service-api-key", "POST", "/api/v1/oauth-clients/{id}/api-keys",
		"Issue an independent, revocable API or MCP key owned by a machine client", auth.OperationAccess{Permission: "instance:admin"}, 201),
		s.managementCreateServiceKey)
}

func (s *Service) managementCreateServiceKey(ctx context.Context, input *ServiceKeyCreateInput) (*AccessKeyCreateOutput, error) {
	caller, body := principal(ctx), input.Body
	if !caller.Admin() {
		return nil, apiError(403, "instance administrator required")
	}
	if body.Audience != "api" && body.Audience != "mcp" {
		return nil, apiError(400, "invalid service key audience")
	}
	if err := s.validateCredentialGrant(ctx, caller, body.Name, body.Permissions, body.Projects); err != nil {
		return nil, err
	}

	tx, err := s.control.BeginTx(ctx, nil)
	if err != nil {
		return nil, apiError(503, "service key storage unavailable")
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE metadata SET value=value WHERE key='initialized'`); err != nil {
		return nil, apiError(503, "service key storage unavailable")
	}
	if err := s.requireExternalCurrent(ctx, tx); err != nil {
		return nil, err
	}
	if err := requireActiveProjectSet(ctx, tx, body.Projects); err != nil {
		return nil, err
	}
	parent, err := loadOAuthClient(ctx, tx, input.ID)
	if err != nil || parent.ID != input.ID {
		return nil, apiError(404, "active service account not found")
	}
	if !slices.Equal(parent.GrantTypes, []string{"client_credentials"}) {
		return nil, apiError(400, "a service key must belong to a machine-only client")
	}
	if !parent.ContainsProjects(body.Projects) {
		return nil, apiError(403, "key projects exceed service account grants")
	}
	for _, permission := range body.Permissions {
		if !parent.Can(permission) {
			return nil, apiError(403, "key permissions exceed service account grants")
		}
	}
	sourceID, err := delegatedSourceID(parent.Principal, "", body.Permissions, body.Projects)
	if err != nil {
		return nil, err
	}

	secret, err := auth.RandomSecret(serviceKeyPrefix)
	if err != nil {
		return nil, apiError(503, "service key generation unavailable")
	}
	permissions, _ := json.Marshal(body.Permissions)
	projects, _ := json.Marshal(body.Projects)
	id := eventapi.NewID()
	if _, err := tx.ExecContext(ctx, `INSERT INTO access_keys(id,name,token_hash,permissions,projects,source_id,created_at) VALUES($1,$2,$3,$4,$5,$6,$7)`,
		id, body.Name, hash(secret), string(permissions), string(projects), sourceID, now()); err != nil {
		return nil, apiError(503, "service key storage unavailable")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO service_api_keys(principal_id,client_id,audience) VALUES($1,$2,$3)`, id, parent.ID, body.Audience); err != nil {
		return nil, apiError(503, "service key storage unavailable")
	}
	if len(parent.TenantIDs) == 1 {
		if _, err := tx.ExecContext(ctx, `INSERT INTO tenant_credentials(principal_id,tenant_id) VALUES($1,$2)`, id, parent.TenantIDs[0]); err != nil {
			return nil, apiError(503, "service key ownership unavailable")
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, apiError(503, "service key storage unavailable")
	}
	tenantID := ""
	if len(parent.TenantIDs) == 1 {
		tenantID = parent.TenantIDs[0]
	}
	return &AccessKeyCreateOutput{Body: AccessKeyCreated{TenantID: tenantID, ID: id, Name: body.Name, ServiceClientID: parent.ID, Audience: body.Audience, Permissions: body.Permissions, Projects: body.Projects, SourceID: sourceID, Token: secret}}, nil
}

func (s *Service) authenticateServiceKey(ctx context.Context, raw, resource string) (auth.Principal, error) {
	if s.resources == nil || len(raw) != len(serviceKeyPrefix)+43 || !strings.HasPrefix(raw, serviceKeyPrefix) {
		return auth.Principal{}, errors.New("invalid service key")
	}
	var audience string
	switch resource {
	case s.resources.apiResource:
		audience = "api"
	case s.resources.mcpResource:
		audience = "mcp"
	default:
		return auth.Principal{}, errors.New("invalid service key resource")
	}
	a := auth.Principal{ServiceKey: true}
	var permissions, projects string
	err := s.control.QueryRowContext(ctx, `SELECT k.id,k.permissions,k.projects,k.source_id FROM service_api_keys b
		JOIN access_keys k ON k.id=b.principal_id JOIN access_keys parent ON parent.id=b.client_id
		WHERE k.token_hash=$1 AND b.audience=$2 AND k.revoked_at IS NULL AND parent.revoked_at IS NULL`, hash(raw), audience).Scan(&a.ID, &permissions, &projects, &a.SourceID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return auth.Principal{}, err
		}
		return auth.Principal{}, auth.ErrUnavailable
	}
	if json.Unmarshal([]byte(permissions), &a.Permissions) != nil || json.Unmarshal([]byte(projects), &a.Projects) != nil {
		return auth.Principal{}, auth.ErrUnavailable
	}
	return constrainServiceKeyActor(ctx, s.control, a)
}

func constrainServiceKeyActor(ctx context.Context, query oauthQueryer, a auth.Principal) (auth.Principal, error) {
	var parentID string
	err := query.QueryRowContext(ctx, `SELECT client_id FROM service_api_keys WHERE principal_id=$1`, a.ID).Scan(&parentID)
	if errors.Is(err, sql.ErrNoRows) {
		return a, nil
	}
	if err != nil {
		return auth.Principal{}, auth.ErrUnavailable
	}
	a, err = constrainTenantCredentialActor(ctx, query, a)
	if err != nil {
		return auth.Principal{}, err
	}
	parent, err := loadOAuthClient(ctx, query, parentID)
	if err != nil {
		return auth.Principal{}, err
	}
	if !slices.Equal(parent.GrantTypes, []string{"client_credentials"}) || !slices.Equal(a.TenantIDs, parent.TenantIDs) {
		return auth.Principal{}, errors.New("service key parent authority unavailable")
	}
	for _, project := range a.Projects {
		for _, permission := range a.Permissions {
			if !auth.Authorize(parent.Principal, permission, project) {
				return auth.Principal{}, errors.New("service key exceeds current parent grants")
			}
		}
	}
	a.ServiceKey = true
	return a, nil
}
