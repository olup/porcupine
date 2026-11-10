package porcupine

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"

	"github.com/danielgtaylor/huma/v2"
)

const serviceKeyPrefix = "pc_sk_"

type ServiceKeyCreateInput struct {
	ID   string `path:"id" doc:"Machine OAuth client ID that owns this key."`
	Body struct {
		Name        string   `json:"name" minLength:"1" maxLength:"200"`
		Audience    string   `json:"audience" enum:"api,mcp" doc:"Use the key with this resource only."`
		Permissions []string `json:"permissions" minItems:"1" doc:"Subset of the service account's permissions."`
		Projects    []string `json:"projects" minItems:"1" doc:"Subset of the service account's projects."`
	}
}

func (s *Service) initializeServiceKeys(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS service_api_keys(
		principal_id TEXT PRIMARY KEY REFERENCES access_keys(id),
		client_id TEXT NOT NULL REFERENCES oauth_clients(client_id),
		audience TEXT NOT NULL CHECK(audience IN ('api','mcp'))
	);
	CREATE INDEX IF NOT EXISTS service_api_keys_by_client ON service_api_keys(client_id);
	CREATE TRIGGER IF NOT EXISTS revoke_service_api_keys AFTER UPDATE OF revoked_at,permissions,projects,source_id ON access_keys
	WHEN (OLD.revoked_at IS NULL AND NEW.revoked_at IS NOT NULL) OR NEW.permissions<>OLD.permissions
	     OR NEW.projects<>OLD.projects OR NEW.source_id<>OLD.source_id
	BEGIN
		UPDATE access_keys SET revoked_at=coalesce(NEW.revoked_at,datetime('now')) WHERE revoked_at IS NULL
		AND id IN (SELECT principal_id FROM service_api_keys WHERE client_id=NEW.id);
	END;`)
	return err
}

func (s *Service) registerServiceKeys(api huma.API, op func(string, string, string, string, operationAccess, int) huma.Operation) {
	if s.oauth == nil {
		return
	}
	huma.Register(api, op("create-service-api-key", "POST", "/api/v1/oauth-clients/{id}/api-keys",
		"Issue an independent, revocable API or MCP key owned by a machine client", operationAccess{Permission: "instance:admin"}, 201),
		s.managementCreateServiceKey)
}

func (s *Service) managementCreateServiceKey(ctx context.Context, input *ServiceKeyCreateInput) (*AccessKeyCreateOutput, error) {
	caller, body := principal(ctx), input.Body
	if !caller.admin() {
		return nil, apiError(403, "instance administrator required")
	}
	if body.Audience != "api" && body.Audience != "mcp" {
		return nil, apiError(400, "invalid service key audience")
	}
	if err := s.validateCredentialGrant(ctx, caller, body.Name, body.Permissions, body.Projects); err != nil {
		return nil, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, apiError(503, "service key storage unavailable")
	}
	defer tx.Rollback()
	parent, err := loadOAuthClient(ctx, tx, input.ID)
	if err != nil || parent.ID != input.ID {
		return nil, apiError(404, "active service account not found")
	}
	if !slices.Equal(parent.GrantTypes, []string{"client_credentials"}) {
		return nil, apiError(400, "a service key must belong to a machine-only client")
	}
	if !parent.containsProjects(body.Projects) {
		return nil, apiError(403, "key projects exceed service account grants")
	}
	for _, permission := range body.Permissions {
		if !parent.can(permission) {
			return nil, apiError(403, "key permissions exceed service account grants")
		}
	}
	sourceID, err := delegatedSourceID(parent.actor, "", body.Permissions, body.Projects)
	if err != nil {
		return nil, err
	}

	secret, err := oauthRandom(serviceKeyPrefix)
	if err != nil {
		return nil, apiError(503, "service key generation unavailable")
	}
	permissions, _ := json.Marshal(body.Permissions)
	projects, _ := json.Marshal(body.Projects)
	id := newID()
	if _, err := tx.ExecContext(ctx, `INSERT INTO access_keys(id,name,token_hash,permissions,projects,source_id,created_at) VALUES(?,?,?,?,?,?,?)`,
		id, body.Name, hash(secret), string(permissions), string(projects), sourceID, now()); err != nil {
		return nil, apiError(503, "service key storage unavailable")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO service_api_keys(principal_id,client_id,audience) VALUES(?,?,?)`, id, parent.ID, body.Audience); err != nil {
		return nil, apiError(503, "service key storage unavailable")
	}
	if err := tx.Commit(); err != nil {
		return nil, apiError(503, "service key storage unavailable")
	}
	return &AccessKeyCreateOutput{Body: AccessKeyCreated{ID: id, Name: body.Name, ServiceClientID: parent.ID, Audience: body.Audience, Permissions: body.Permissions, Projects: body.Projects, SourceID: sourceID, Token: secret}}, nil
}

func (s *Service) authenticateServiceKey(ctx context.Context, raw, resource string) (actor, error) {
	if s.oauth == nil || len(raw) != len(serviceKeyPrefix)+43 || !strings.HasPrefix(raw, serviceKeyPrefix) {
		return actor{}, errors.New("invalid service key")
	}
	var audience string
	switch resource {
	case s.oauth.apiResource:
		audience = "api"
	case s.oauth.mcpResource:
		audience = "mcp"
	default:
		return actor{}, errors.New("invalid service key resource")
	}
	a := actor{serviceKey: true}
	var permissions, projects string
	err := s.db.QueryRowContext(ctx, `SELECT k.id,k.permissions,k.projects,k.source_id FROM service_api_keys b
		JOIN access_keys k ON k.id=b.principal_id JOIN access_keys parent ON parent.id=b.client_id
		WHERE k.token_hash=? AND b.audience=? AND k.revoked_at IS NULL AND parent.revoked_at IS NULL`, hash(raw), audience).Scan(&a.ID, &permissions, &projects, &a.SourceID)
	if err != nil {
		return actor{}, err
	}
	if json.Unmarshal([]byte(permissions), &a.Permissions) != nil || json.Unmarshal([]byte(projects), &a.Projects) != nil {
		return actor{}, errors.New("invalid service key grants")
	}
	return a, nil
}
