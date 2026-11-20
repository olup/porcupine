package porcupine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"

	"github.com/olup/porcupine/internal/apierr"
	"github.com/olup/porcupine/internal/auth"
	eventapi "github.com/olup/porcupine/internal/event"
)

const (
	clientIngestionKeyKind   = "client_ingestion"
	clientIngestionKeyPrefix = "pc_ci_"
)

type ClientIngestionKeyOutput struct {
	Body struct {
		Key string `json:"key" doc:"Public key for this project's capture routes."`
	}
}

func createClientIngestionKey(ctx context.Context, tx *sql.Tx, project, tenant, name, created string) (string, error) {
	raw, err := auth.RandomSecret(clientIngestionKeyPrefix)
	if err != nil {
		return "", err
	}
	scope, _ := json.Marshal([]string{project})
	id := eventapi.NewID()
	if _, err := tx.ExecContext(ctx, `INSERT INTO access_keys(id,name,token_hash,permissions,projects,created_at,kind,client_ingestion_token)
VALUES($1,$2,$3,'["events:write"]',$4,$5,'client_ingestion',$6)`, id, name, hash(raw), string(scope), created, raw); err != nil {
		return "", err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO tenant_credentials(principal_id,tenant_id) VALUES($1,$2)`, id, tenant); err != nil {
		return "", err
	}
	return raw, nil
}

func loadClientIngestionKey(ctx context.Context, query oauthQueryer, project string) (string, error) {
	scope, _ := json.Marshal([]string{project})
	var raw string
	err := query.QueryRowContext(ctx, `SELECT k.client_ingestion_token FROM access_keys k
WHERE k.projects=$1 AND k.kind='client_ingestion' AND k.revoked_at IS NULL
AND EXISTS(SELECT 1 FROM projects p JOIN tenants t ON t.id=p.tenant_id
	WHERE p.id=$2 AND p.ingestion_mode='mixed'
	AND NOT EXISTS(SELECT 1 FROM project_deletions d WHERE d.project_id=p.id)
	AND NOT EXISTS(SELECT 1 FROM tenant_deletions d WHERE d.tenant_id=t.id))`, string(scope), project).Scan(&raw)
	return raw, err
}

func (s *Service) getClientIngestionKey(ctx context.Context, input *ProjectIDInput) (*ClientIngestionKeyOutput, error) {
	key, err := loadClientIngestionKey(ctx, s.control, input.ID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, apierr.New(404, "active client ingestion key not found")
	}
	if err != nil {
		return nil, apierr.New(503, "client ingestion key storage unavailable")
	}
	out := &ClientIngestionKeyOutput{}
	out.Body.Key = key
	return out, nil
}

func (s *Service) ensureClientIngestionKey(ctx context.Context, input *ProjectIDInput) (*ClientIngestionKeyOutput, error) {
	tenant, err := s.projectAdministrationTenant(ctx, input.ID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, apierr.New(404, "project not found")
	}
	if err != nil {
		return nil, apierr.New(503, "project ownership unavailable")
	}
	tx, err := s.beginTenantAdministration(ctx, tenant)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	// Shared project deletion takes the project row lock independently of tenant administration.
	lock := ""
	if s.shared != nil {
		lock = " FOR UPDATE"
	}
	var name, mode string
	err = tx.QueryRowContext(ctx, `SELECT name,ingestion_mode FROM projects WHERE id=$1 AND tenant_id=$2`+lock, input.ID, tenant).Scan(&name, &mode)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, apierr.New(404, "project not found")
	}
	if err != nil {
		return nil, apierr.New(503, "project ingestion policy unavailable")
	}
	if err := requireActiveProjectSet(ctx, tx, []string{input.ID}); err != nil {
		return nil, err
	}
	if mode != ingestionMixed {
		return nil, apierr.New(409, "client ingestion keys require mixed ingestion")
	}
	key, err := loadClientIngestionKey(ctx, tx, input.ID)
	if errors.Is(err, sql.ErrNoRows) {
		key, err = createClientIngestionKey(ctx, tx, input.ID, tenant, name+" ingestion", now())
	}
	if err != nil || tx.Commit() != nil {
		return nil, apierr.New(503, "client ingestion key storage unavailable")
	}
	out := &ClientIngestionKeyOutput{}
	out.Body.Key = key
	return out, nil
}

func (s *Service) authenticateIngestionKey(ctx context.Context, raw string) (auth.Principal, error) {
	if !strings.HasPrefix(raw, clientIngestionKeyPrefix) {
		return s.authenticate(ctx, raw)
	}
	var a auth.Principal
	var projects string
	err := s.control.QueryRowContext(ctx, `SELECT id,projects FROM access_keys
WHERE token_hash=$1 AND kind='client_ingestion' AND revoked_at IS NULL`, hash(raw)).Scan(&a.ID, &projects)
	if errors.Is(err, sql.ErrNoRows) {
		return auth.Principal{}, err
	}
	if err != nil || json.Unmarshal([]byte(projects), &a.Projects) != nil {
		return auth.Principal{}, auth.ErrUnavailable
	}
	if len(a.Projects) != 1 || a.Projects[0] == "" || a.Projects[0] == "*" {
		return auth.Principal{}, errors.New("invalid client ingestion key scope")
	}
	a.Permissions = []string{"events:write"}
	a.ClientIngestionKey = true
	return a, nil
}
