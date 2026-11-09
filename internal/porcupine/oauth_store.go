package porcupine

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/url"
	"slices"
	"strings"
	"time"

	liboauth "github.com/go-oauth2/oauth2/v4"
	oautherrors "github.com/go-oauth2/oauth2/v4/errors"
	"github.com/go-oauth2/oauth2/v4/models"
)

const oauthMaxTokensPerClient = 64
const oauthMaxTokens = 10000

var errOAuthCapacity = errors.New("OAuth token capacity reached")

type oauthIssueContextKey struct{}
type oauthIssue struct{ resource string }

type oauthClient struct {
	actor
	secretHash   string
	GrantTypes   []string
	RedirectURIs []string
	AuthMethod   string
}

func (c *oauthClient) GetID() string     { return c.ID }
func (c *oauthClient) GetSecret() string { return "" }
func (c *oauthClient) GetDomain() string { return "" }
func (c *oauthClient) IsPublic() bool    { return c.AuthMethod == "none" }
func (c *oauthClient) GetUserID() string { return c.ID }
func (c *oauthClient) VerifyPassword(secret string) bool {
	return subtle.ConstantTimeCompare([]byte(hash(secret)), []byte(c.secretHash)) == 1
}

type oauthQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func loadOAuthClient(ctx context.Context, query oauthQueryer, id string) (*oauthClient, error) {
	client := &oauthClient{}
	var permissions, projects string
	var grants, redirects string
	var row *sql.Row
	if id == "bootstrap" {
		row = query.QueryRowContext(ctx, `SELECT id,token_hash,permissions,projects,source_id,'["client_credentials"]','[]','client_secret_basic' FROM access_keys WHERE id=(SELECT value FROM metadata WHERE key='oauth_bootstrap_id') AND revoked_at IS NULL`)
	} else {
		row = query.QueryRowContext(ctx, `SELECT k.id,c.secret_hash,k.permissions,k.projects,k.source_id,c.grant_types,c.redirect_uris,c.token_endpoint_auth_method FROM oauth_clients c JOIN access_keys k ON k.id=c.client_id WHERE c.client_id=? AND k.revoked_at IS NULL`, id)
	}
	if err := row.Scan(&client.ID, &client.secretHash, &permissions, &projects, &client.SourceID, &grants, &redirects, &client.AuthMethod); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, oautherrors.ErrInvalidClient
		}
		return nil, err
	}
	if json.Unmarshal([]byte(permissions), &client.Permissions) != nil || json.Unmarshal([]byte(projects), &client.Projects) != nil {
		return nil, errors.New("invalid OAuth client grants")
	}
	if json.Unmarshal([]byte(grants), &client.GrantTypes) != nil || json.Unmarshal([]byte(redirects), &client.RedirectURIs) != nil {
		return nil, errors.New("invalid OAuth registration")
	}
	return client, nil
}

type oauthClientStore struct{ s *Service }

func (store oauthClientStore) GetByID(ctx context.Context, id string) (liboauth.ClientInfo, error) {
	return loadOAuthClient(ctx, store.s.db, id)
}

func oauthPermissions(current actor) []string {
	permissions := []string{}
	for _, permission := range validPermissions {
		if current.can(permission) {
			permissions = append(permissions, permission)
		}
	}
	slices.Sort(permissions)
	return permissions
}

func oauthScope(current actor, requested string, present bool) (string, error) {
	allowed := oauthPermissions(current)
	if !present {
		return strings.Join(allowed, " "), nil
	}
	if requested == "" {
		return "", oautherrors.ErrInvalidScope
	}
	requestedPermissions := strings.Split(requested, " ")
	for _, permission := range requestedPermissions {
		if !slices.Contains(validPermissions, permission) || !slices.Contains(allowed, permission) {
			return "", oautherrors.ErrInvalidScope
		}
	}
	if slices.Contains(requestedPermissions, "instance:admin") {
		requestedPermissions = append([]string{}, validPermissions...)
	}
	slices.Sort(requestedPermissions)
	requestedPermissions = slices.Compact(requestedPermissions)
	return strings.Join(requestedPermissions, " "), nil
}

type oauthOpaqueGenerator struct{ s *Service }

func (g oauthOpaqueGenerator) Token(ctx context.Context, data *liboauth.GenerateBasic, refresh bool) (string, string, error) {
	if refresh {
		return "", "", oautherrors.ErrUnsupportedGrantType
	}
	var random [32]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", "", errors.New("cannot generate OAuth token")
	}
	data.TokenInfo.SetAccessCreateAt(g.s.clock().UTC())
	return "pc_at_" + base64.RawURLEncoding.EncodeToString(random[:]), "", nil
}

type oauthTokenStore struct{ s *Service }

func (store oauthTokenStore) Create(ctx context.Context, info liboauth.TokenInfo) error {
	issue, ok := ctx.Value(oauthIssueContextKey{}).(oauthIssue)
	if !ok || store.s.oauth == nil || (issue.resource != store.s.oauth.apiResource && issue.resource != store.s.oauth.mcpResource) || info.GetAccess() == "" || info.GetCode() != "" || info.GetRefresh() != "" {
		return oautherrors.ErrInvalidRequest
	}
	tx, err := store.s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `UPDATE metadata SET value=value WHERE key='initialized'`); err != nil {
		return err
	}
	client, err := loadOAuthClient(ctx, tx, info.GetClientID())
	if err != nil {
		return err
	}
	if !slices.Contains(client.GrantTypes, "client_credentials") {
		return oautherrors.ErrUnauthorizedClient
	}

	allowed := oauthPermissions(client.actor)
	for _, permission := range strings.Split(info.GetScope(), " ") {
		if !slices.Contains(allowed, permission) {
			return oautherrors.ErrInvalidScope
		}
	}
	now := store.s.clock().UTC()
	if _, err = tx.ExecContext(ctx, `DELETE FROM oauth_tokens WHERE expires_at_ms<=?`, now.UnixMilli()); err != nil {
		return err
	}
	var perClient, total int
	if err = tx.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM oauth_tokens WHERE client_id=?),(SELECT count(*) FROM oauth_tokens)`, client.ID).Scan(&perClient, &total); err != nil {
		return err
	}
	if perClient >= oauthMaxTokensPerClient || total >= oauthMaxTokens {
		return errOAuthCapacity
	}
	info.SetClientID(client.ID)
	info.SetAccessCreateAt(now)
	info.SetAccessExpiresIn(time.Duration(store.s.oauth.config.AccessTokenLifetimeSeconds) * time.Second)
	projects, err := json.Marshal(client.Projects)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO oauth_tokens(token_hash,client_id,resource,scope,projects_json,issued_at_ms,expires_at_ms) VALUES(?,?,?,?,?,?,?)`, hash(info.GetAccess()), client.ID, issue.resource, info.GetScope(), string(projects), now.UnixMilli(), now.Add(info.GetAccessExpiresIn()).UnixMilli())
	if err != nil {
		return err
	}
	return tx.Commit()
}

type oauthStoredToken struct {
	*models.Token
	resource string
	projects []string
	grantID  string
}

func (store oauthTokenStore) GetByAccess(ctx context.Context, raw string) (liboauth.TokenInfo, error) {
	if !strings.HasPrefix(raw, "pc_at_") || len(raw) != 49 {
		return nil, oautherrors.ErrInvalidAccessToken
	}
	var clientID, resource, scope, projectsJSON, grantID string
	var issued, expires int64
	err := store.s.db.QueryRowContext(ctx, `SELECT t.client_id,t.resource,t.scope,t.projects_json,t.issued_at_ms,t.expires_at_ms,t.interactive_grant_id FROM oauth_tokens t WHERE t.token_hash=?`, hash(raw)).Scan(&clientID, &resource, &scope, &projectsJSON, &issued, &expires, &grantID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, oautherrors.ErrInvalidAccessToken
		}
		return nil, err
	}
	if store.s.clock().UnixMilli() >= expires {
		return nil, oautherrors.ErrExpiredAccessToken
	}
	var projects []string
	if json.Unmarshal([]byte(projectsJSON), &projects) != nil {
		return nil, oautherrors.ErrInvalidAccessToken
	}
	token := models.NewToken()
	token.SetAccess(raw)
	token.SetClientID(clientID)
	token.SetScope(scope)
	token.SetAccessCreateAt(time.UnixMilli(issued))
	token.SetAccessExpiresIn(time.Duration(expires-issued) * time.Millisecond)
	token.SetExtension(url.Values{"resource": {resource}})
	return &oauthStoredToken{Token: token, resource: resource, projects: projects, grantID: grantID}, nil
}
func (store oauthTokenStore) RemoveByAccess(ctx context.Context, raw string) error {
	_, err := store.s.db.ExecContext(ctx, `DELETE FROM oauth_tokens WHERE token_hash=?`, hash(raw))
	return err
}
func (oauthTokenStore) RemoveByCode(context.Context, string) error {
	return oautherrors.ErrUnsupportedGrantType
}
func (oauthTokenStore) RemoveByRefresh(context.Context, string) error {
	return oautherrors.ErrUnsupportedGrantType
}
func (oauthTokenStore) GetByCode(context.Context, string) (liboauth.TokenInfo, error) {
	return nil, oautherrors.ErrUnsupportedGrantType
}
func (oauthTokenStore) GetByRefresh(context.Context, string) (liboauth.TokenInfo, error) {
	return nil, oautherrors.ErrUnsupportedGrantType
}

func (s *Service) authenticateOAuthToken(ctx context.Context, raw, resource string) (actor, error) {
	if s.oauth == nil || (resource != s.oauth.apiResource && resource != s.oauth.mcpResource) {
		return actor{}, oautherrors.ErrInvalidAccessToken
	}
	info, err := (oauthTokenStore{s: s}).GetByAccess(ctx, raw)
	if err != nil {
		return actor{}, err
	}
	stored, ok := info.(*oauthStoredToken)
	if !ok || stored.resource != resource {
		return actor{}, oautherrors.ErrInvalidAccessToken
	}
	current, err := s.exportActor(ctx, info.GetClientID())
	if stored.grantID != "" {
		current, err = s.loadInteractiveOAuthActor(ctx, s.db, stored.grantID)
	}
	if err != nil {
		return actor{}, oautherrors.ErrInvalidAccessToken
	}
	allowed := oauthPermissions(current)
	permissions := []string{}
	for _, permission := range strings.Split(info.GetScope(), " ") {
		if slices.Contains(allowed, permission) && slices.Contains(validPermissions, permission) {
			permissions = append(permissions, permission)
		}
	}
	current.Permissions = permissions
	current.Projects = oauthProjectIntersection(stored.projects, current.Projects)
	return current, nil
}

func oauthProjectIntersection(issued, current []string) []string {
	if slices.Contains(issued, "*") {
		return append([]string{}, current...)
	}
	if slices.Contains(current, "*") {
		return append([]string{}, issued...)
	}
	projects := []string{}
	for _, project := range issued {
		if slices.Contains(current, project) {
			projects = append(projects, project)
		}
	}
	return projects
}
