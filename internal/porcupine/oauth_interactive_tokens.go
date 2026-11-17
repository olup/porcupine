package porcupine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"slices"
	"strings"

	"github.com/olup/porcupine/internal/auth"
)

var errInteractiveGrant = errors.New("invalid interactive grant")
var errInteractiveTarget = errors.New("invalid interactive resource")

func (s *Service) loadInteractiveOAuthActor(ctx context.Context, query oauthQueryer, grantID string) (auth.Principal, error) {
	if !s.interactiveOAuthEnabled() {
		return auth.Principal{}, errInteractiveGrant
	}
	var principalID, humanID, clientID, resource string
	var expires int64
	err := query.QueryRowContext(ctx, `SELECT principal_id,human_id,client_id,resource,expires_at_ms FROM oauth_interactive_grants WHERE id=$1`, grantID).Scan(&principalID, &humanID, &clientID, &resource, &expires)
	if err != nil || expires <= s.clock().UnixMilli() {
		return auth.Principal{}, errInteractiveGrant
	}
	var a auth.Principal
	var permissions, projects string
	err = query.QueryRowContext(ctx, `SELECT id,permissions,projects FROM access_keys WHERE id=$1 AND revoked_at IS NULL`, principalID).Scan(&a.ID, &permissions, &projects)
	if err != nil || json.Unmarshal([]byte(permissions), &a.Permissions) != nil || json.Unmarshal([]byte(projects), &a.Projects) != nil {
		return auth.Principal{}, errInteractiveGrant
	}
	human, err := s.loadOAuthHuman(ctx, query, humanID)
	if err != nil {
		return auth.Principal{}, errInteractiveGrant
	}
	client, err := loadOAuthClient(ctx, query, clientID)
	if err != nil || !slices.Contains(client.GrantTypes, "authorization_code") {
		return auth.Principal{}, errInteractiveGrant
	}
	if tenantAdminConsent(a.Permissions) {
		if !s.interactiveScopesAllowed(human, client, resource, a.Permissions) {
			return auth.Principal{}, errInteractiveGrant
		}
		var tenantID string
		if err := query.QueryRowContext(ctx, `SELECT tenant_id FROM oauth_grant_tenants WHERE grant_id=$1`, grantID).Scan(&tenantID); err != nil {
			return auth.Principal{}, errInteractiveGrant
		}
		narrowed, err := s.tenantAdminActor(ctx, query, human, tenantID)
		if err != nil {
			return auth.Principal{}, errInteractiveGrant
		}
		narrowed.ID = principalID
		return narrowed, nil
	}
	filtered := []string{}
	for _, permission := range a.Permissions {
		if s.interactiveScopesAllowed(human, client, resource, []string{permission}) {
			filtered = append(filtered, permission)
		}
	}
	a.Permissions = filtered
	a.Projects = oauthProjectIntersection(oauthProjectIntersection(a.Projects, human.Projects), client.Projects)
	if len(a.Permissions) == 0 || !auth.InteractiveProjectsAllowed(human, client.Principal, a.Permissions, a.Projects) {
		return auth.Principal{}, errInteractiveGrant
	}
	if human.ProjectPermissions != nil {
		a.ProjectPermissions = make(map[string][]string, len(a.Projects))
		for _, project := range a.Projects {
			for _, permission := range a.Permissions {
				if human.Admin() || slices.Contains(human.ProjectPermissions[project], permission) {
					a.ProjectPermissions[project] = append(a.ProjectPermissions[project], permission)
				}
			}
		}
	}
	a.Interactive = true
	return a, nil
}

func revokeInteractiveGrant(ctx context.Context, tx *sql.Tx, grantID string) error {
	_, err := tx.ExecContext(ctx, `UPDATE access_keys SET revoked_at=coalesce(revoked_at,$2) WHERE id=(SELECT principal_id FROM oauth_interactive_grants WHERE id=$1)`, grantID, now())
	return err
}

func (s *Service) interactiveTokenResponse(ctx context.Context, tx *sql.Tx, client *oauthClient, grantID, familyID, resource, scope string, projects []string, refreshExpiry int64) (map[string]any, error) {
	current, err := s.loadInteractiveOAuthActor(ctx, tx, grantID)
	if err != nil {
		return nil, errInteractiveGrant
	}
	permissions := strings.Fields(scope)
	for _, permission := range permissions {
		if !current.Can(permission) {
			return nil, errInteractiveGrant
		}
	}
	tenantRole := tenantAdminConsent(permissions) && len(current.TenantAdmins) == 1
	if len(permissions) == 0 || !tenantRole && (len(projects) == 0 || !current.ContainsProjects(projects)) {
		return nil, errInteractiveGrant
	}
	if err = s.pruneInteractiveOAuth(ctx, tx); err != nil {
		return nil, err
	}
	var perGrant, total, refreshCount, refreshTotal int
	if err = tx.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM oauth_tokens WHERE interactive_grant_id=$1),(SELECT count(*) FROM oauth_tokens),(SELECT count(*) FROM oauth_refresh_tokens WHERE grant_id=$2),(SELECT count(*) FROM oauth_refresh_tokens)`, grantID, grantID).Scan(&perGrant, &total, &refreshCount, &refreshTotal); err != nil {
		return nil, err
	}
	if perGrant >= oauthMaxTokensPerClient || total >= oauthMaxTokens || refreshCount >= 1000 || refreshTotal >= 100000 {
		return nil, errOAuthCapacity
	}
	if !s.oauth.limiter.Allow() {
		return nil, errOAuthCapacity
	}
	access, err := auth.RandomSecret("pc_at_")
	if err != nil {
		return nil, err
	}
	refresh, err := auth.RandomSecret("pc_rt_")
	if err != nil {
		return nil, err
	}
	if tenantRole {
		projects = []string{}
	}
	encodedProjects, _ := json.Marshal(projects)
	now := s.clock().UnixMilli()
	lifetime := min(int64(s.oauth.config.AccessTokenLifetimeSeconds), (refreshExpiry-now)/1000)
	if lifetime < 1 {
		return nil, errInteractiveGrant
	}
	expiry := now + lifetime*1000
	_, err = tx.ExecContext(ctx, `INSERT INTO oauth_tokens(token_hash,client_id,resource,scope,projects_json,issued_at_ms,expires_at_ms,interactive_grant_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, hash(access), client.ID, resource, scope, string(encodedProjects), now, expiry, grantID)
	if err != nil {
		return nil, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO oauth_interactive_access(token_hash,grant_id,family_id) VALUES($1,$2,$3)`, hash(access), grantID, familyID)
	if err != nil {
		return nil, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO oauth_refresh_tokens(token_hash,grant_id,family_id,client_id,scope,projects_json,expires_at_ms) VALUES($1,$2,$3,$4,$5,$6,$7)`, hash(refresh), grantID, familyID, client.ID, scope, string(encodedProjects), refreshExpiry)
	if err != nil {
		return nil, err
	}
	return map[string]any{"access_token": access, "token_type": "Bearer", "expires_in": lifetime, "scope": scope, "resource": resource, "refresh_token": refresh}, nil
}

func (s *Service) exchangeInteractiveCode(ctx context.Context, client *oauthClient, form url.Values) (map[string]any, error) {
	if !s.interactiveOAuthEnabled() || !slices.Contains(client.GrantTypes, "authorization_code") {
		return nil, errInteractiveGrant
	}
	if resource, present := form["resource"]; present && !s.interactiveResourceAllowed(resource[0]) {
		return nil, errInteractiveTarget
	}
	if form.Get("code") == "" || len(form.Get("code")) > 256 || form.Get("redirect_uri") == "" || !slices.Contains(client.RedirectURIs, form.Get("redirect_uri")) {
		return nil, errInteractiveGrant
	}
	tx, err := s.control.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `UPDATE metadata SET value=value WHERE key='initialized'`); err != nil {
		return nil, err
	}
	var grantID, clientID, redirect, challenge, resource string
	var expires, used int64
	err = tx.QueryRowContext(ctx, `SELECT c.grant_id,c.client_id,c.redirect_uri,c.challenge,c.expires_at_ms,c.used_at_ms,g.resource FROM oauth_authorization_codes c JOIN oauth_interactive_grants g ON g.id=c.grant_id WHERE c.code_hash=$1`, hash(form.Get("code"))).Scan(&grantID, &clientID, &redirect, &challenge, &expires, &used, &resource)
	if err != nil || clientID != client.ID || redirect != form.Get("redirect_uri") || !oauthPKCE(form.Get("code_verifier"), challenge) {
		return nil, errInteractiveGrant
	}
	if target, present := form["resource"]; present && target[0] != resource {
		return nil, errInteractiveTarget
	}
	if used != 0 {
		if err = revokeInteractiveGrant(ctx, tx, grantID); err != nil {
			return nil, err
		}
		if err = tx.Commit(); err != nil {
			return nil, err
		}
		return nil, errInteractiveGrant
	}
	if expires <= s.clock().UnixMilli() {
		return nil, errInteractiveGrant
	}
	a, err := s.loadInteractiveOAuthActor(ctx, tx, grantID)
	if err != nil {
		return nil, errInteractiveGrant
	}
	var refreshExpiry int64
	if err = tx.QueryRowContext(ctx, `SELECT expires_at_ms FROM oauth_interactive_grants WHERE id=$1`, grantID).Scan(&refreshExpiry); err != nil {
		return nil, err
	}
	scope := strings.Join(a.Permissions, " ")
	if len(a.TenantAdmins) == 1 {
		scope = tenantAdminScope
	}
	response, err := s.interactiveTokenResponse(ctx, tx, client, grantID, grantID, resource, scope, a.Projects, refreshExpiry)
	if err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE oauth_authorization_codes SET used_at_ms=$1 WHERE code_hash=$2 AND used_at_ms=0`, max(int64(1), s.clock().UnixMilli()), hash(form.Get("code"))); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return response, nil
}

func (s *Service) exchangeInteractiveRefresh(ctx context.Context, client *oauthClient, form url.Values) (map[string]any, error) {
	if !s.interactiveOAuthEnabled() || !slices.Contains(client.GrantTypes, "refresh_token") {
		return nil, errInteractiveGrant
	}
	if resource, present := form["resource"]; present && !s.interactiveResourceAllowed(resource[0]) {
		return nil, errInteractiveTarget
	}
	raw := form.Get("refresh_token")
	if !strings.HasPrefix(raw, "pc_rt_") || len(raw) != 49 {
		return nil, errInteractiveGrant
	}
	tx, err := s.control.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `UPDATE metadata SET value=value WHERE key='initialized'`); err != nil {
		return nil, err
	}
	var grantID, familyID, clientID, scope, projectsJSON, resource string
	var expires, used int64
	err = tx.QueryRowContext(ctx, `SELECT r.grant_id,r.family_id,r.client_id,r.scope,r.projects_json,r.expires_at_ms,r.used_at_ms,g.resource FROM oauth_refresh_tokens r JOIN oauth_interactive_grants g ON g.id=r.grant_id WHERE r.token_hash=$1`, hash(raw)).Scan(&grantID, &familyID, &clientID, &scope, &projectsJSON, &expires, &used, &resource)
	if err != nil || clientID != client.ID || expires <= s.clock().UnixMilli() {
		return nil, errInteractiveGrant
	}
	if target, present := form["resource"]; present && target[0] != resource {
		return nil, errInteractiveTarget
	}
	if used != 0 {
		if err = revokeInteractiveGrant(ctx, tx, grantID); err != nil {
			return nil, err
		}
		if err = tx.Commit(); err != nil {
			return nil, err
		}
		return nil, errInteractiveGrant
	}
	a, err := s.loadInteractiveOAuthActor(ctx, tx, grantID)
	if err != nil {
		return nil, errInteractiveGrant
	}
	var issuedProjects []string
	if json.Unmarshal([]byte(projectsJSON), &issuedProjects) != nil {
		return nil, errInteractiveGrant
	}
	projects := oauthProjectIntersection(issuedProjects, a.Projects)
	allowed := strings.Fields(scope)
	if requested, present := form["scope"]; present {
		permissions := strings.Split(requested[0], " ")
		if len(permissions) == 0 {
			return nil, errInteractiveGrant
		}
		for _, permission := range permissions {
			if !slices.Contains(allowed, permission) {
				return nil, errInteractiveGrant
			}
		}
		allowed = permissions
	}
	current := []string{}
	for _, permission := range allowed {
		if a.Can(permission) {
			current = append(current, permission)
		}
	}
	slices.Sort(current)
	current = slices.Compact(current)
	if len(current) == 0 || len(projects) == 0 && !(tenantAdminConsent(current) && len(a.TenantAdmins) == 1) {
		return nil, errInteractiveGrant
	}
	response, err := s.interactiveTokenResponse(ctx, tx, client, grantID, familyID, resource, strings.Join(current, " "), projects, expires)
	if err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE oauth_refresh_tokens SET used_at_ms=$1 WHERE token_hash=$2 AND used_at_ms=0`, max(int64(1), s.clock().UnixMilli()), hash(raw)); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return response, nil
}

func (s *Service) oauthRevoke(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Pragma", "no-cache")
	form, status, code := auth.ReadOAuthForm(w, r)
	if code != "" {
		oauthTokenError(w, status, code)
		return
	}
	client, status, code := s.authenticateOAuthEndpoint(r, form)
	if code != "" {
		oauthTokenError(w, status, code)
		return
	}
	raw := form.Get("token")
	if raw == "" || len(raw) > 512 {
		oauthTokenError(w, 400, "invalid_request")
		return
	}
	tx, err := s.control.BeginTx(r.Context(), nil)
	if err != nil {
		oauthTokenError(w, 503, "temporarily_unavailable")
		return
	}
	defer tx.Rollback()
	var grantID string
	err = tx.QueryRowContext(r.Context(), `SELECT grant_id FROM oauth_refresh_tokens WHERE token_hash=$1 AND client_id=$2 UNION ALL SELECT t.interactive_grant_id FROM oauth_tokens t WHERE t.token_hash=$3 AND t.client_id=$4 AND t.interactive_grant_id<>''  LIMIT 1`, hash(raw), client.ID, hash(raw), client.ID).Scan(&grantID)
	if err == nil {
		err = revokeInteractiveGrant(r.Context(), tx, grantID)
	} else if errors.Is(err, sql.ErrNoRows) && client.AuthMethod == "none" {
		err = nil
	} else if errors.Is(err, sql.ErrNoRows) {
		_, err = tx.ExecContext(r.Context(), `DELETE FROM oauth_tokens WHERE token_hash=$1 AND client_id=$2`, hash(raw), client.ID)
	}
	if err != nil || tx.Commit() != nil {
		oauthTokenError(w, 503, "temporarily_unavailable")
		return
	}
	w.WriteHeader(200)
}

func (s *Service) validateInteractivePrincipal(ctx context.Context, query oauthQueryer, principal auth.Principal) (auth.Principal, error) {
	var grantID string
	err := query.QueryRowContext(ctx, `SELECT id FROM oauth_interactive_grants WHERE principal_id=$1`, principal.ID).Scan(&grantID)
	if errors.Is(err, sql.ErrNoRows) {
		return principal, nil
	}
	if err != nil {
		return auth.Principal{}, err
	}
	return s.loadInteractiveOAuthActor(ctx, query, grantID)
}

func (s *Service) pruneInteractiveOAuth(ctx context.Context, tx *sql.Tx) error {
	cutoff := s.clock().UnixMilli()
	_, err := tx.ExecContext(ctx, `UPDATE access_keys SET revoked_at=coalesce(revoked_at,$3) WHERE revoked_at IS NULL AND id IN(
 SELECT principal_id FROM oauth_interactive_grants WHERE expires_at_ms<=$1 OR id IN(SELECT grant_id FROM oauth_authorization_codes WHERE used_at_ms=0 AND expires_at_ms<=$2))`, cutoff, cutoff, now())
	if err != nil {
		return err
	}
	for _, query := range []string{
		`DELETE FROM oauth_authorization_codes WHERE grant_id IN(SELECT g.id FROM oauth_interactive_grants g JOIN access_keys k ON k.id=g.principal_id WHERE k.revoked_at IS NOT NULL)`,
		`DELETE FROM oauth_refresh_tokens WHERE grant_id IN(SELECT g.id FROM oauth_interactive_grants g JOIN access_keys k ON k.id=g.principal_id WHERE k.revoked_at IS NOT NULL)`,
	} {
		if _, err = tx.ExecContext(ctx, query); err != nil {
			return err
		}
	}
	_, err = tx.ExecContext(ctx, `DELETE FROM oauth_tokens WHERE expires_at_ms<=$1`, cutoff)
	return err
}
