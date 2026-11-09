package porcupine

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"
)

const oauthRequestTTL = 10 * time.Minute
const oauthCodeTTL = 5 * time.Minute
const oauthRefreshTTL = 7 * 24 * time.Hour

func (s *Service) initializeInteractiveOAuth(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS oauth_authorization_requests(
 request_hash TEXT PRIMARY KEY,client_id TEXT NOT NULL REFERENCES access_keys(id),redirect_uri TEXT NOT NULL,
 scope TEXT NOT NULL,resource TEXT NOT NULL,state TEXT NOT NULL,challenge TEXT NOT NULL,expires_at_ms INTEGER NOT NULL,consumed INTEGER NOT NULL DEFAULT 0,peer_hash TEXT NOT NULL DEFAULT '');
 CREATE TABLE IF NOT EXISTS oauth_interactive_grants(
 id TEXT PRIMARY KEY,principal_id TEXT NOT NULL UNIQUE REFERENCES access_keys(id),human_id TEXT NOT NULL REFERENCES access_keys(id),
 client_id TEXT NOT NULL REFERENCES access_keys(id),created_at_ms INTEGER NOT NULL,expires_at_ms INTEGER NOT NULL);
 CREATE INDEX IF NOT EXISTS oauth_grants_human ON oauth_interactive_grants(human_id);
 CREATE INDEX IF NOT EXISTS oauth_grants_client ON oauth_interactive_grants(client_id);
 CREATE TABLE IF NOT EXISTS oauth_authorization_codes(
 code_hash TEXT PRIMARY KEY,grant_id TEXT NOT NULL REFERENCES oauth_interactive_grants(id),client_id TEXT NOT NULL,
 redirect_uri TEXT NOT NULL,challenge TEXT NOT NULL,expires_at_ms INTEGER NOT NULL,used_at_ms INTEGER NOT NULL DEFAULT 0);
 CREATE TABLE IF NOT EXISTS oauth_refresh_tokens(
 token_hash TEXT PRIMARY KEY,grant_id TEXT NOT NULL REFERENCES oauth_interactive_grants(id),family_id TEXT NOT NULL,
 client_id TEXT NOT NULL,scope TEXT NOT NULL,projects_json TEXT NOT NULL,expires_at_ms INTEGER NOT NULL,used_at_ms INTEGER NOT NULL DEFAULT 0);
 CREATE INDEX IF NOT EXISTS oauth_refresh_grant ON oauth_refresh_tokens(grant_id);
 CREATE INDEX IF NOT EXISTS oauth_refresh_expiry ON oauth_refresh_tokens(expires_at_ms);
 CREATE INDEX IF NOT EXISTS oauth_code_grant ON oauth_authorization_codes(grant_id);
 CREATE TABLE IF NOT EXISTS oauth_interactive_access(
 token_hash TEXT PRIMARY KEY REFERENCES oauth_tokens(token_hash) ON DELETE CASCADE,grant_id TEXT NOT NULL REFERENCES oauth_interactive_grants(id),family_id TEXT NOT NULL);
 CREATE TRIGGER IF NOT EXISTS oauth_human_client_revocation AFTER UPDATE OF revoked_at,permissions,projects ON access_keys
 WHEN NEW.revoked_at IS NOT NULL OR NEW.permissions<>OLD.permissions OR NEW.projects<>OLD.projects BEGIN
 UPDATE access_keys SET revoked_at=coalesce(NEW.revoked_at,datetime('now'))
 WHERE id IN(SELECT principal_id FROM oauth_interactive_grants WHERE human_id=NEW.id OR client_id=NEW.id) AND revoked_at IS NULL;
 END;`)
	if err != nil {
		return err
	}
	for _, column := range []struct{ table, name, definition string }{{"oauth_tokens", "interactive_grant_id", "TEXT NOT NULL DEFAULT ''"}, {"oauth_authorization_requests", "peer_hash", "TEXT NOT NULL DEFAULT ''"}} {
		var exists bool
		if err = s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pragma_table_info(?) WHERE name=?)`, column.table, column.name).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			if _, err = s.db.ExecContext(ctx, `ALTER TABLE `+column.table+` ADD COLUMN `+column.name+` `+column.definition); err != nil {
				return err
			}
		}
	}
	_, err = s.db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS oauth_tokens_interactive_grant ON oauth_tokens(interactive_grant_id)`)
	return err
}

func (s *Service) loadOAuthAuthorizationRequest(ctx context.Context, rawID string) (oauthAuthorizationRequest, error) {
	return s.loadOAuthRequest(ctx, s.db, rawID)
}
func (s *Service) loadOAuthRequest(ctx context.Context, query oauthQueryer, rawID string) (oauthAuthorizationRequest, error) {
	request := oauthAuthorizationRequest{ID: rawID}
	if !strings.HasPrefix(rawID, "pc_ar_") || len(rawID) != 49 {
		return request, errors.New("invalid authorization request")
	}
	err := query.QueryRowContext(ctx, `SELECT r.client_id,k.name,r.redirect_uri,r.scope,r.resource,r.state,r.challenge,r.expires_at_ms
 FROM oauth_authorization_requests r JOIN access_keys k ON k.id=r.client_id
 WHERE r.request_hash=? AND r.consumed=0 AND r.expires_at_ms>? AND k.revoked_at IS NULL`, hash(rawID), s.clock().UnixMilli()).Scan(&request.ClientID, &request.ClientName, &request.RedirectURI, &request.Scope, &request.Resource, &request.State, &request.CodeChallenge, &request.ExpiresAtMS)
	return request, err
}

func oauthSingleQuery(r *http.Request) (url.Values, error) {
	if len(r.URL.RawQuery) > 16384 {
		return nil, errors.New("invalid authorization request")
	}
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return nil, err
	}
	for _, v := range values {
		if len(v) != 1 {
			return nil, errors.New("duplicate authorization parameter")
		}
	}
	return values, nil
}
func (s *Service) oauthAuthorize(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Referrer-Policy", "no-referrer")
	if !s.interactiveOAuthEnabled() {
		oauthTokenError(w, 503, "temporarily_unavailable")
		return
	}
	q, err := oauthSingleQuery(r)
	if err != nil {
		oauthTokenError(w, 400, "invalid_request")
		return
	}
	client, err := loadOAuthClient(r.Context(), s.db, q.Get("client_id"))
	if err != nil || !slices.Contains(client.GrantTypes, "authorization_code") || !slices.Contains(client.RedirectURIs, q.Get("redirect_uri")) {
		oauthTokenError(w, 400, "invalid_request")
		return
	}
	// Do not redirect an invalid request, even if it names a registered callback.
	if q.Get("response_type") != "code" || q.Get("code_challenge_method") != "S256" || len(q.Get("state")) > 2048 || strings.ContainsAny(q.Get("state"), "\r\n\x00") {
		oauthTokenError(w, 400, "invalid_request")
		return
	}
	challenge, err := base64.RawURLEncoding.DecodeString(q.Get("code_challenge"))
	if err != nil || len(challenge) != 32 || base64.RawURLEncoding.EncodeToString(challenge) != q.Get("code_challenge") {
		oauthTokenError(w, 400, "invalid_request")
		return
	}
	resource := q.Get("resource")
	if _, present := q["resource"]; present && resource == "" {
		oauthTokenError(w, 400, "invalid_target")
		return
	}
	if resource == "" {
		resource = s.oauth.mcpResource
	}
	if resource != s.oauth.mcpResource {
		oauthTokenError(w, 400, "invalid_target")
		return
	}
	requested := strings.Split(q.Get("scope"), " ")
	if _, present := q["scope"]; !present {
		requested = []string{}
		for _, permission := range interactiveOAuthScopes {
			if client.can(permission) {
				requested = append(requested, permission)
			}
		}
	}
	if len(requested) == 0 || len(q.Get("scope")) > 1024 {
		oauthTokenError(w, 400, "invalid_scope")
		return
	}
	for _, permission := range requested {
		if !slices.Contains(interactiveOAuthScopes, permission) || !client.can(permission) {
			oauthTokenError(w, 400, "invalid_scope")
			return
		}
	}
	slices.Sort(requested)
	requested = slices.Compact(requested)
	peerHash, allowed := s.allowOAuthAuthorization(r)
	if !allowed {
		w.Header().Set("Retry-After", "1")
		oauthTokenError(w, 429, "temporarily_unavailable")
		return
	}
	rawID, err := oauthRandom("pc_ar_")
	if err != nil {
		oauthTokenError(w, 503, "temporarily_unavailable")
		return
	}
	request := oauthAuthorizationRequest{ID: rawID, ClientID: client.ID, ClientName: clientName(r.Context(), s.db, client.ID), RedirectURI: q.Get("redirect_uri"), Scope: strings.Join(requested, " "), Resource: resource, State: q.Get("state"), CodeChallenge: q.Get("code_challenge"), ExpiresAtMS: s.clock().Add(oauthRequestTTL).UnixMilli()}
	tx, err := s.db.BeginTx(r.Context(), nil)
	if err != nil {
		oauthTokenError(w, 503, "temporarily_unavailable")
		return
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(r.Context(), `DELETE FROM oauth_authorization_requests WHERE expires_at_ms<=? OR consumed=1`, s.clock().UnixMilli()); err != nil {
		oauthTokenError(w, 503, "temporarily_unavailable")
		return
	}
	if err = s.pruneInteractiveOAuth(r.Context(), tx); err != nil {
		oauthTokenError(w, 503, "temporarily_unavailable")
		return
	}
	var count, perClient, perPeer int
	if tx.QueryRowContext(r.Context(), `SELECT count(*),count(*) FILTER(WHERE client_id=?),count(*) FILTER(WHERE peer_hash=?) FROM oauth_authorization_requests`, client.ID, peerHash).Scan(&count, &perClient, &perPeer) != nil || count >= 1000 || perClient >= 64 || perPeer >= 8 {
		w.Header().Set("Retry-After", "1")
		oauthTokenError(w, 429, "temporarily_unavailable")
		return
	}
	_, err = tx.ExecContext(r.Context(), `INSERT INTO oauth_authorization_requests(request_hash,client_id,redirect_uri,scope,resource,state,challenge,expires_at_ms,peer_hash) VALUES(?,?,?,?,?,?,?,?,?)`, hash(rawID), request.ClientID, request.RedirectURI, request.Scope, request.Resource, request.State, request.CodeChallenge, request.ExpiresAtMS, peerHash)
	if err != nil || tx.Commit() != nil {
		oauthTokenError(w, 503, "temporarily_unavailable")
		return
	}
	s.oauthHumanAuthorize(w, r, request)
}
func clientName(ctx context.Context, query oauthQueryer, id string) string {
	var name string
	_ = query.QueryRowContext(ctx, `SELECT name FROM access_keys WHERE id=?`, id).Scan(&name)
	return name
}

func (s *Service) issueOAuthAuthorizationCode(ctx context.Context, request oauthAuthorizationRequest, humanID string, approved actor) (string, error) {
	if !s.interactiveOAuthEnabled() {
		return "", errors.New("interactive OAuth unavailable")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `UPDATE metadata SET value=value WHERE key='initialized'`); err != nil {
		return "", err
	}
	current, err := s.loadOAuthRequest(ctx, tx, request.ID)
	if err != nil {
		return "", err
	}
	client, err := loadOAuthClient(ctx, tx, current.ClientID)
	if err != nil {
		return "", err
	}
	if !slices.Contains(client.GrantTypes, "authorization_code") || !slices.Contains(client.RedirectURIs, current.RedirectURI) {
		return "", errors.New("invalid authorization client")
	}
	human, err := s.loadOAuthHuman(ctx, tx, humanID)
	if err != nil {
		return "", err
	}
	if len(approved.Permissions) == 0 || len(approved.Projects) == 0 || len(approved.Projects) > 64 {
		return "", errors.New("invalid consent grants")
	}
	for _, permission := range approved.Permissions {
		if !slices.Contains(strings.Fields(current.Scope), permission) || !slices.Contains(interactiveOAuthScopes, permission) || !human.can(permission) || !client.can(permission) {
			return "", errors.New("invalid consent scope")
		}
	}
	for _, project := range approved.Projects {
		if project == "*" || project == "" || !authorize(human, "", project) || !authorize(client.actor, "", project) {
			return "", errors.New("invalid consent project")
		}
	}
	slices.Sort(approved.Permissions)
	approved.Permissions = slices.Compact(approved.Permissions)
	slices.Sort(approved.Projects)
	approved.Projects = slices.Compact(approved.Projects)
	var count, perHuman int
	if tx.QueryRowContext(ctx, `SELECT count(*),count(*) FILTER(WHERE g.human_id=?) FROM oauth_interactive_grants g JOIN access_keys k ON k.id=g.principal_id WHERE k.revoked_at IS NULL AND g.expires_at_ms>?`, human.ID, s.clock().UnixMilli()).Scan(&count, &perHuman) != nil || count >= 1000 || perHuman >= 64 {
		return "", errOAuthCapacity
	}
	grantID := newID()
	code, err := oauthRandom("pc_ac_")
	if err != nil {
		return "", err
	}
	unusable, err := oauthRandom("pc_ig_")
	if err != nil {
		return "", err
	}
	permissions, _ := json.Marshal(approved.Permissions)
	projects, _ := json.Marshal(approved.Projects)
	_, err = tx.ExecContext(ctx, `INSERT INTO access_keys(id,name,token_hash,permissions,projects,created_at) VALUES(?,?,?,?,?,?)`, grantID, "Interactive "+current.ClientName, hash(unusable), string(permissions), string(projects), now())
	if err != nil {
		return "", err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO oauth_interactive_grants(id,principal_id,human_id,client_id,created_at_ms,expires_at_ms) VALUES(?,?,?,?,?,?)`, grantID, grantID, human.ID, client.ID, s.clock().UnixMilli(), s.clock().Add(oauthRefreshTTL).UnixMilli())
	if err != nil {
		return "", err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO oauth_authorization_codes(code_hash,grant_id,client_id,redirect_uri,challenge,expires_at_ms) VALUES(?,?,?,?,?,?)`, hash(code), grantID, client.ID, current.RedirectURI, current.CodeChallenge, s.clock().Add(oauthCodeTTL).UnixMilli())
	if err != nil {
		return "", err
	}
	_, err = tx.ExecContext(ctx, `UPDATE oauth_authorization_requests SET consumed=1 WHERE request_hash=?`, hash(request.ID))
	if err != nil {
		return "", err
	}
	if tx.Commit() != nil {
		return "", errors.New("cannot persist authorization")
	}
	redirect, _ := url.Parse(current.RedirectURI)
	values := redirect.Query()
	values.Set("code", code)
	values.Set("iss", s.oauth.issuer)
	if current.State != "" {
		values.Set("state", current.State)
	}
	redirect.RawQuery = values.Encode()
	return redirect.String(), nil
}

func oauthPKCE(verifier, challenge string) bool {
	if len(verifier) < 43 || len(verifier) > 128 {
		return false
	}
	for _, c := range verifier {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("-._~", c)) {
			return false
		}
	}
	digest := sha256.Sum256([]byte(verifier))
	actual := base64.RawURLEncoding.EncodeToString(digest[:])
	return subtle.ConstantTimeCompare([]byte(actual), []byte(challenge)) == 1
}
