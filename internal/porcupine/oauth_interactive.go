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

	"github.com/olup/porcupine/internal/auth"
	eventapi "github.com/olup/porcupine/internal/event"
)

const oauthRequestTTL = 10 * time.Minute
const oauthCodeTTL = 5 * time.Minute
const oauthRefreshTTL = 7 * 24 * time.Hour

func (s *Service) initializeInteractiveOAuth(ctx context.Context) error {
	if s.shared != nil {
		return nil
	}
	return s.retireManualInvitations(ctx)
}

// Revoke the old principals and their dependent grants before discarding the
// one-use codes. Keeping the principal rows preserves grant and job audit IDs.
func (s *Service) retireManualInvitations(ctx context.Context) error {
	tx, err := s.control.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var exists bool
	if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='local_invitations')`).Scan(&exists); err != nil {
		return err
	}
	if exists {
		if _, err = tx.ExecContext(ctx, `UPDATE access_keys SET revoked_at=$1 WHERE revoked_at IS NULL AND id IN (SELECT principal_id FROM local_invitations)`, now()); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `DROP TABLE local_invitations`); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Service) loadOAuthAuthorizationRequest(ctx context.Context, rawID string) (oauthAuthorizationRequest, error) {
	return s.loadOAuthRequest(ctx, s.control, rawID)
}
func (s *Service) loadOAuthRequest(ctx context.Context, query oauthQueryer, rawID string) (oauthAuthorizationRequest, error) {
	request := oauthAuthorizationRequest{ID: rawID}
	if !strings.HasPrefix(rawID, "pc_ar_") || len(rawID) != 49 {
		return request, errors.New("invalid authorization request")
	}
	err := query.QueryRowContext(ctx, `SELECT r.client_id,k.name,r.redirect_uri,r.scope,r.resource,r.state,r.challenge,r.expires_at_ms
 FROM oauth_authorization_requests r JOIN access_keys k ON k.id=r.client_id
 WHERE r.request_hash=$1 AND r.consumed=0 AND r.expires_at_ms>$2 AND k.revoked_at IS NULL`, hash(rawID), s.clock().UnixMilli()).Scan(&request.ClientID, &request.ClientName, &request.RedirectURI, &request.Scope, &request.Resource, &request.State, &request.CodeChallenge, &request.ExpiresAtMS)
	return request, err
}

func (s *Service) oauthAuthorize(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Referrer-Policy", "no-referrer")
	if !s.interactiveOAuthEnabled() {
		oauthTokenError(w, 503, "temporarily_unavailable")
		return
	}
	q, err := auth.ReadOAuthQuery(r)
	if err != nil {
		oauthTokenError(w, 400, "invalid_request")
		return
	}
	client, err := loadOAuthClient(r.Context(), s.control, q.Get("client_id"))
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
		resource = s.resources.mcpResource
	}
	if !s.interactiveResourceAllowed(resource) {
		oauthTokenError(w, 400, "invalid_target")
		return
	}
	requested := strings.Split(q.Get("scope"), " ")
	if _, present := q["scope"]; !present {
		requested = []string{}
		for _, permission := range auth.InteractiveReadScopes() {
			if client.Principal.PermitsInteractiveScope(permission, client.GrantTypes) {
				requested = append(requested, permission)
			}
		}
	}
	if len(requested) == 0 || len(q.Get("scope")) > 1024 {
		oauthTokenError(w, 400, "invalid_scope")
		return
	}
	// Discovery recommends both read scopes. A read-only tool can be registered for
	// just one. The token and consent page report the subset the tool actually gets.
	if s.oauth.config.Email != nil && auth.OnlyInteractiveReadScopes(requested) {
		requested = slices.DeleteFunc(requested, func(permission string) bool {
			return !client.Principal.PermitsInteractiveScope(permission, client.GrantTypes)
		})
	}
	if len(requested) == 0 {
		oauthTokenError(w, 400, "invalid_scope")
		return
	}
	for _, permission := range requested {
		if !slices.Contains(auth.Permissions(), permission) || !client.Principal.PermitsInteractiveScope(permission, client.GrantTypes) || s.oauth.config.Email == nil && !slices.Contains(auth.InteractiveReadScopes(), permission) {
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
	rawID, err := auth.RandomSecret("pc_ar_")
	if err != nil {
		oauthTokenError(w, 503, "temporarily_unavailable")
		return
	}
	request := oauthAuthorizationRequest{ID: rawID, ClientID: client.ID, ClientName: clientName(r.Context(), s.control, client.ID), RedirectURI: q.Get("redirect_uri"), Scope: strings.Join(requested, " "), Resource: resource, State: q.Get("state"), CodeChallenge: q.Get("code_challenge"), ExpiresAtMS: s.clock().Add(oauthRequestTTL).UnixMilli()}
	tx, err := s.control.BeginTx(r.Context(), nil)
	if err != nil {
		oauthTokenError(w, 503, "temporarily_unavailable")
		return
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(r.Context(), `UPDATE metadata SET value=value WHERE key='initialized'`); err != nil {
		oauthTokenError(w, 503, "temporarily_unavailable")
		return
	}
	if _, err = tx.ExecContext(r.Context(), `DELETE FROM oauth_authorization_requests WHERE expires_at_ms<=$1 OR consumed=1`, s.clock().UnixMilli()); err != nil {
		oauthTokenError(w, 503, "temporarily_unavailable")
		return
	}
	if err = s.pruneInteractiveOAuth(r.Context(), tx); err != nil {
		oauthTokenError(w, 503, "temporarily_unavailable")
		return
	}
	var count, perClient, perPeer int
	if tx.QueryRowContext(r.Context(), `SELECT count(*),count(*) FILTER(WHERE client_id=$1),count(*) FILTER(WHERE peer_hash=$2) FROM oauth_authorization_requests`, client.ID, peerHash).Scan(&count, &perClient, &perPeer) != nil || count >= 1000 || perClient >= 64 || perPeer >= 8 {
		w.Header().Set("Retry-After", "1")
		oauthTokenError(w, 429, "temporarily_unavailable")
		return
	}
	_, err = tx.ExecContext(r.Context(), `INSERT INTO oauth_authorization_requests(request_hash,client_id,redirect_uri,scope,resource,state,challenge,expires_at_ms,peer_hash) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`, hash(rawID), request.ClientID, request.RedirectURI, request.Scope, request.Resource, request.State, request.CodeChallenge, request.ExpiresAtMS, peerHash)
	if err != nil || tx.Commit() != nil {
		oauthTokenError(w, 503, "temporarily_unavailable")
		return
	}
	s.oauth.browser.Start(w, r, auth.LoginRequest{ID: request.ID, ClientName: request.ClientName})
}
func clientName(ctx context.Context, query oauthQueryer, id string) string {
	var name string
	_ = query.QueryRowContext(ctx, `SELECT name FROM access_keys WHERE id=$1`, id).Scan(&name)
	return name
}

func (s *Service) issueOAuthAuthorizationCode(ctx context.Context, request oauthAuthorizationRequest, humanID string, proof auth.LoginProof, approved auth.Principal) (string, error) {
	if !s.interactiveOAuthEnabled() {
		return "", errors.New("interactive OAuth unavailable")
	}
	tx, err := s.control.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `UPDATE metadata SET value=value WHERE key='initialized'`); err != nil {
		return "", err
	}
	if err := checkHumanLoginProof(ctx, tx, humanID, proof); err != nil {
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
	if !s.interactiveScopesAllowed(human, client, current.Resource, approved.Permissions) {
		return "", errors.New("invalid consent grants")
	}
	tenantID := ""
	if tenantAdminConsent(approved.Permissions) {
		if len(approved.TenantAdmins) != 1 || len(approved.Projects) != 0 {
			return "", errors.New("invalid tenant consent")
		}
		tenantID = approved.TenantAdmins[0]
		if _, err := s.tenantAdminActor(ctx, tx, human, tenantID); err != nil {
			return "", errors.New("tenant administration is not granted")
		}
	} else if !auth.InteractiveProjectsAllowed(human, client.Principal, approved.Permissions, approved.Projects) {
		return "", errors.New("invalid consent grants")
	}
	requested := strings.Fields(current.Scope)
	for _, permission := range approved.Permissions {
		if !slices.Contains(requested, permission) {
			return "", errors.New("invalid consent scope")
		}
	}
	slices.Sort(approved.Permissions)
	approved.Permissions = slices.Compact(approved.Permissions)
	slices.Sort(approved.Projects)
	approved.Projects = slices.Compact(approved.Projects)
	var count, perHuman int
	if tx.QueryRowContext(ctx, `SELECT count(*),count(*) FILTER(WHERE g.human_id=$1) FROM oauth_interactive_grants g JOIN access_keys k ON k.id=g.principal_id WHERE k.revoked_at IS NULL AND g.expires_at_ms>$2`, human.ID, s.clock().UnixMilli()).Scan(&count, &perHuman) != nil || count >= 1000 || perHuman >= 64 {
		return "", errOAuthCapacity
	}
	grantID := eventapi.NewID()
	code, err := auth.RandomSecret("pc_ac_")
	if err != nil {
		return "", err
	}
	unusable, err := auth.RandomSecret("pc_ig_")
	if err != nil {
		return "", err
	}
	permissions, _ := json.Marshal(approved.Permissions)
	projects, _ := json.Marshal(approved.Projects)
	_, err = tx.ExecContext(ctx, `INSERT INTO access_keys(id,name,token_hash,permissions,projects,created_at) VALUES($1,$2,$3,$4,$5,$6)`, grantID, "Interactive "+current.ClientName, hash(unusable), string(permissions), string(projects), now())
	if err != nil {
		return "", err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO oauth_interactive_grants(id,principal_id,human_id,client_id,resource,created_at_ms,expires_at_ms) VALUES($1,$2,$3,$4,$5,$6,$7)`, grantID, grantID, human.ID, client.ID, current.Resource, s.clock().UnixMilli(), s.clock().Add(oauthRefreshTTL).UnixMilli())
	if err != nil {
		return "", err
	}
	if tenantID != "" {
		if _, err := tx.ExecContext(ctx, `INSERT INTO oauth_grant_tenants(grant_id,tenant_id) VALUES($1,$2)`, grantID, tenantID); err != nil {
			return "", err
		}
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO oauth_authorization_codes(code_hash,grant_id,client_id,redirect_uri,challenge,expires_at_ms) VALUES($1,$2,$3,$4,$5,$6)`, hash(code), grantID, client.ID, current.RedirectURI, current.CodeChallenge, s.clock().Add(oauthCodeTTL).UnixMilli())
	if err != nil {
		return "", err
	}
	_, err = tx.ExecContext(ctx, `UPDATE oauth_authorization_requests SET consumed=1 WHERE request_hash=$1`, hash(request.ID))
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
