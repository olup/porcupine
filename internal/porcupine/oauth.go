package porcupine

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"slices"
	"time"

	liboauth "github.com/go-oauth2/oauth2/v4"
	oautherrors "github.com/go-oauth2/oauth2/v4/errors"
	"github.com/go-oauth2/oauth2/v4/manage"
	"github.com/go-oauth2/oauth2/v4/server"
)

func (s *Service) initializeOAuth(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS oauth_tokens (
token_hash TEXT PRIMARY KEY,client_id TEXT NOT NULL REFERENCES access_keys(id),resource TEXT NOT NULL,scope TEXT NOT NULL,projects_json TEXT NOT NULL,
issued_at_ms INTEGER NOT NULL,expires_at_ms INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS oauth_tokens_client ON oauth_tokens(client_id);
CREATE INDEX IF NOT EXISTS oauth_tokens_expiry ON oauth_tokens(expires_at_ms);`)
	if err != nil {
		return err
	}
	if err = s.initializeInteractiveOAuth(ctx); err != nil {
		return err
	}
	if err = s.initializeOIDCHumans(ctx); err != nil {
		return err
	}
	if err = s.initializeEmailAccounts(ctx); err != nil {
		return err
	}
	return s.initializeLocalEmailClients(ctx)
}

func oauthTokenError(w http.ResponseWriter, status int, code string) {
	if code == "invalid_client" {
		w.Header().Set("WWW-Authenticate", `Basic realm="oauth/token"`)
	}
	write(w, status, map[string]string{"error": code})
}

func (s *Service) oauthToken(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Pragma", "no-cache")
	if s.oauth == nil {
		oauthTokenError(w, 503, "temporarily_unavailable")
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		oauthTokenError(w, 405, "invalid_request")
		return
	}
	form, status, code := readOAuthForm(w, r)
	if code != "" {
		oauthTokenError(w, status, code)
		return
	}
	grantType := form.Get("grant_type")
	if grantType == "" {
		oauthTokenError(w, 400, "invalid_request")
		return
	}
	if grantType != "client_credentials" && (!s.interactiveOAuthEnabled() || grantType != "authorization_code" && grantType != "refresh_token") {
		oauthTokenError(w, 400, "unsupported_grant_type")
		return
	}
	client, status, code := s.authenticateOAuthEndpoint(r, form)
	if code != "" {
		oauthTokenError(w, status, code)
		return
	}
	if !slices.Contains(client.GrantTypes, grantType) {
		oauthTokenError(w, 400, "unauthorized_client")
		return
	}
	if grantType != "client_credentials" {
		var response map[string]any
		var err error
		if grantType == "authorization_code" {
			response, err = s.exchangeInteractiveCode(r.Context(), client, form)
		} else {
			response, err = s.exchangeInteractiveRefresh(r.Context(), client, form)
		}
		if err != nil {
			if errors.Is(err, errOAuthCapacity) {
				w.Header().Set("Retry-After", "1")
				oauthTokenError(w, 429, "temporarily_unavailable")
			} else if errors.Is(err, errInteractiveTarget) {
				oauthTokenError(w, 400, "invalid_target")
			} else if errors.Is(err, errInteractiveGrant) {
				oauthTokenError(w, 400, "invalid_grant")
			} else {
				oauthTokenError(w, 503, "temporarily_unavailable")
			}
			return
		}
		write(w, 200, response)
		return
	}
	clientID, secret := form.Get("client_id"), form.Get("client_secret")
	if client.AuthMethod == "client_secret_basic" {
		clientID, secret, _ = r.BasicAuth()
		clientID, _ = url.QueryUnescape(clientID)
		secret, _ = url.QueryUnescape(secret)
	}
	resource := s.oauth.apiResource
	if values, present := form["resource"]; present {
		resource = values[0]
	}
	if resource != s.oauth.apiResource && resource != s.oauth.mcpResource {
		oauthTokenError(w, 400, "invalid_target")
		return
	}
	_, scopePresent := form["scope"]
	scope, err := oauthScope(client.actor, form.Get("scope"), scopePresent)
	if err != nil {
		oauthTokenError(w, 400, "invalid_scope")
		return
	}
	if !s.oauth.limiter.Allow() {
		w.Header().Set("Retry-After", "1")
		oauthTokenError(w, 429, "temporarily_unavailable")
		return
	}
	// RFC 6749 requires ignoring unknown parameters. Do not let those fields
	// reach library handlers for any other grant or redirect behavior.
	tokenForm := url.Values{"grant_type": {"client_credentials"}, "scope": {scope}}
	r.Form = tokenForm
	r.PostForm = tokenForm
	manager := manage.NewManager()
	manager.MapClientStorage(oauthClientStore{s: s})
	manager.MapTokenStorage(oauthTokenStore{s: s})
	manager.MapAccessGenerate(oauthOpaqueGenerator{s: s})
	manager.SetClientTokenCfg(&manage.Config{AccessTokenExp: time.Duration(s.oauth.config.AccessTokenLifetimeSeconds) * time.Second, IsGenerateRefresh: false})
	authorization := server.NewServer(&server.Config{TokenType: "Bearer", AllowedGrantTypes: []liboauth.GrantType{liboauth.ClientCredentials}}, manager)
	authorization.ClientInfoHandler = func(*http.Request) (string, string, error) { return clientID, secret, nil }
	grant, request, err := authorization.ValidationTokenRequest(r)
	if err != nil {
		oauthTokenError(w, 400, "invalid_request")
		return
	}
	ctx := context.WithValue(r.Context(), oauthIssueContextKey{}, oauthIssue{resource: resource})
	info, err := authorization.GetAccessToken(ctx, grant, request)
	if err != nil {
		switch {
		case errors.Is(err, oautherrors.ErrInvalidClient):
			oauthTokenError(w, 401, "invalid_client")
		case errors.Is(err, oautherrors.ErrInvalidScope):
			oauthTokenError(w, 400, "invalid_scope")
		case errors.Is(err, errOAuthCapacity):
			w.Header().Set("Retry-After", "1")
			oauthTokenError(w, 429, "temporarily_unavailable")
		default:
			oauthTokenError(w, 503, "temporarily_unavailable")
		}
		return
	}
	write(w, 200, map[string]any{"access_token": info.GetAccess(), "token_type": "Bearer", "expires_in": int64(info.GetAccessExpiresIn() / time.Second), "scope": info.GetScope(), "resource": resource})
}
