package porcupine

import (
	"context"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"
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
	return err
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
	contentType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || contentType != "application/x-www-form-urlencoded" || r.URL.RawQuery != "" || r.URL.ForceQuery || (r.Header.Get("Content-Encoding") != "" && r.Header.Get("Content-Encoding") != "identity") {
		oauthTokenError(w, 400, "invalid_request")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var large *http.MaxBytesError
		if errors.As(err, &large) {
			oauthTokenError(w, 413, "invalid_request")
		} else {
			oauthTokenError(w, 400, "invalid_request")
		}
		return
	}
	form, err := url.ParseQuery(string(body))
	if err != nil {
		oauthTokenError(w, 400, "invalid_request")
		return
	}
	for _, values := range form {
		if len(values) != 1 {
			oauthTokenError(w, 400, "invalid_request")
			return
		}
	}
	for _, credential := range []string{"client_secret", "client_assertion", "client_assertion_type"} {
		if _, present := form[credential]; present {
			oauthTokenError(w, 400, "invalid_request")
			return
		}
	}
	if form.Get("grant_type") == "" {
		oauthTokenError(w, 400, "invalid_request")
		return
	}
	if form.Get("grant_type") != "client_credentials" {
		oauthTokenError(w, 400, "unsupported_grant_type")
		return
	}
	if len(r.Header.Values("Authorization")) != 1 {
		oauthTokenError(w, 401, "invalid_client")
		return
	}
	clientID, secret, ok := r.BasicAuth()
	if !ok {
		oauthTokenError(w, 401, "invalid_client")
		return
	}
	clientID, err = url.QueryUnescape(clientID)
	if err != nil {
		oauthTokenError(w, 401, "invalid_client")
		return
	}
	secret, err = url.QueryUnescape(secret)
	if err != nil || clientID == "" || secret == "" || len(clientID) > 256 || len(secret) > 4096 || strings.ContainsAny(clientID+secret, "\r\n\x00") {
		oauthTokenError(w, 401, "invalid_client")
		return
	}
	if values, present := form["client_id"]; present && values[0] != clientID {
		oauthTokenError(w, 400, "invalid_request")
		return
	}
	client, err := loadOAuthClient(r.Context(), s.db, clientID)
	if err != nil || !client.VerifyPassword(secret) {
		oauthTokenError(w, 401, "invalid_client")
		return
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
