package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"time"
	"unicode"

	"github.com/gofrs/flock"
)

const (
	localCallbackAddr = "127.0.0.1:43179"
	analyticsClient   = "porcupine-local"
	adminClient       = "porcupine-local-admin"
	tenantAdminClient = "porcupine-local-tenant-admin"
	cloudOAuthClient  = "porcupine-cloud-cli"
)

func oauthScope(client string) string {
	if client == tenantAdminClient {
		return "tenant:admin"
	}
	if client == adminClient {
		return "instance:admin"
	}
	return "events:read management:read"
}

var loginScopeName = regexp.MustCompile(`^[a-z][a-z0-9_]*:[a-z][a-z0-9_]*$`)

func projectLoginScopes(raw string) ([]string, error) {
	scopes := strings.FieldsFunc(raw, func(r rune) bool { return r == ',' || unicode.IsSpace(r) })
	if len(scopes) == 0 || len(scopes) > 32 {
		return nil, errors.New("request 1 to 32 project scopes")
	}
	for _, scope := range scopes {
		if len(scope) > 64 || !loginScopeName.MatchString(scope) || strings.HasPrefix(scope, "instance:") || strings.HasPrefix(scope, "tenant:") {
			return nil, errors.New("--scopes accepts project scopes only; use --admin or --tenant-admin for administration")
		}
	}
	slices.Sort(scopes)
	return slices.Compact(scopes), nil
}

func loginOptions(admin, tenantAdmin bool, raw string) (string, []string, error) {
	if admin && tenantAdmin || raw != "" && (admin || tenantAdmin) {
		return "", nil, errors.New("cannot combine --admin, --tenant-admin, or --scopes")
	}
	client := analyticsClient
	if admin {
		client = adminClient
	} else if tenantAdmin {
		client = tenantAdminClient
	}
	if raw == "" {
		return client, strings.Fields(oauthScope(client)), nil
	}
	scopes, err := projectLoginScopes(raw)
	return client, scopes, err
}

func validSessionScopes(client string, scopes []string) bool {
	switch client {
	case adminClient, tenantAdminClient:
		return slices.Equal(scopes, strings.Fields(oauthScope(client)))
	case analyticsClient:
		_, err := projectLoginScopes(strings.Join(scopes, " "))
		return err == nil
	default:
		return false
	}
}

func validProviderScopes(provider oauthProvider, client string, scopes []string) bool {
	if !provider.External {
		return validSessionScopes(client, scopes)
	}
	if client == "" || len(client) > 256 {
		return false
	}
	if slices.Equal(scopes, []string{"instance:admin"}) || slices.Equal(scopes, []string{"tenant:admin"}) {
		return true
	}
	_, err := projectLoginScopes(strings.Join(scopes, " "))
	return err == nil
}

type oauthSession struct {
	Origin   string         `json:"origin"`
	Client   string         `json:"client"`
	Access   string         `json:"access_token"`
	Refresh  string         `json:"refresh_token"`
	Expires  time.Time      `json:"expires_at"`
	Scopes   []string       `json:"scopes"`
	Provider *oauthProvider `json:"provider,omitempty"`
}

func sessionFile(origin string) (string, error) {
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	dir = filepath.Join(dir, "porcupine", "credentials")
	if err = os.MkdirAll(dir, 0700); err != nil {
		return "", err
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return "", err
	}
	if !info.IsDir() || (runtime.GOOS != "windows" && info.Mode().Perm()&0077 != 0) {
		return "", fmt.Errorf("OAuth credentials need a private directory: %s", dir)
	}
	return filepath.Join(dir, fmt.Sprintf("%x.json", sha256.Sum256([]byte(origin)))), nil
}

func lockSession(ctx context.Context, path string) (func(), error) {
	lock := flock.New(path + ".lock")
	wait, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	held, err := lock.TryLockContext(wait, 100*time.Millisecond)
	if err != nil {
		return nil, fmt.Errorf("lock OAuth session: %w", err)
	}
	if !held {
		return nil, errors.New("OAuth session is busy")
	}
	return func() { _ = lock.Unlock() }, nil
}

func saveSession(path string, s oauthSession) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".session-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if err = json.NewEncoder(f).Encode(s); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	return os.Rename(f.Name(), path)
}

func accessToken(ctx context.Context, origin string) (string, error) {
	path, err := sessionFile(origin)
	if err != nil {
		return "", err
	}
	return storedAccessToken(ctx, origin, path)
}

func storedAccessToken(ctx context.Context, origin, path string) (string, error) {
	unlock, err := lockSession(ctx, path)
	if err != nil {
		return "", err
	}
	defer unlock()
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("no OAuth session for %s. Run porcupine login for that instance", origin)
	}
	if err != nil {
		return "", err
	}
	var s oauthSession
	if json.Unmarshal(raw, &s) != nil || s.Origin != origin || s.Access == "" {
		return "", errors.New("invalid OAuth session for this instance; run porcupine login again")
	}
	provider := builtinProvider(origin)
	if s.Provider != nil {
		provider = *s.Provider
		if err := provider.validate(); err != nil {
			return "", errors.New("invalid OAuth provider in session; run porcupine login again")
		}
	}
	if !validProviderScopes(provider, s.Client, s.Scopes) {
		return "", errors.New("invalid OAuth session scopes; run porcupine login again")
	}
	if !provider.External && s.Refresh == "" {
		return "", errors.New("invalid OAuth session for this instance; run porcupine login again")
	}
	if time.Now().Add(30 * time.Second).Before(s.Expires) {
		return s.Access, nil
	}
	if s.Refresh == "" {
		return "", errors.New("OAuth session expired; run porcupine login again")
	}
	next, err := exchangeProviderToken(ctx, origin, provider, s.Client, url.Values{"grant_type": {"refresh_token"}, "refresh_token": {s.Refresh}}, s.Scopes)
	if err != nil {
		return "", err
	}
	if err = saveSession(path, next); err != nil {
		return "", err
	}
	return next.Access, nil
}

func exchangeProviderToken(ctx context.Context, origin string, provider oauthProvider, clientID string, form url.Values, allowed []string) (oauthSession, error) {
	if !validProviderScopes(provider, clientID, allowed) {
		return oauthSession{}, errors.New("invalid requested OAuth scopes")
	}
	resource := origin + "/api/v1"
	form.Set("resource", resource)
	form.Set("client_id", clientID)
	if err := provider.validate(); err != nil {
		return oauthSession{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, provider.TokenEndpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return oauthSession{}, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	client := http.Client{Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	res, err := client.Do(req)
	if err != nil {
		return oauthSession{}, errors.New("could not reach the OAuth token endpoint")
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		if res.StatusCode == 400 || res.StatusCode == 401 {
			return oauthSession{}, fmt.Errorf("OAuth grant rejected (HTTP %d); run porcupine login again", res.StatusCode)
		}
		return oauthSession{}, fmt.Errorf("OAuth token endpoint returned HTTP %d", res.StatusCode)
	}
	var token struct {
		Access   string `json:"access_token"`
		Refresh  string `json:"refresh_token"`
		Type     string `json:"token_type"`
		Seconds  int64  `json:"expires_in"`
		Scope    string `json:"scope"`
		Resource string `json:"resource"`
	}
	if err = json.NewDecoder(io.LimitReader(res.Body, 64<<10)).Decode(&token); err != nil {
		return oauthSession{}, errors.New("invalid OAuth token response")
	}
	scopes := strings.Fields(token.Scope)
	if provider.External && token.Scope == "" {
		scopes = slices.Clone(allowed)
	}
	if provider.External && form.Get("grant_type") == "refresh_token" && token.Refresh == "" {
		token.Refresh = form.Get("refresh_token")
	}
	if !strings.EqualFold(token.Type, "Bearer") || token.Access == "" || !provider.External && token.Refresh == "" ||
		(token.Resource != resource && !(provider.External && token.Resource == "")) || len(scopes) == 0 ||
		token.Seconds < 1 || token.Seconds > math.MaxInt64/int64(time.Second) {
		return oauthSession{}, errors.New("invalid OAuth token response")
	}
	for _, scope := range scopes {
		if !slices.Contains(allowed, scope) {
			return oauthSession{}, errors.New("unexpected OAuth scope")
		}
	}
	slices.Sort(scopes)
	scopes = slices.Compact(scopes)
	session := oauthSession{Origin: origin, Client: clientID, Access: token.Access, Refresh: token.Refresh, Expires: time.Now().Add(time.Duration(token.Seconds) * time.Second), Scopes: scopes}
	if provider.External {
		session.Provider = &provider
	}
	return session, nil
}

func loginClientID(origin string, provider oauthProvider, builtinClient, configuredClient string) (string, error) {
	if configuredClient != "" {
		return configuredClient, nil
	}
	if !provider.External {
		return builtinClient, nil
	}
	if origin == cloudOrigin {
		if builtinClient == adminClient {
			return "", errors.New("the Porcupine Cloud public CLI client supports tenant administration with --tenant-admin. Select your own instance with --url for self-hosted administration")
		}
		return cloudOAuthClient, nil
	}
	return "", errors.New("this instance uses an external identity provider. Register a public PKCE client and pass --client-id")
}

func login(ctx context.Context, origin, builtinClient string, scopes []string, configuredClient string) error {
	provider, err := discoverProvider(ctx, origin)
	if err != nil {
		return err
	}
	clientID, err := loginClientID(origin, provider, builtinClient, configuredClient)
	if err != nil {
		return err
	}
	path, err := sessionFile(origin)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp4", localCallbackAddr)
	if err != nil {
		return fmt.Errorf("open OAuth callback on %s: %w", localCallbackAddr, err)
	}
	return browserProviderLogin(ctx, origin, provider, clientID, scopes, path, listener, func(address string) {
		fmt.Fprintln(os.Stderr, "Open this URL to authorize Porcupine:")
		fmt.Fprintln(os.Stderr, address)
		openLoginBrowser(address)
	})
}

func openLoginBrowser(address string) {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", address)
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", address)
	default:
		cmd = exec.Command("xdg-open", address)
	}
	if cmd.Start() == nil {
		go cmd.Wait()
	}
}

func oauthRandom() (string, error) {
	var bytes [32]byte
	_, err := rand.Read(bytes[:])
	return base64.RawURLEncoding.EncodeToString(bytes[:]), err
}

func browserProviderLogin(ctx context.Context, origin string, provider oauthProvider, clientID string, scopes []string, path string, listener net.Listener, open func(string)) error {
	defer listener.Close()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	state, err := oauthRandom()
	if err != nil {
		return err
	}
	verifier, err := oauthRandom()
	if err != nil {
		return err
	}
	hash := sha256.Sum256([]byte(verifier))
	callbackHost := listener.Addr().String()
	redirect := "http://" + callbackHost + "/callback"
	params := url.Values{
		"response_type": {"code"}, "client_id": {clientID}, "redirect_uri": {redirect},
		"scope": {strings.Join(scopes, " ")}, "resource": {origin + "/api/v1"}, "state": {state},
		"code_challenge_method": {"S256"}, "code_challenge": {base64.RawURLEncoding.EncodeToString(hash[:])},
	}
	result := make(chan string, 1)
	server := &http.Server{ReadHeaderTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 5 * time.Second, MaxHeaderBytes: 8192}
	server.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		q, parseErr := url.ParseQuery(r.URL.RawQuery)
		if r.Method != http.MethodGet || r.Host != callbackHost || r.URL.Path != "/callback" ||
			parseErr != nil || len(q["iss"]) != 1 || q.Get("iss") != provider.Issuer ||
			len(q["state"]) != 1 || subtle.ConstantTimeCompare([]byte(q.Get("state")), []byte(state)) != 1 {
			http.Error(w, "Invalid OAuth callback.", http.StatusBadRequest)
			return
		}
		code := q.Get("code")
		if q.Has("error") {
			code = ""
		} else if code == "" || len(q["code"]) != 1 {
			http.Error(w, "Missing authorization code.", http.StatusBadRequest)
			return
		}
		select {
		case result <- code:
			fmt.Fprintln(w, "Return to the terminal to see the login result.")
		default:
			http.Error(w, "This callback was already received.", http.StatusConflict)
		}
	})
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(listener) }()
	defer func() {
		stop, cancelStop := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancelStop()
		_ = server.Shutdown(stop)
		_ = server.Close()
	}()
	open(provider.AuthorizationEndpoint + "?" + params.Encode())
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err = <-serveErr:
		return err
	case code := <-result:
		if code == "" {
			return errors.New("OAuth authorization declined")
		}
		unlock, err := lockSession(ctx, path)
		if err != nil {
			return err
		}
		defer unlock()
		s, err := exchangeProviderToken(ctx, origin, provider, clientID, url.Values{"grant_type": {"authorization_code"}, "code": {code}, "redirect_uri": {redirect}, "code_verifier": {verifier}}, scopes)
		if err != nil {
			return err
		}
		return saveSession(path, s)
	}
}
