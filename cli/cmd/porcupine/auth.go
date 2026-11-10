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
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/gofrs/flock"
)

const (
	localCallbackAddr = "127.0.0.1:43179"
	analyticsClient   = "porcupine-local"
	adminClient       = "porcupine-local-admin"
)

func oauthScope(client string) string {
	if client == adminClient {
		return "instance:admin"
	}
	return "events:read management:read"
}

type oauthSession struct {
	Origin  string    `json:"origin"`
	Client  string    `json:"client"`
	Access  string    `json:"access_token"`
	Refresh string    `json:"refresh_token"`
	Expires time.Time `json:"expires_at"`
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
		return "", errors.New("no OAuth session for this instance; run porcupine login")
	}
	if err != nil {
		return "", err
	}
	var s oauthSession
	if json.Unmarshal(raw, &s) != nil || s.Origin != origin ||
		(s.Client != analyticsClient && s.Client != adminClient) || s.Refresh == "" || s.Access == "" {
		return "", errors.New("invalid OAuth session for this instance; run porcupine login again")
	}
	if time.Now().Add(30 * time.Second).Before(s.Expires) {
		return s.Access, nil
	}
	next, err := exchangeToken(ctx, origin, s.Client, url.Values{"grant_type": {"refresh_token"}, "refresh_token": {s.Refresh}})
	if err != nil {
		return "", err
	}
	if err = saveSession(path, next); err != nil {
		return "", err
	}
	return next.Access, nil
}

func exchangeToken(ctx context.Context, origin, clientID string, form url.Values) (oauthSession, error) {
	resource := origin + "/api/v1"
	form.Set("resource", resource)
	form.Set("client_id", clientID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, origin+"/oauth/token", strings.NewReader(form.Encode()))
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
	allowed := strings.Fields(oauthScope(clientID))
	scopes := strings.Fields(token.Scope)
	if !strings.EqualFold(token.Type, "Bearer") || token.Access == "" || token.Refresh == "" ||
		token.Resource != resource || len(scopes) == 0 ||
		token.Seconds < 1 || token.Seconds > math.MaxInt64/int64(time.Second) {
		return oauthSession{}, errors.New("invalid OAuth token response")
	}
	for _, scope := range scopes {
		if !slices.Contains(allowed, scope) {
			return oauthSession{}, errors.New("unexpected OAuth scope")
		}
	}
	return oauthSession{Origin: origin, Client: clientID, Access: token.Access, Refresh: token.Refresh, Expires: time.Now().Add(time.Duration(token.Seconds) * time.Second)}, nil
}

func login(ctx context.Context, origin string, admin bool) error {
	path, err := sessionFile(origin)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp4", localCallbackAddr)
	if err != nil {
		return fmt.Errorf("open OAuth callback on %s: %w", localCallbackAddr, err)
	}
	return browserLogin(ctx, origin, admin, path, listener, func(address string) {
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

func browserLogin(ctx context.Context, origin string, admin bool, path string, listener net.Listener, open func(string)) error {
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
	clientID := analyticsClient
	if admin {
		clientID = adminClient
	}
	params := url.Values{
		"response_type": {"code"}, "client_id": {clientID}, "redirect_uri": {redirect},
		"scope": {oauthScope(clientID)}, "resource": {origin + "/api/v1"}, "state": {state},
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
			parseErr != nil || len(q["iss"]) != 1 || q.Get("iss") != origin ||
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
	open(origin + "/oauth/authorize?" + params.Encode())
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
		s, err := exchangeToken(ctx, origin, clientID, url.Values{"grant_type": {"authorization_code"}, "code": {code}, "redirect_uri": {redirect}, "code_verifier": {verifier}})
		if err != nil {
			return err
		}
		return saveSession(path, s)
	}
}
