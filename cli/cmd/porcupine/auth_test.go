package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLocalLoginAndRotatingRefresh(t *testing.T) {
	for _, tc := range []struct{ name, client, scope string }{
		{"reader", analyticsClient, oauthScope(analyticsClient)},
		{"contributor", analyticsClient, "events:read events:write imports:write"},
		{"instance-admin", adminClient, oauthScope(adminClient)},
		{"tenant-admin", tenantAdminClient, oauthScope(tenantAdminClient)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clientID := tc.client
			scopes := strings.Fields(tc.scope)
			var challenge atomic.Value
			var refreshes atomic.Int32
			var origin string
			issuer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/oauth/authorize":
					q := r.URL.Query()
					if q.Get("client_id") != clientID || q.Get("scope") != tc.scope || q.Get("resource") != origin+"/api/v1" || q.Get("code_challenge_method") != "S256" || q.Get("response_type") != "code" || len(q.Get("state")) < 40 {
						t.Error("incorrect authorization parameters")
					}
					challenge.Store(q.Get("code_challenge"))
					v := url.Values{"state": {q.Get("state")}, "iss": {origin}, "code": {"test-code"}}
					for _, key := range []string{"state", "iss"} {
						original := v.Get(key)
						v.Set(key, "wrong")
						res, err := http.Get(q.Get("redirect_uri") + "?" + v.Encode())
						if err != nil {
							t.Error(err)
							return
						}
						res.Body.Close()
						if res.StatusCode != 400 {
							t.Error("accepted callback with invalid " + key)
						}
						v.Set(key, original)
					}
					http.Redirect(w, r, q.Get("redirect_uri")+"?"+v.Encode(), http.StatusFound)
				case "/oauth/token":
					if err := r.ParseForm(); err != nil {
						t.Error(err)
						return
					}
					if r.Method != "POST" || r.PostForm.Get("client_id") != clientID || r.PostForm.Get("resource") != origin+"/api/v1" || r.PostForm.Has("client_secret") {
						t.Error("incorrect token request")
					}
					access, refresh, expires := "issued", "refresh1", 900
					switch r.PostForm.Get("grant_type") {
					case "authorization_code":
						hash := sha256.Sum256([]byte(r.PostForm.Get("code_verifier")))
						if r.PostForm.Get("code") != "test-code" || challenge.Load() != base64.RawURLEncoding.EncodeToString(hash[:]) {
							t.Error("invalid PKCE")
						}
					case "refresh_token":
						if r.PostForm.Get("refresh_token") != "refresh1" || refreshes.Add(1) != 1 {
							t.Error("refresh token reused")
						}
						access, refresh = "renewed", "refresh2"
					default:
						t.Error("unexpected grant")
					}
					json.NewEncoder(w).Encode(map[string]any{"access_token": access, "refresh_token": refresh, "scope": tc.scope, "expires_in": expires, "token_type": "Bearer", "resource": origin + "/api/v1"})
				default:
					t.Error("unexpected request")
					http.NotFound(w, r)
				}
			}))
			defer issuer.Close()
			origin = issuer.URL
			listener, err := net.Listen("tcp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "session.json")
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			err = browserLogin(ctx, origin, clientID, scopes, path, listener, func(address string) {
				res, err := http.Get(address)
				if err != nil {
					t.Fatal(err)
				}
				res.Body.Close()
				if res.StatusCode != 200 {
					t.Fatal("callback returned", res.StatusCode)
				}
			})
			if err != nil {
				t.Fatal(err)
			}
			if runtime.GOOS != "windows" {
				st, err := os.Stat(path)
				if err != nil || st.Mode().Perm()&0077 != 0 {
					t.Fatal("unprotected token store")
				}
			}
			if got, err := storedAccessToken(ctx, origin, path); got != "issued" || err != nil {
				t.Fatal("fresh token was not reused:", err)
			}
			if _, err := storedAccessToken(ctx, "https://other.example", path); err == nil {
				t.Fatal("cross-origin access returned a token")
			}
			b, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var session oauthSession
			if err = json.Unmarshal(b, &session); err != nil {
				t.Fatal(err)
			}
			session.Expires = time.Now().Add(-time.Minute)
			if err = saveSession(path, session); err != nil {
				t.Fatal(err)
			}
			var wg sync.WaitGroup
			for range 4 {
				wg.Go(func() {
					if got, err := storedAccessToken(ctx, origin, path); err != nil || got != "renewed" {
						t.Error("expired token not renewed:", err)
					}
				})
			}
			wg.Wait()
			if refreshes.Load() != 1 {
				t.Fatal("wrong number of refreshes")
			}
			b, err = os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if json.Unmarshal(b, &session) != nil || session.Refresh != "refresh2" || session.Client != clientID || strings.Join(session.Scopes, " ") != tc.scope {
				t.Fatal("rotation was not persisted")
			}
		})
	}
}

func TestTokenEndpointRejectsUnsafeResponses(t *testing.T) {
	for _, kind := range []string{"wrong-resource", "expanded-scope", "expanded-project-scope", "expired", "invalid-grant", "redirect"} {
		t.Run(kind, func(t *testing.T) {
			leaked := false
			attacker := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { leaked = true }))
			defer attacker.Close()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				reply := map[string]any{"expires_in": 900, "token_type": "Bearer", "resource": "http://" + r.Host + "/api/v1", "scope": "events:read", "access_token": "secret-in-body", "refresh_token": "secret-in-body"}
				switch kind {
				case "wrong-resource":
					reply["resource"] = "https://other.example/api/v1"
				case "expanded-scope":
					reply["scope"] = "instance:admin"
				case "expanded-project-scope":
					reply["scope"] = "events:read events:write"
				case "expired":
					reply["expires_in"] = 0
				case "invalid-grant":
					http.Error(w, "secret-in-body", 400)
					return
				case "redirect":
					http.Redirect(w, r, attacker.URL, 307)
					return
				}
				json.NewEncoder(w).Encode(reply)
			}))
			defer server.Close()
			_, err := exchangeToken(context.Background(), server.URL, analyticsClient, url.Values{"grant_type": {"refresh_token"}, "refresh_token": {"secret-in-body"}}, strings.Fields(oauthScope(analyticsClient)))
			if err == nil || strings.Contains(err.Error(), "secret-in-body") || leaked {
				t.Fatal("unexpected token response accepted or secret disclosed")
			}
		})
	}
}

func TestLoginRejectsCombinedAdministratorModes(t *testing.T) {
	_, err := invoke(t, "http://127.0.0.1:8080", "login", "--admin", "--tenant-admin")
	if err == nil || !strings.Contains(err.Error(), "cannot combine") {
		t.Fatalf("combined administrator modes were not rejected: %v", err)
	}
}

func TestProjectLoginScopeSelection(t *testing.T) {
	for _, raw := range []string{"events:write,events:read,imports:write", "events:write events:read imports:write events:read"} {
		client, scopes, err := loginOptions(false, false, raw)
		if err != nil || client != analyticsClient || strings.Join(scopes, " ") != "events:read events:write imports:write" {
			t.Fatalf("project scope selection failed: %s %v %v", client, scopes, err)
		}
	}
	for _, raw := range []string{"instance:admin", "tenant:admin", "events:read tenant:admin", "events:read?x=y", " ", ",,"} {
		if _, _, err := loginOptions(false, false, raw); err == nil {
			t.Fatalf("unsafe project login scope accepted: %q", raw)
		}
	}
	for _, mode := range []string{"--admin", "--tenant-admin"} {
		if _, err := invoke(t, "http://127.0.0.1:8080", "login", mode, "--scopes", "events:read"); err == nil || !strings.Contains(err.Error(), "cannot combine") {
			t.Fatalf("combined scope and role login accepted: %v", err)
		}
	}
}
