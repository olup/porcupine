package main

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestExternalOAuthDiscoveryAndAccessOnlyLogin(t *testing.T) {
	t.Parallel()
	var issuer, origin string
	authorization := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/.well-known/oauth-authorization-server/identity":
			json.NewEncoder(w).Encode(map[string]any{"issuer": issuer, "authorization_endpoint": issuer + "/authorize", "token_endpoint": issuer + "/token", "response_types_supported": []string{"code"}, "code_challenge_methods_supported": []string{"S256"}, "authorization_response_iss_parameter_supported": true})
		case "/identity/authorize":
			q := r.URL.Query()
			if q.Get("resource") != origin+"/api/v1" || q.Get("client_id") != "registered-cli" || q.Get("scope") != "events:read" || q.Get("code_challenge_method") != "S256" {
				t.Error("incorrect external authorization request")
			}
			callback := url.Values{"state": {q.Get("state")}, "iss": {issuer}, "code": {"external-code"}}
			http.Redirect(w, r, q.Get("redirect_uri")+"?"+callback.Encode(), http.StatusFound)
		case "/identity/token":
			r.ParseForm()
			if r.PostForm.Get("resource") != origin+"/api/v1" || r.PostForm.Get("client_id") != "registered-cli" || r.PostForm.Get("code") != "external-code" || r.PostForm.Get("code_verifier") == "" || r.PostForm.Has("client_secret") {
				t.Error("incorrect external token request")
			}
			json.NewEncoder(w).Encode(map[string]any{"access_token": "short-lived-api-jwt", "token_type": "Bearer", "expires_in": 300})
		default:
			t.Errorf("unexpected authorization request: %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer authorization.Close()
	issuer = authorization.URL + "/identity"
	resource := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/.well-known/oauth-protected-resource/api/v1" {
			t.Error("requested a builtin OAuth route on external resource server")
		}
		json.NewEncoder(w).Encode(map[string]any{"resource": origin + "/api/v1", "authorization_servers": []string{issuer}, "bearer_methods_supported": []string{"header"}})
	}))
	defer resource.Close()
	origin = resource.URL
	provider, err := discoverProvider(context.Background(), origin)
	if err != nil || !provider.External || provider.Issuer != issuer {
		t.Fatalf("external discovery failed: %+v %v", provider, err)
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "session.json")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = browserProviderLogin(ctx, origin, provider, "registered-cli", []string{"events:read"}, path, listener, func(address string) {
		response, err := http.Get(address)
		if err != nil {
			t.Error(err)
			return
		}
		response.Body.Close()
	})
	if err != nil {
		t.Fatal(err)
	}
	if access, err := storedAccessToken(ctx, origin, path); err != nil || access != "short-lived-api-jwt" {
		t.Fatalf("access-only session failed: %s %v", access, err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var session oauthSession
	json.Unmarshal(data, &session)
	if session.Provider == nil || session.Provider.Issuer != issuer || session.Refresh != "" {
		t.Fatal("external authority was not bound to the saved session")
	}
	session.Expires = time.Now().Add(-time.Second)
	if err := saveSession(path, session); err != nil {
		t.Fatal(err)
	}
	if _, err := storedAccessToken(ctx, origin, path); err == nil || !strings.Contains(err.Error(), "session expired") {
		t.Fatalf("expired access-only session accepted: %v", err)
	}
}

func TestExternalOAuthMetadataRejectsConfusion(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"mcp-resource", "multiple-issuers", "wrong-issuer", "foreign-token-endpoint", "no-pkce", "no-callback-issuer", "redirect"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			var origin string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasPrefix(r.URL.Path, "/.well-known/oauth-protected-resource") {
					resource := origin + "/api/v1"
					issuers := []string{origin}
					if kind == "mcp-resource" {
						resource = origin + "/mcp"
					}
					if kind == "multiple-issuers" {
						issuers = append(issuers, "https://other.example")
					}
					json.NewEncoder(w).Encode(map[string]any{"resource": resource, "authorization_servers": issuers, "bearer_methods_supported": []string{"header"}})
					return
				}
				metadata := map[string]any{"issuer": origin, "authorization_endpoint": origin + "/authorize", "token_endpoint": origin + "/token", "response_types_supported": []string{"code"}, "code_challenge_methods_supported": []string{"S256"}, "authorization_response_iss_parameter_supported": true}
				switch kind {
				case "wrong-issuer":
					metadata["issuer"] = "https://other.example"
				case "foreign-token-endpoint":
					metadata["token_endpoint"] = "https://other.example/token"
				case "no-pkce":
					metadata["code_challenge_methods_supported"] = []string{"plain"}
				case "no-callback-issuer":
					metadata["authorization_response_iss_parameter_supported"] = false
				case "redirect":
					http.Redirect(w, r, "https://other.example", http.StatusFound)
					return
				}
				json.NewEncoder(w).Encode(metadata)
			}))
			defer server.Close()
			origin = server.URL
			if _, err := discoverProvider(context.Background(), origin); err == nil {
				t.Fatal("unsafe OAuth metadata accepted")
			}
		})
	}
}

func TestExternalOAuthRefreshPreservesEndpointAndScope(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"refresh", "expanded-scope", "mcp-resource"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			var issuer string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/external/token" {
					t.Error("refresh used the wrong endpoint")
				}
				r.ParseForm()
				if r.PostForm.Get("resource") != "https://porcupine.example/api/v1" || r.PostForm.Get("refresh_token") != "rotation-token" || r.PostForm.Get("client_id") != "registered-cli" {
					t.Error("refresh changed its resource or client")
				}
				response := map[string]any{"access_token": "next", "expires_in": 300, "token_type": "Bearer"}
				if kind == "expanded-scope" {
					response["scope"] = "events:read events:write"
				}
				if kind == "mcp-resource" {
					response["resource"] = "https://porcupine.example/mcp"
				}
				json.NewEncoder(w).Encode(response)
			}))
			defer server.Close()
			issuer = server.URL
			provider := oauthProvider{Issuer: issuer, AuthorizationEndpoint: issuer + "/external/authorize", TokenEndpoint: issuer + "/external/token", External: true}
			session, err := exchangeProviderToken(context.Background(), "https://porcupine.example", provider, "registered-cli", url.Values{"grant_type": {"refresh_token"}, "refresh_token": {"rotation-token"}}, []string{"events:read"})
			if kind == "refresh" {
				if err != nil || session.Refresh != "rotation-token" || session.Provider == nil || session.Provider.TokenEndpoint != provider.TokenEndpoint {
					t.Fatalf("refresh failed: %+v %v", session, err)
				}
			} else if err == nil {
				t.Fatal("unsafe refreshed token accepted")
			}
		})
	}
}

func TestOAuthDiscoveryBuiltinAndOIDCFallback(t *testing.T) {
	t.Parallel()
	for _, external := range []bool{false, true} {
		t.Run(map[bool]string{false: "builtin", true: "oidc"}[external], func(t *testing.T) {
			t.Parallel()
			var origin string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				issuer := origin
				if external {
					issuer += "/realm"
				}
				switch r.URL.Path {
				case "/.well-known/oauth-protected-resource/api/v1":
					json.NewEncoder(w).Encode(map[string]any{"resource": origin + "/api/v1", "authorization_servers": []string{issuer}, "bearer_methods_supported": []string{"header"}})
				case "/.well-known/oauth-authorization-server/realm":
					http.NotFound(w, r)
				case "/.well-known/oauth-authorization-server", "/realm/.well-known/openid-configuration":
					json.NewEncoder(w).Encode(map[string]any{"issuer": issuer, "authorization_endpoint": origin + "/oauth/authorize", "token_endpoint": origin + "/oauth/token", "response_types_supported": []string{"code"}, "code_challenge_methods_supported": []string{"S256"}, "authorization_response_iss_parameter_supported": true})
				default:
					t.Errorf("unexpected metadata path: %s", r.URL.Path)
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			origin = server.URL
			provider, err := discoverProvider(context.Background(), origin)
			if err != nil || provider.External != external {
				t.Fatalf("unexpected provider: %+v %v", provider, err)
			}
		})
	}
}

func TestExternalOAuthClientNamesDoNotSelectPermissions(t *testing.T) {
	t.Parallel()
	provider := oauthProvider{External: true}
	for _, client := range []string{analyticsClient, adminClient, tenantAdminClient, "registered-cli"} {
		for _, scopes := range [][]string{{"events:read"}, {"instance:admin"}, {"tenant:admin"}} {
			if !validProviderScopes(provider, client, scopes) {
				t.Fatalf("client name changed external scope selection: %s %v", client, scopes)
			}
		}
	}
	for _, scopes := range [][]string{{"events:read", "instance:admin"}, {"tenant:admin", "instance:admin"}, {""}, nil} {
		if validProviderScopes(provider, "registered-cli", scopes) {
			t.Fatalf("invalid external scopes accepted: %v", scopes)
		}
	}
	if validProviderScopes(oauthProvider{}, "registered-cli", []string{"events:read"}) {
		t.Fatal("external client accepted by builtin session validation")
	}
}
