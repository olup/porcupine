package porcupine

import (
	"bytes"
	"github.com/danielgtaylor/huma/v2"
	"go/ast"
	"go/format"
	"go/parser"
	gotoken "go/token"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/olup/porcupine/internal/auth"
)

func TestEveryManagementOperationRejectsMissingInvalidAndInsufficientCredentials(t *testing.T) {
	t.Parallel()
	j := newEmailJourney(t)
	s := j.service
	operator := oauthProtocolRequest(t, s, "bootstrap", adminToken, url.Values{"grant_type": {"client_credentials"}, "resource": {j.issuer + "/api/v1"}, "scope": {"instance:admin"}}, 200)["access_token"].(string)
	project := request(t, s, "POST", "/api/v1/projects", operator, `{"name":"policy coverage"}`, 201)["project"].(map[string]any)["id"].(string)
	client := request(t, s, "POST", "/api/v1/oauth-clients", operator, tenantTestBody(t, map[string]any{"name": "insufficient", "permissions": []string{"pipelines:read"}, "projects": []string{project}}), 201)
	insufficient := oauthProtocolRequest(t, s, client["client_id"].(string), client["client_secret"].(string), url.Values{"grant_type": {"client_credentials"}, "resource": {j.issuer + "/api/v1"}}, 200)["access_token"].(string)
	api := s.registerManagement(http.NewServeMux())
	count, httpOnly := 0, 0
	for path, item := range api.OpenAPI().Paths {
		if !strings.HasPrefix(path, "/api/v1/") {
			continue
		}
		for _, op := range []*huma.Operation{item.Get, item.Post, item.Put, item.Patch, item.Delete} {
			if op == nil {
				continue
			}
			count++
			managementOperationAccess(op)
			if len(op.Security) == 0 {
				t.Fatalf("%s has no published authentication requirement", op.OperationID)
			}
			if op.Metadata["http_only"] == true {
				httpOnly++
			}
			target := path
			for _, parameter := range op.Parameters {
				if parameter.In == "path" {
					target = strings.ReplaceAll(target, "{"+parameter.Name+"}", "policy-test-id")
				}
			}
			for _, tc := range []struct {
				credential string
				want       int
			}{{"", 401}, {"pc_at_invalid", 401}, {insufficient, 403}} {
				r := httptest.NewRequest(op.Method, target, strings.NewReader(`{}`))
				r.Header.Set("Content-Type", "application/json")
				if tc.credential != "" {
					r.Header.Set("Authorization", "Bearer "+tc.credential)
				}
				w := httptest.NewRecorder()
				s.Handler().ServeHTTP(w, r)
				if w.Code != tc.want {
					t.Fatalf("%s: missing/invalid/insufficient credential returned %d instead of %d: %s", op.OperationID, w.Code, tc.want, w.Body.String())
				}
			}
		}
	}
	if count < 50 || httpOnly == 0 {
		t.Fatalf("incomplete policy inventory: %d operations, %d HTTP-only", count, httpOnly)
	}
}

func TestManagementPolicyValidationIncludesHTTPOnlyRoutes(t *testing.T) {
	t.Parallel()
	for i, access := range []any{nil, auth.OperationAccess{}, auth.OperationAccess{Permission: "not-a-permission"}, auth.OperationAccess{Permission: "events:read", ProjectParameter: "missing"}, auth.OperationAccess{Permission: "management:read", TenantParameter: "tenant_id"}} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			op := &huma.Operation{OperationID: "test-http-only", Method: "GET", Path: "/api/v1/tenants/{tenant_id}", Metadata: map[string]any{"http_only": true, "access": access}}
			defer func() {
				if recover() == nil {
					t.Fatal("unprotected HTTP-only operation was accepted")
				}
			}()
			managementOperations(&huma.OpenAPI{Paths: map[string]*huma.PathItem{op.Path: {Get: op}}})
		})
	}
}

// Non-Huma endpoints use protocol-specific guards. A new registration must be
// classified here instead of silently bypassing the management policy inventory.
func TestRawHTTPRouteProtectionInventory(t *testing.T) {
	t.Parallel()
	policies := map[string]string{
		"GET /healthz":           "public-health",
		"GET /readyz":            "public-health",
		"GET /ingest/v1/schema":  "public-contract",
		"/mcp":                   "mcp-bearer",
		"POST /capture/":         "producer-credential",
		"POST /batch/":           "producer-credential",
		"POST /i/v0/e/":          "producer-credential",
		"POST /e/":               "producer-credential",
		"POST /ingest/v1/events": "producer-credential",
		`"POST " + path`:         "segment-producer-credential",
		"POST /oauth/token":      "oauth-token-exchange",
		"POST /oauth/revoke":     "oauth-token-revocation",
		"GET /oauth/authorize":   "oauth-authorization-request",
		"GET /.well-known/oauth-authorization-server":                "public-oauth-discovery",
		`"GET /.well-known/oauth-protected-resource" + entry.suffix`: "public-oauth-discovery",
		"POST /oauth/email/request":                                  "browser-session-origin-csrf",
		"POST /oauth/email/verify":                                   "browser-session-origin-csrf-otp",
		"POST /oauth/oidc/start":                                     "browser-session-origin-csrf",
		"GET /oauth/oidc/callback":                                   "oidc-state-pkce-nonce-verified-identity",
		"GET /oauth/consent":                                         "verified-browser-session",
		"POST /oauth/consent":                                        "verified-session-origin-csrf-current-authority",
	}
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		set := gotoken.NewFileSet()
		file, err := parser.ParseFile(set, entry.Name(), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok || len(call.Args) != 2 {
				return true
			}
			method, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || method.Sel.Name != "Handle" && method.Sel.Name != "HandleFunc" {
				return true
			}
			var expression bytes.Buffer
			if err := format.Node(&expression, set, call.Args[0]); err != nil {
				t.Fatal(err)
			}
			pattern := expression.String()
			if literal, err := strconv.Unquote(pattern); err == nil {
				pattern = literal
			}
			if policies[pattern] == "" {
				t.Errorf("%s: raw HTTP route %s has no explicit protection classification", set.Position(call.Pos()), pattern)
			}
			seen[pattern] = true
			return true
		})
	}
	for pattern := range policies {
		if !seen[pattern] {
			t.Errorf("stale HTTP protection inventory entry: %s", pattern)
		}
	}
}
