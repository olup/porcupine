package main

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func invoke(t *testing.T, origin string, args ...string) (string, error) {
	t.Helper()
	var output bytes.Buffer
	app := newApp()
	app.Writer = &output
	app.ErrWriter = io.Discard
	err := app.Run(context.Background(), append([]string{"porcupine", "--url", origin}, args...))
	return output.String(), err
}

func TestCommandOriginsAndPaths(t *testing.T) {
	t.Setenv("PORCUPINE_API_KEY", "example")
	for _, origin := range []string{"http://external.example", "https://key@example.org", "https://example.org/path", "https://example.org?test", "https://example.org#fragment"} {
		if _, err := invoke(t, origin, "status"); err == nil || !strings.Contains(err.Error(), "--url") {
			t.Fatalf("accepted invalid origin: %v", err)
		}
	}
	for _, value := range []string{".", "..", "a/b", "a\\b"} {
		if _, err := invoke(t, "https://example.org", "get-event-schema", "--id", value); err == nil || !strings.Contains(err.Error(), "path segment") {
			t.Fatalf("accepted invalid path segment: %v", err)
		}
	}
}

func TestRedirectDoesNotForwardKey(t *testing.T) {
	t.Setenv("PORCUPINE_API_KEY", "example")
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("followed a redirect")
	}))
	defer other.Close()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer example" {
			t.Error("service key missing")
		}
		http.Redirect(w, r, other.URL, http.StatusTemporaryRedirect)
	}))
	defer s.Close()
	if _, err := invoke(t, s.URL, "list-projects"); err == nil || !strings.Contains(err.Error(), "307") {
		t.Fatalf("expected HTTP 307, got %v", err)
	}
}

func TestStatusIsPublic(t *testing.T) {
	t.Setenv("PORCUPINE_API_KEY", "example")
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/readyz" || r.Header.Get("Authorization") != "" {
			t.Error("unexpected status request")
		}
		io.WriteString(w, "ready")
	}))
	defer s.Close()
	result, err := invoke(t, s.URL, "status")
	if err != nil || result != "ready" {
		t.Fatalf("unexpected status %q / %v", result, err)
	}
}

func TestCommandAPIErrorMessages(t *testing.T) {
	t.Setenv("PORCUPINE_API_KEY", "private-test-credential")
	for _, tc := range []struct {
		name, contentType, body, message string
	}{
		{"porcupine", "application/json; charset=utf-8", `{"error":"name must contain 1 to 200 UTF-8 bytes"}`, "name must contain 1 to 200 UTF-8 bytes"},
		{"problem", "application/problem+json", `{"title":"Invalid request","detail":"name is required"}`, "name is required"},
		{"problem title", "application/problem+json", `{"title":"Invalid request"}`, "Invalid request"},
		{"unicode", "application/json", `{"error":"Projet introuvable — Café"}`, "Projet introuvable — Café"},
		{"reflected secret", "application/json", `{"error":"Rejected private-test-credential","token":"private-test-credential"}`, "Rejected [redacted]"},
		{"terminal controls", "application/json", `{"error":"invalid\nname\u001b[31m\u202e value"}`, "invalid name [31m value"},
		{"gateway html", "text/html", "<html>private-test-credential</html>", ""},
		{"invalid json", "application/json", `{"error":"private-test-credential"`, ""},
		{"nonstring error", "application/json", `{"error":{"token":"private-test-credential"}}`, ""},
		{"other fields", "application/json", `{"token":"private-test-credential","title":"ignored"}`, ""},
		{"trailing data", "application/json", `{"error":"private-test-credential"} other`, ""},
		{"empty", "application/json", "", ""},
		{"whitespace", "application/json", `{"error":"\n\t"}`, ""},
		{"oversized", "application/json", `{"error":"` + strings.Repeat("x", 64<<10) + `"}`, ""},
		{"bounded message", "application/json", `{"error":"` + strings.Repeat("é", 1025) + `"}`, strings.Repeat("é", 1024) + "…"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", tc.contentType)
				w.WriteHeader(http.StatusBadRequest)
				io.WriteString(w, tc.body)
			}))
			defer server.Close()
			output, err := invoke(t, server.URL, "list-projects")
			want := "GET /api/v1/projects returned HTTP 400 (Bad Request)"
			if tc.message != "" {
				want += ": " + tc.message
			}
			if err == nil || err.Error() != want {
				t.Fatalf("want %q, got %v", want, err)
			}
			if output != "" {
				t.Fatalf("error response leaked to stdout: %q", output)
			}
		})
	}
}

func TestErrorBodyReadFailureKeepsHTTPStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", "100")
		w.WriteHeader(http.StatusServiceUnavailable)
		io.WriteString(w, `{"error":"partial`)
	}))
	defer server.Close()
	_, err := invoke(t, server.URL, "status")
	if err == nil || err.Error() != "GET /readyz returned HTTP 503 (Service Unavailable)" {
		t.Fatalf("unexpected error: %v", err)
	}
}
