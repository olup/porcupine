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
