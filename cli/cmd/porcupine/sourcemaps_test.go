package main

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

const sourceMapUploadFixture = `{"version":3,"sources":["src/app.ts"],"names":[],"mappings":"AAAA"}`

const sourceMapFixture = "{ \"version\": 3, \"sources\": [\"src/app.ts\"], \"names\": [], \"mappings\": \"AAAA\" }\n"

func TestUploadSourceMaps(t *testing.T) {
	const apiKey = "private-source-map-upload-key"
	t.Setenv("PORCUPINE_API_KEY", apiKey)

	t.Run("build assets", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		name := "assets/app #%.js.map"
		writeSourceMapFixture(t, dir, name, sourceMapFixture)
		outside := t.TempDir()
		writeSourceMapFixture(t, outside, "outside.js.map", sourceMapFixture)
		if err := os.Symlink(filepath.Join(outside, "outside.js.map"), filepath.Join(dir, "escape.js.map")); err != nil {
			t.Fatal(err)
		}
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			raw, err := io.ReadAll(r.Body)
			if err != nil || string(raw) != sourceMapUploadFixture {
				t.Errorf("unexpected upload body %q, %v", raw, err)
			}
			for name, expected := range map[string]string{
				"Authorization":          "Bearer " + apiKey,
				"Content-Type":           "application/octet-stream",
				"X-Source-Map-Build-ID":  "commit-1",
				"X-Source-Map-Asset-URL": "https://cdn.example.com/prefix%20name/assets/app%20%23%25.js",
				"X-Source-Map-Chunk-ID":  "",
			} {
				if r.Header.Get(name) != expected {
					t.Errorf("unexpected %s header", name)
				}
			}
			if r.Method != http.MethodPut || r.URL.EscapedPath() != "/api/v1/projects/project%20one/source-maps" {
				t.Errorf("unexpected route: %s %s", r.Method, r.URL)
			}
			_, _ = io.WriteString(w, "{}")
		}))
		defer s.Close()
		output, err := invoke(t, s.URL, "upload-source-maps", "--id", "project one", "--dir", dir, "--build", "commit-1", "--asset-base", "https://cdn.example.com/prefix%20name")
		if err != nil || output != "Uploaded 1 source map(s).\n" {
			t.Fatalf("upload result %q, %v", output, err)
		}
		raw, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil || string(raw) != sourceMapFixture {
			t.Fatalf("source map changed: %q, %v", raw, err)
		}
	})

	t.Run("PostHog artifacts", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		bundle := "console.log(1);\n//# sourceMappingURL=../maps/shared%20chunk.map\n//# chunkId=first\n"
		shared := strings.Replace(sourceMapFixture, "{ ", "{ \"chunk_id\": \"map-field\", ", 1)
		writeSourceMapFixture(t, dir, "maps/shared chunk.map", shared)
		writeSourceMapFixture(t, dir, "assets/first.js", bundle)
		writeSourceMapFixture(t, dir, "assets/second.mjs", strings.Replace(bundle, "Id=first", "Id=second", 1))
		writeSourceMapFixture(t, dir, "map-only.js.map", strings.Replace(sourceMapFixture, "{ ", "{ \"chunkId\": \"camel-field\", ", 1))
		writeSourceMapFixture(t, dir, "unrelated.js.map", sourceMapFixture)
		writeSourceMapFixture(t, dir, "fallback.cjs.map", sourceMapFixture)
		writeSourceMapFixture(t, dir, "fallback.cjs", "console.log(1);\n//# sourceMappingURL=https://cdn.example.com/fallback.cjs.map\n//# chunkId=remote-reference\n")

		var mu sync.Mutex
		uploads := map[string]int{}
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			chunk := r.Header.Get("X-Source-Map-Chunk-ID")
			mu.Lock()
			uploads[chunk]++
			mu.Unlock()
			if r.Header.Get("X-Source-Map-Build-ID") != "" || r.Header.Get("X-Source-Map-Asset-URL") != "" {
				t.Error("PostHog request sent extra selector or release headers")
			}
			_, _ = io.WriteString(w, "{}")
		}))
		defer s.Close()
		output, err := invoke(t, s.URL, "upload-source-maps", "--id", "project", "--dir", dir)
		if err != nil || output != "Uploaded 4 source map(s).\n" {
			t.Fatalf("upload result %q, %v", output, err)
		}
		mu.Lock()
		defer mu.Unlock()
		if len(uploads) != 4 || uploads["first"] != 1 || uploads["second"] != 1 || uploads["camel-field"] != 1 || uploads["remote-reference"] != 1 {
			t.Errorf("unexpected chunk IDs: %v", uploads)
		}
		raw, err := os.ReadFile(filepath.Join(dir, "assets/first.js"))
		if err != nil || string(raw) != bundle {
			t.Fatalf("bundle changed: %q, %v", raw, err)
		}
	})

	t.Run("conflict redacts credentials", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		writeSourceMapFixture(t, dir, "app.js.map", sourceMapFixture)
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusConflict)
			_, _ = fmt.Fprintf(w, `{"error":"selector already used by %s"}`, apiKey)
		}))
		defer s.Close()
		output, err := invoke(t, s.URL, "upload-source-maps", "--id", "project", "--dir", dir, "--build", "commit-1", "--asset-base", "https://cdn.example.com/")
		if output != "" || err == nil || !strings.Contains(err.Error(), "409") || strings.Contains(err.Error(), apiKey) {
			t.Fatalf("conflict result %q, %v", output, err)
		}
	})

	t.Run("invalid asset arguments and oversized maps", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		writeSourceMapFixture(t, dir, "huge.js.map", sourceMapFixture)
		if err := os.Truncate(filepath.Join(dir, "huge.js.map"), maxSourceMapInput+1); err != nil {
			t.Fatal(err)
		}
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Error("unexpected upload")
		}))
		defer s.Close()
		for _, tc := range []struct {
			args    []string
			message string
		}{
			{[]string{"--build", "commit-1"}, "must be supplied together"},
			{[]string{"--build", "commit-1", "--asset-base", "https://user:secret@cdn.example.com"}, "requires an absolute HTTP"},
			{[]string{"--build", "commit-1", "--asset-base", "https://cdn.example.com"}, "16777216 bytes"},
		} {
			_, err := invoke(t, s.URL, append([]string{"upload-source-maps", "--id", "project", "--dir", dir}, tc.args...)...)
			if err == nil || !strings.Contains(err.Error(), tc.message) || strings.Contains(err.Error(), "secret") {
				t.Errorf("expected %q, got %v", tc.message, err)
			}
		}
	})
}

func writeSourceMapFixture(t *testing.T, dir, name, contents string) {
	t.Helper()
	filename := filepath.Join(dir, name)
	if err := os.MkdirAll(filepath.Dir(filename), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filename, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestSourceMapUploadStripsEmbeddedSourcesBeforeApplyingWireLimit(t *testing.T) {
	t.Setenv("PORCUPINE_API_KEY", "scoped-key")
	dir := t.TempDir()
	source := strings.TrimSpace(sourceMapFixture)
	source = strings.TrimSuffix(source, "}") + `,"sourcesContent":["` + strings.Repeat("source text ", 200000) + `"]}`
	indexed := `{"version":3,"sections":[{"offset":{"line":0,"column":0},"map":` + source + `}]}`
	writeSourceMapFixture(t, dir, "app.js.map", indexed)
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		want := `{"version":3,"sections":[{"offset":{"line":0,"column":0},"map":` + sourceMapUploadFixture + `}]}`
		if err != nil || string(body) != want {
			t.Errorf("unexpected sanitized upload of %d bytes: %v", len(body), err)
		}
		_, _ = io.WriteString(w, "{}")
	}))
	defer s.Close()
	if _, err := invoke(t, s.URL, "upload-source-maps", "--id", "project", "--dir", dir, "--build", "build", "--asset-base", "https://cdn.example.com/"); err != nil {
		t.Fatal(err)
	}
}
