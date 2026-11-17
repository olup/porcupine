package docsite

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func repository(t *testing.T) string {
	t.Helper()
	root, err := FindRoot(".")
	if err != nil {
		t.Fatal(err)
	}
	return root
}
func put(t *testing.T, root, name, text string) {
	t.Helper()
	if err := writeFile(filepath.Join(root, filepath.FromSlash(name)), []byte(text)); err != nil {
		t.Fatal(err)
	}
}
func contents(t *testing.T, root, name string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(name)))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
func releaseFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	source := repository(t)
	for _, name := range []string{"VERSION", "compose.release.yaml", "README.md", "site/llms.txt", "docs/hosting.md", "site/index.html", "docs-theme/layouts/_default/baseof.html"} {
		if err := copyFile(filepath.Join(source, name), filepath.Join(root, name)); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func TestReleasePointers(t *testing.T) {
	t.Parallel()
	source := releaseFixture(t)
	version, err := ReleaseVersion(source)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := CheckRelease(source, "v"+version); err != nil || got != version {
		t.Fatalf("version=%q error=%v", got, err)
	}
	if _, err := CheckRelease(source, "v999.0.0"); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("wrong tag: %v", err)
	}
	for _, name := range []string{"compose.release.yaml", "README.md", "site/llms.txt", "docs/hosting.md"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			root := releaseFixture(t)
			put(t, root, name, strings.ReplaceAll(contents(t, root, name), version, "999.0.0"))
			if _, err := CheckRelease(root, ""); err == nil || !strings.Contains(err.Error(), "current release pointers") {
				t.Fatalf("stale %s: %v", name, err)
			}
		})
	}
}

func TestQuickstartCheckoutMustMatchRelease(t *testing.T) {
	t.Parallel()
	root := releaseFixture(t)
	version, _ := ReleaseVersion(root)
	put(t, root, "README.md", strings.ReplaceAll(contents(t, root, "README.md"), "git clone --branch v"+version, "git clone --branch v999.0.0"))
	if _, err := CheckRelease(root, ""); err == nil {
		t.Fatal("accepted stale checkout")
	}
}
func TestVersionRequiresStableRelease(t *testing.T) {
	t.Parallel()
	for _, version := range []string{"v1.2.3", "1.2.3-dev", "", "1.2"} {
		root := t.TempDir()
		put(t, root, "VERSION", version)
		if _, err := ReleaseVersion(root); err == nil {
			t.Fatalf("accepted %q", version)
		}
	}
}
func TestGeneratedVersionMarkers(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"site/index.html", "docs-theme/layouts/_default/baseof.html"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			root := releaseFixture(t)
			text := contents(t, root, name)
			text = strings.ReplaceAll(text, "__PORCUPINE_RELEASE_TAG__", "v0.9.0")
			text = strings.ReplaceAll(text, "{{ .Site.Params.release_version }}", "0.10.0")
			put(t, root, name, text)
			if _, err := CheckRelease(root, ""); err == nil || !strings.Contains(err.Error(), "generated version") {
				t.Fatalf("hardcoded version accepted: %v", err)
			}
		})
	}
}
func TestBuiltVersions(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	put(t, root, "index.html", `<a href="releases/tag/v1.2.3">Release v1.2.3</a>`)
	put(t, root, "docs/hosting/index.html", `<span>Porcupine v1.2.3</span>`)
	if err := CheckBuiltSite(root, "1.2.3"); err != nil {
		t.Fatal(err)
	}
	put(t, root, "docs/hosting/index.html", `<span>Porcupine v0.10.0</span>`)
	if err := CheckBuiltSite(root, "1.2.3"); err == nil {
		t.Fatal("accepted stale badge")
	}
}

func TestPublishedRelease(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name                 string
		status               int
		body                 string
		published, wantError bool
	}{
		{"stable", 200, `{"draft":false,"prerelease":false}`, true, false}, {"draft", 200, `{"draft":true,"prerelease":false}`, false, false}, {"prerelease", 200, `{"draft":false,"prerelease":true}`, false, false}, {"not published", 404, "", false, false}, {"unavailable", 503, "", false, true}, {"invalid body", 200, `{}`, false, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/repos/olup/porcupine/releases/tags/v1.2.3" || r.Header.Get("Authorization") != "Bearer test-token" {
					t.Errorf("unexpected release request: %s", r.URL.Path)
				}
				w.WriteHeader(test.status)
				fmt.Fprint(w, test.body)
			}))
			defer server.Close()
			got, err := PublishedRelease(context.Background(), server.Client(), server.URL, "olup/porcupine", "test-token", "1.2.3")
			if (err != nil) != test.wantError || got != test.published {
				t.Fatalf("published=%t error=%v", got, err)
			}
		})
	}
}

func TestLinkRewriting(t *testing.T) {
	t.Parallel()
	root := repository(t)
	relocated := map[string]string{"analytics.md": "reference/analytics.md"}
	for _, test := range []struct{ uri, source, input, want string }{
		{"reference/mcp.md", "docs/reference/mcp.md", "[Start](../README.md) [SQL](analytics.md)", "[Start](../README.md) [SQL](analytics.md)"},
		{"mcp.md", "docs/reference/mcp.md", "[Start](../README.md) [SQL](analytics.md)", "[Start](README.md) [SQL](reference/analytics.md)"},
		{"quickstart.md", "README.md", "[Host](docs/hosting.md#start) [CLI](cli/README.md)", "[Host](hosting.md#start) [CLI](cli.md)"},
		{"design/releasing.md", "docs/design/releasing.md", "[Config](../../compose.yaml)", "[Config](https://github.com/olup/porcupine/blob/main/compose.yaml)"},
		{"reference/mcp.md", "docs/reference/mcp.md", "[External](https://example.com/x.md#y) [Anchor](#section)", "[External](https://example.com/x.md#y) [Anchor](#section)"},
	} {
		got := rewriteLinks(root, document{URI: test.uri, Source: filepath.Join(root, test.source), Markdown: test.input}, relocated)
		if got != test.want {
			t.Errorf("%s: %s, want %s", test.uri, got, test.want)
		}
	}
}

func TestGeneratedDocumentsAndPublicAliases(t *testing.T) {
	t.Parallel()
	docs, _, err := loadDocuments(repository(t))
	if err != nil {
		t.Fatal(err)
	}
	documents := map[string]document{}
	for _, doc := range docs {
		documents[doc.URI] = doc
	}
	for _, uri := range []string{"reference/mcp.md", "design/ducklake-validation.md", "native-event-contract.md", "cli.md", "quickstart.md"} {
		if documents[uri].Markdown == "" {
			t.Errorf("missing generated document %s", uri)
		}
	}
	if !strings.Contains(documents["mcp.md"].Markdown, "reference/analytics.md") {
		t.Fatal("alias relative links did not use source directory")
	}
	if !strings.HasPrefix(documents["quickstart.md"].Markdown, "# Quickstart\n") || strings.Contains(documents["quickstart.md"].Markdown, "## Quickstart") {
		t.Fatal("quickstart includes repeated title or introduction")
	}
	if strings.Contains(documents["quickstart.md"].Markdown, "# Development") {
		t.Fatal("quickstart includes README sections after quickstart")
	}
}

func TestMarkdownPreview(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	put(t, root, "porcupine/docs/reference/contract.md", "# Native contract\n")
	server := httptest.NewServer(PreviewHandler(root))
	defer server.Close()
	response, err := server.Client().Get(server.URL + "/porcupine/docs/reference/contract.md")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, _ := io.ReadAll(response.Body)
	if response.StatusCode != 200 || response.Header.Get("Content-Type") != "text/plain; charset=utf-8" || response.Header.Get("Content-Disposition") != "" || string(data) != "# Native contract\n" {
		t.Fatalf("markdown response: %d %v %q", response.StatusCode, response.Header, data)
	}
}

func TestBuiltLinkValidation(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	put(t, root, "index.html", `<a href="docs/nested/#good">Guide</a>`)
	put(t, root, "docs/nested/index.html", `<h1 id="good">Guide</h1><a href="/porcupine/">Home</a><a href="../source.md">Markdown</a><img src="/porcupine/assets/icon.svg">`)
	put(t, root, "docs/source.md", "# Source\n")
	put(t, root, "assets/icon.svg", "<svg/>")
	if err := ValidateLinks(root); err != nil {
		t.Fatal(err)
	}
	put(t, root, "index.html", `<a href="docs/nested/#missing">Missing</a><img src="/porcupine/missing.svg">`)
	err := ValidateLinks(root)
	if err == nil || !strings.Contains(err.Error(), "missing anchor") || !strings.Contains(err.Error(), "missing.svg") {
		t.Fatalf("broken links not rejected: %v", err)
	}
}

func TestPublicAssetAllowlist(t *testing.T) {
	t.Parallel()
	root, output := t.TempDir(), t.TempDir()
	for _, name := range []string{"site/index.html", "compat/matrix.json", "compat/profiles.mjs", "compose.yaml", "compose.release.yaml", "compose.private.yaml", "porcupine.json", ".env", "examples/events/test.jsonl", "docs/example.jsonl", "internal/porcupine/contracts/native.schema.json"} {
		put(t, root, name, "test __PORCUPINE_RELEASE_TAG__")
	}
	if err := copyPublicAssets(root, output, "1.2.3"); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"compose.yaml", "compose.release.yaml", "schemas/native.schema.json", "examples/events/test.jsonl", "docs/example.jsonl"} {
		if _, err := os.Stat(filepath.Join(output, name)); err != nil {
			t.Error(err)
		}
	}
	for _, name := range []string{"compose.private.yaml", "porcupine.json", ".env"} {
		if _, err := os.Stat(filepath.Join(output, name)); !os.IsNotExist(err) {
			t.Errorf("private asset published: %s", name)
		}
	}
	if contents(t, output, "index.html") != "test v1.2.3" {
		t.Fatal("landing version not generated")
	}
}

func TestHugoArchiveExtraction(t *testing.T) {
	t.Parallel()
	var tarball bytes.Buffer
	gz := gzip.NewWriter(&tarball)
	archive := tar.NewWriter(gz)
	payload := []byte("standalone binary")
	if err := archive.WriteHeader(&tar.Header{Name: "hugo", Mode: 0755, Size: int64(len(payload))}); err != nil {
		t.Fatal(err)
	}
	archive.Write(payload)
	archive.Close()
	gz.Close()
	got, err := extractHugo(context.Background(), "hugo.tar.gz", tarball.Bytes())
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("tar extraction: %q %v", got, err)
	}
	var zipped bytes.Buffer
	zipArchive := zip.NewWriter(&zipped)
	file, _ := zipArchive.Create("hugo.exe")
	file.Write(payload)
	zipArchive.Close()
	got, err = extractHugo(context.Background(), "hugo.zip", zipped.Bytes())
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("zip extraction: %q %v", got, err)
	}
	if _, err := extractHugo(context.Background(), "invalid.tar.gz", []byte("not gzip")); err == nil {
		t.Fatal("invalid archive accepted")
	}
}
