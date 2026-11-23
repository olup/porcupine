package docsite

import (
	"encoding/json"
	"io/fs"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/olup/porcupine/internal/apischema"
)

var (
	envTag       = regexp.MustCompile(`\benv:"(PORCUPINE_[A-Z0-9_]+)"`)
	envPrefixTag = regexp.MustCompile(`\b(\w+)[ \t]+\x60[^\x60\n]*\benvprefix:"(PORCUPINE_[A-Z0-9_]+)"`)
	envSuffixTag = regexp.MustCompile(`\bsuffix:"([A-Z0-9_]+)"`)
	configStruct = regexp.MustCompile(`(?ms)^type (\w+) struct \{(.*?)^\}`)
	envMention   = regexp.MustCompile(`\bPORCUPINE_[A-Z0-9_]*\b`)

	mcpTool    = regexp.MustCompile(`\bName:\s*"(porcupine_[a-z0-9_]+)"`)
	mcpMention = regexp.MustCompile("`(porcupine_[a-z0-9_]+)`")
	mcpExample = regexp.MustCompile(`"name"\s*:\s*"(porcupine_[a-z0-9_]+)"`)

	operationExample = regexp.MustCompile(`\boperation_id"?\s*:\s*"?([a-z][a-z0-9]*(?:-[a-z0-9]+)+)\b`)
	operationLiteral = regexp.MustCompile("`([a-z][a-z0-9]*(?:-[a-z0-9]+)+)`")
	releaseAnchor    = regexp.MustCompile(`(?i)(^#{1,6}[ \t]+.*\bunreleased\b|\bid=["'][^"']*unreleased[^"']*["'])`)
)

func TestPublishedDocumentationContracts(t *testing.T) {
	t.Parallel()
	root := repository(t)
	var schema struct {
		Paths map[string]map[string]struct {
			ID string `json:"operationId"`
		}
	}
	if err := json.Unmarshal(apischema.Document(), &schema); err != nil {
		t.Fatal(err)
	}
	operations, verbs := map[string]bool{}, map[string]bool{}
	for _, methods := range schema.Paths {
		for _, op := range methods {
			if verb, _, ok := strings.Cut(op.ID, "-"); ok {
				operations[op.ID], verbs[verb] = true, true
			}
		}
	}
	tools := map[string]bool{}
	for _, match := range mcpTool.FindAllStringSubmatch(contents(t, root, "internal/porcupine/mcp.go"), -1) {
		tools[match[1]] = true
	}
	env := documentedConfigurationEnv(contents(t, root, "cmd/porcupine/config_file.go"))

	check := func(file string) {
		for i, line := range strings.Split(contents(t, root, file), "\n") {
			checkNames := func(pattern *regexp.Regexp, known map[string]bool, kind string) {
				for _, match := range pattern.FindAllStringSubmatch(line, -1) {
					if !known[match[1]] {
						t.Errorf("%s:%d: unknown %s %q", file, i+1, kind, match[1])
					}
				}
			}
			checkNames(operationExample, operations, "operation ID")
			for _, match := range operationLiteral.FindAllStringSubmatch(line, -1) {
				verb, _, _ := strings.Cut(match[1], "-")
				if verbs[verb] && !operations[match[1]] {
					t.Errorf("%s:%d: unknown operation ID %q", file, i+1, match[1])
				}
			}
			checkNames(mcpExample, tools, "MCP tool")
			if file == "docs/reference/mcp.md" || file == "site/llms.txt" {
				checkNames(mcpMention, tools, "MCP tool")
			}
			for _, name := range envMention.FindAllString(line, -1) {
				if !env[name] {
					t.Errorf("%s:%d: unknown environment variable %q", file, i+1, name)
				}
			}
			if releaseAnchor.MatchString(line) {
				t.Errorf("%s:%d: use a release-neutral heading or anchor", file, i+1)
			}
		}
	}

	err := filepath.WalkDir(filepath.Join(root, "docs"), func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == filepath.Join(root, "docs/design") {
			return filepath.SkipDir
		}
		if !entry.IsDir() && filepath.Ext(path) == ".md" {
			file, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			check(filepath.ToSlash(file))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	check("site/llms.txt")
}

func documentedConfigurationEnv(config string) map[string]bool {
	known := map[string]bool{
		"PORCUPINE_CONFIG": true,                                                         // Server configuration file selector, outside FileConfig.
		"PORCUPINE_URL":    true, "PORCUPINE_API_KEY": true, "PORCUPINE_WRITE_KEY": true, // Client examples.
		"PORCUPINE_IMAGE": true, "PORCUPINE_BACKUP_HOST_DIR": true, // Compose and shell setup.
		"PORCUPINE_PROBE_INGEST_TOKEN": true, "PORCUPINE_PROBE_QUERY_TOKEN": true, // Visibility probe.
		"PORCUPINE_RELEASE_ARCH": true, "PORCUPINE_RELEASE_ARCHIVE": true, // Offline image loader.
		"PORCUPINE_RELEASE_URL": true, "PORCUPINE_RELEASE_VERSION": true,
	}
	bodies := map[string]string{}
	for _, match := range configStruct.FindAllStringSubmatch(config, -1) {
		bodies[match[1]] = match[2]
	}
	for _, match := range envTag.FindAllStringSubmatch(config, -1) {
		known[match[1]] = true
	}
	for _, match := range envPrefixTag.FindAllStringSubmatch(config, -1) {
		for _, suffix := range envSuffixTag.FindAllStringSubmatch(bodies[match[1]], -1) {
			known[match[2]+suffix[1]] = true
		}
	}
	// The guides also name families such as PORCUPINE_EXPORT_.
	for name := range known {
		for i := range len(name) {
			if name[i] == '_' {
				known[name[:i+1]] = true
			}
		}
	}
	return known
}
