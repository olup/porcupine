package docsite

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

type document struct {
	URI, Source, Markdown string
}

var markdownLink = regexp.MustCompile(`\]\(([^)\s]+)\)`)
var heading = regexp.MustCompile(`(?m)^# (.+)$`)

func loadDocuments(root string) ([]document, map[string]string, error) {
	relocated := map[string]string{}
	if err := readJSON(filepath.Join(root, "docs-theme/relocated-guides.json"), &relocated); err != nil {
		return nil, nil, err
	}
	var documents []document
	err := filepath.WalkDir(filepath.Join(root, "docs"), func(file string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || filepath.Ext(file) != ".md" {
			return err
		}
		relative, _ := filepath.Rel(filepath.Join(root, "docs"), file)
		body, err := os.ReadFile(file)
		if err != nil {
			return err
		}
		documents = append(documents, document{URI: filepath.ToSlash(relative), Source: file, Markdown: string(body)})
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	generated := map[string]string{"quickstart.md": "README.md", "cli.md": "cli/README.md"}
	for old, current := range relocated {
		generated[old] = "docs/" + current
	}
	for uri, source := range generated {
		body, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(source)))
		if err != nil {
			return nil, nil, err
		}
		markdown := string(body)
		if uri == "quickstart.md" {
			_, section, found := strings.Cut(markdown, "\n## Quickstart\n")
			if !found {
				return nil, nil, fmt.Errorf("README.md is missing its Quickstart section")
			}
			section, _, _ = strings.Cut(section, "\n## ")
			markdown = "# Quickstart\n" + strings.ReplaceAll(section, "\n### ", "\n## ")
			markdown = strings.ReplaceAll(markdown, `src="site/assets/`, `src="/porcupine/assets/`)
		}
		documents = append(documents, document{URI: uri, Source: filepath.Join(root, filepath.FromSlash(source)), Markdown: markdown})
	}
	for i := range documents {
		documents[i].Markdown = rewriteLinks(root, documents[i], relocated)
	}
	return documents, relocated, nil
}

func rewriteLinks(root string, doc document, relocated map[string]string) string {
	return markdownLink.ReplaceAllStringFunc(doc.Markdown, func(match string) string {
		href := markdownLink.FindStringSubmatch(match)[1]
		parsed, err := url.Parse(href)
		if err != nil || parsed.Scheme != "" || parsed.Host != "" || strings.HasPrefix(href, "#") || strings.HasPrefix(href, "/") {
			return match
		}
		target := filepath.Clean(filepath.Join(filepath.Dir(doc.Source), filepath.FromSlash(parsed.Path)))
		relative, err := filepath.Rel(root, target)
		if err != nil || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return match
		}
		relative = filepath.ToSlash(relative)
		var mapped string
		switch relative {
		case "README.md":
			mapped = "quickstart.md"
		case "cli/README.md":
			mapped = "cli.md"
		default:
			if strings.HasPrefix(relative, "docs/") && strings.HasSuffix(relative, ".md") {
				mapped = strings.TrimPrefix(relative, "docs/")
				if canonical := relocated[mapped]; canonical != "" {
					mapped = canonical
				}
			}
		}
		if mapped != "" {
			destination, _ := filepath.Rel(filepath.Dir(filepath.FromSlash(doc.URI)), filepath.FromSlash(mapped))
			parsed.Path = filepath.ToSlash(destination)
			return "](" + parsed.String() + ")"
		}
		if info, err := os.Stat(target); err == nil && !info.IsDir() {
			parsed.Path = "/olup/porcupine/blob/main/" + relative
			parsed.Scheme, parsed.Host = "https", "github.com"
			return "](" + parsed.String() + ")"
		}
		return match
	})
}

func writeDocument(stage string, doc document, params map[string]any) error {
	title := strings.TrimSuffix(filepath.Base(doc.URI), ".md")
	if match := heading.FindStringSubmatch(doc.Markdown); match != nil {
		title = strings.Trim(match[1], "` ")
	}
	params["raw_url"] = BasePath + "docs/" + doc.URI
	params["source_uri"] = doc.URI
	header, err := json.Marshal(map[string]any{"title": title, "url": "/" + HTMLPath(doc.URI), "params": params})
	if err != nil {
		return err
	}
	file := doc.URI
	if filepath.Base(file) == "README.md" {
		file = filepath.Join(filepath.Dir(file), "_index.md")
	}
	return writeFile(filepath.Join(stage, "content", filepath.FromSlash(file)), append(append(header, '\n', '\n'), doc.Markdown...))
}

func writeFile(file string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(file), 0755); err != nil {
		return err
	}
	return os.WriteFile(file, data, 0644)
}
