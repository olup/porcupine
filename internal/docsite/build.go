package docsite

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func Build(ctx context.Context, root string) error {
	version, err := CheckRelease(root, "")
	if err != nil {
		return err
	}
	binary, err := hugoBinary(ctx)
	if err != nil {
		return err
	}
	stage, err := os.MkdirTemp("", "porcupine-docs-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(stage)
	output := filepath.Join(root, "bin/docs-web/porcupine")
	if err := os.RemoveAll(output); err != nil {
		return err
	}
	documents, relocated, err := loadDocuments(root)
	if err != nil {
		return err
	}
	var navigation []NavGroup
	if err := readJSON(filepath.Join(root, "docs-theme/navigation.json"), &navigation); err != nil {
		return err
	}
	var flat []NavItem
	for g := range navigation {
		for i := range navigation[g].Items {
			item := &navigation[g].Items[i]
			item.URL = DocsURL(item.Page)
			flat = append(flat, *item)
		}
	}
	for _, doc := range documents {
		canonical := doc.URI
		if alias := relocated[canonical]; alias != "" {
			canonical = alias
		}
		params := map[string]any{"nav_uri": canonical}
		for i, item := range flat {
			if item.Page == canonical {
				if i > 0 {
					params["previous"] = flat[i-1]
				}
				if i+1 < len(flat) {
					params["next"] = flat[i+1]
				}
			}
		}
		if err := writeDocument(stage, doc, params); err != nil {
			return err
		}
	}
	config := map[string]any{
		"baseURL": "https://olup.github.io/porcupine/docs/", "title": "Porcupine", "disableKinds": []string{"taxonomy", "term", "RSS", "404"},
		"params": map[string]any{"release_version": version, "navigation": navigation},
		"markup": map[string]any{"goldmark": map[string]any{"renderer": map[string]bool{"unsafe": true}}, "highlight": map[string]bool{"codeFences": false}, "tableOfContents": map[string]int{"startLevel": 2, "endLevel": 2}},
	}
	data, err := json.Marshal(config)
	if err != nil {
		return err
	}
	if err := writeFile(filepath.Join(stage, "hugo.json"), data); err != nil {
		return err
	}
	if err := copyTree(filepath.Join(root, "docs-theme/layouts"), filepath.Join(stage, "layouts")); err != nil {
		return err
	}
	for _, file := range []string{"docs.css", "docs.js"} {
		if err := copyFile(filepath.Join(root, "docs-theme", file), filepath.Join(stage, "static", file)); err != nil {
			return err
		}
	}
	command := exec.CommandContext(ctx, binary, "--source", stage, "--destination", filepath.Join(output, "docs"), "--panicOnWarning", "--quiet")
	command.Stdout, command.Stderr = os.Stdout, os.Stderr
	if err := command.Run(); err != nil {
		return fmt.Errorf("render documentation: %w", err)
	}
	if err := compressSitemap(filepath.Join(output, "docs")); err != nil {
		return err
	}
	if err := copyPublicAssets(root, output, version); err != nil {
		return err
	}
	for _, doc := range documents {
		if err := writeFile(filepath.Join(output, "docs", filepath.FromSlash(doc.URI)), []byte(doc.Markdown)); err != nil {
			return err
		}
		if _, err := os.Stat(filepath.Join(output, "docs", filepath.FromSlash(HTMLPath(doc.URI)), "index.html")); err != nil {
			return fmt.Errorf("missing rendered guide %s: %w", doc.URI, err)
		}
	}
	for _, file := range []string{"README.md", "cli/README.md"} {
		if err := copyFile(filepath.Join(root, file), filepath.Join(output, file)); err != nil {
			return err
		}
	}
	if err := CheckBuiltSite(output, version); err != nil {
		return err
	}
	return ValidateLinks(output)
}

func compressSitemap(directory string) error {
	sitemap, err := os.ReadFile(filepath.Join(directory, "sitemap.xml"))
	if err != nil {
		return fmt.Errorf("read rendered sitemap: %w", err)
	}
	file, err := os.Create(filepath.Join(directory, "sitemap.xml.gz"))
	if err != nil {
		return err
	}
	writer := gzip.NewWriter(file)
	_, writeErr := writer.Write(sitemap)
	zipErr := writer.Close()
	closeErr := file.Close()
	if writeErr != nil {
		return writeErr
	}
	if zipErr != nil {
		return zipErr
	}
	return closeErr
}

func copyPublicAssets(root, output, version string) error {
	if err := copyTree(filepath.Join(root, "site"), output); err != nil {
		return err
	}
	landing, err := os.ReadFile(filepath.Join(output, "index.html"))
	if err != nil {
		return err
	}
	if err := writeFile(filepath.Join(output, "index.html"), []byte(strings.ReplaceAll(string(landing), "__PORCUPINE_RELEASE_TAG__", "v"+version))); err != nil {
		return err
	}
	for _, file := range []string{"compat/matrix.json", "compat/profiles.mjs", "compose.yaml", "compose.release.yaml"} {
		if err := copyFile(filepath.Join(root, file), filepath.Join(output, file)); err != nil {
			return err
		}
	}
	schemas, err := filepath.Glob(filepath.Join(root, "internal/porcupine/contracts/*.schema.json"))
	if err != nil {
		return err
	}
	for _, file := range schemas {
		if err := copyFile(file, filepath.Join(output, "schemas", filepath.Base(file))); err != nil {
			return err
		}
	}
	if err := copyTree(filepath.Join(root, "examples/events"), filepath.Join(output, "examples/events")); err != nil {
		return err
	}
	return filepath.WalkDir(filepath.Join(root, "docs"), func(file string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || filepath.Ext(file) == ".md" {
			return nil
		}
		relative, _ := filepath.Rel(filepath.Join(root, "docs"), file)
		return copyFile(file, filepath.Join(output, "docs", relative))
	})
}

func copyFile(source, destination string) error {
	data, err := os.ReadFile(source)
	if err != nil {
		return err
	}
	return writeFile(destination, data)
}
func copyTree(source, destination string) error {
	return filepath.WalkDir(source, func(file string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("public assets cannot contain symlinks: %s", file)
		}
		relative, _ := filepath.Rel(source, file)
		return copyFile(file, filepath.Join(destination, relative))
	})
}
