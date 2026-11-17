// Package docsite prepares and verifies the static site rendered by Hugo.
package docsite

import (
	"encoding/json"
	"os"
	"path"
	"path/filepath"
	"strings"
)

const BasePath = "/porcupine/"

type NavItem struct {
	Title string `json:"title"`
	Page  string `json:"page"`
	URL   string `json:"url,omitempty"`
}
type NavGroup struct {
	Title string    `json:"title"`
	Items []NavItem `json:"items"`
}

func readJSON(file string, value any) error {
	data, err := os.ReadFile(file)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, value)
}

func HTMLPath(uri string) string {
	if path.Base(uri) == "README.md" {
		return strings.TrimPrefix(path.Dir(uri)+"/", "./")
	}
	return strings.TrimSuffix(uri, ".md") + "/"
}

func DocsURL(uri string) string { return BasePath + "docs/" + HTMLPath(uri) }

func FindRoot(directory string) (string, error) {
	root, err := filepath.Abs(directory)
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(root, "VERSION")); err == nil {
			return root, nil
		}
		parent := filepath.Dir(root)
		if parent == root {
			return "", os.ErrNotExist
		}
		root = parent
	}
}
