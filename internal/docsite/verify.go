package docsite

import (
	"fmt"
	"html"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

var htmlTag = regexp.MustCompile(`(?is)<(?:a|img|link|script|source)\b[^>]*>`)
var linkAttribute = regexp.MustCompile(`\b(?:href|src)="([^"]*)"`)
var anchorAttribute = regexp.MustCompile(`\b(?:id|name)="([^"]*)"`)

func CheckBuiltSite(output, version string) error {
	landing, err := os.ReadFile(filepath.Join(output, "index.html"))
	if err != nil {
		return err
	}
	for _, marker := range []string{"releases/tag/v" + version, "Release v" + version} {
		if !strings.Contains(string(landing), marker) {
			return fmt.Errorf("built landing is missing current release %s", version)
		}
	}
	return filepath.WalkDir(output, func(file string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || filepath.Ext(file) != ".html" {
			return err
		}
		data, err := os.ReadFile(file)
		if err != nil {
			return err
		}
		if strings.Contains(string(data), "__PORCUPINE_RELEASE_TAG__") {
			return fmt.Errorf("unresolved release placeholder in %s", file)
		}
		if strings.HasPrefix(file, filepath.Join(output, "docs")+string(filepath.Separator)) && !strings.Contains(string(data), "<span>Porcupine v"+version+"</span>") {
			return fmt.Errorf("built documentation badge is stale in %s", file)
		}
		return nil
	})
}

func ValidateLinks(output string) error {
	pages := map[string]string{}
	err := filepath.WalkDir(output, func(file string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || filepath.Ext(file) != ".html" {
			return err
		}
		data, err := os.ReadFile(file)
		if err != nil {
			return err
		}
		pages[file] = string(data)
		return nil
	})
	if err != nil {
		return err
	}
	var problems []string
	for file, body := range pages {
		relative, _ := filepath.Rel(output, file)
		for _, tag := range htmlTag.FindAllString(body, -1) {
			for _, match := range linkAttribute.FindAllStringSubmatch(tag, -1) {
				href := html.UnescapeString(match[1])
				parsed, err := url.Parse(href)
				if err != nil {
					problems = append(problems, relative+": invalid URL "+href)
					continue
				}
				if parsed.IsAbs() || parsed.Host != "" || strings.HasPrefix(href, "//") {
					continue
				}
				targetURL := path.Clean(path.Join(BasePath, filepath.ToSlash(filepath.Dir(relative)), parsed.Path))
				if strings.HasPrefix(parsed.Path, "/") {
					targetURL = path.Clean(parsed.Path)
				}
				if !strings.HasPrefix(targetURL+"/", BasePath) {
					problems = append(problems, relative+": link outside site "+href)
					continue
				}
				target := filepath.Join(output, filepath.FromSlash(strings.TrimPrefix(strings.TrimPrefix(targetURL, strings.TrimSuffix(BasePath, "/")), "/")))
				info, err := os.Stat(target)
				if err == nil && info.IsDir() {
					target = filepath.Join(target, "index.html")
					_, err = os.Stat(target)
				}
				if err != nil {
					problems = append(problems, relative+": missing "+href)
					continue
				}
				if parsed.Fragment != "" && filepath.Ext(target) == ".html" {
					found := false
					for _, anchor := range anchorAttribute.FindAllStringSubmatch(pages[target], -1) {
						if html.UnescapeString(anchor[1]) == parsed.Fragment {
							found = true
							break
						}
					}
					if !found {
						problems = append(problems, relative+": missing anchor "+href)
					}
				}
			}
		}
	}
	if len(problems) > 0 {
		sort.Strings(problems)
		return fmt.Errorf("invalid built documentation links:\n%s", strings.Join(problems, "\n"))
	}
	return nil
}

// PreviewHandler serves the built site; Markdown stays readable in a browser.
func PreviewHandler(directory string) http.Handler {
	files := http.FileServer(http.Dir(directory))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, ".md") {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		}
		files.ServeHTTP(w, r)
	})
}
