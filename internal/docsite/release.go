package docsite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

var stableVersion = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+$`)

func ReleaseVersion(root string) (string, error) {
	data, err := os.ReadFile(filepath.Join(root, "VERSION"))
	if err != nil {
		return "", err
	}
	version := strings.TrimSpace(string(data))
	if !stableVersion.MatchString(version) {
		return "", errors.New("VERSION must contain a stable version such as 0.14.0")
	}
	return version, nil
}

func CheckRelease(root, tag string) (string, error) {
	version, err := ReleaseVersion(root)
	if err != nil {
		return "", err
	}
	if tag != "" && tag != "v"+version {
		return "", fmt.Errorf("release tag %s does not match VERSION v%s", tag, version)
	}
	patterns := map[string]string{
		"compose.release.yaml": `ghcr\.io/olup/porcupine:([0-9]+\.[0-9]+\.[0-9]+)`,
		"README.md":            `(?:releases/tag/v|git clone --branch v)([0-9]+\.[0-9]+\.[0-9]+)`,
		"site/llms.txt":        `(?:This guide covers v|releases/tag/v|porcupine/v)([0-9]+\.[0-9]+\.[0-9]+)`,
		"docs/hosting.md":      `(?m)(?:^# Host Porcupine |releases/(?:tag|download)/v|ghcr\.io/olup/porcupine:|porcupine-)([0-9]+\.[0-9]+\.[0-9]+)`,
	}
	for file, pattern := range patterns {
		body, err := os.ReadFile(filepath.Join(root, file))
		if err != nil {
			return "", err
		}
		matches := regexp.MustCompile(pattern).FindAllStringSubmatch(string(body), -1)
		if len(matches) == 0 {
			return "", fmt.Errorf("%s: current release pointers must match VERSION %s", file, version)
		}
		for _, match := range matches {
			if match[1] != version {
				return "", fmt.Errorf("%s: current release pointers must match VERSION %s", file, version)
			}
		}
	}
	landing, err := os.ReadFile(filepath.Join(root, "site/index.html"))
	if err != nil {
		return "", err
	}
	if !strings.Contains(string(landing), "releases/tag/__PORCUPINE_RELEASE_TAG__") || !strings.Contains(string(landing), "Release __PORCUPINE_RELEASE_TAG__") {
		return "", errors.New("landing release link and label must use the generated version")
	}
	template, err := os.ReadFile(filepath.Join(root, "docs-theme/layouts/_default/baseof.html"))
	if err != nil {
		return "", err
	}
	if !strings.Contains(string(template), "Porcupine v{{ .Site.Params.release_version }}") {
		return "", errors.New("documentation badge must use the generated version")
	}
	return version, nil
}

func PublishedRelease(ctx context.Context, client *http.Client, apiURL, repository, token, version string) (bool, error) {
	if !stableVersion.MatchString(version) {
		return false, errors.New("invalid release version")
	}
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimSuffix(apiURL, "/")+"/repos/"+repository+"/releases/tags/v"+version, nil)
	if err != nil {
		return false, errors.New("invalid GitHub release API configuration")
	}
	request.Header.Set("Accept", "application/vnd.github+json")
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := client.Do(request)
	if err != nil {
		return false, errors.New("cannot check release publication")
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound {
		return false, nil
	}
	if response.StatusCode != http.StatusOK {
		return false, fmt.Errorf("cannot check release publication: HTTP %d", response.StatusCode)
	}
	var release struct {
		Draft      *bool `json:"draft"`
		Prerelease *bool `json:"prerelease"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&release); err != nil || release.Draft == nil || release.Prerelease == nil {
		return false, errors.New("invalid GitHub release publication response")
	}
	return !*release.Draft && !*release.Prerelease, nil
}
