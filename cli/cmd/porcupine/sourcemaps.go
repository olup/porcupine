package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path"
	"slices"
	"strings"
	"time"
	"unicode"

	"github.com/urfave/cli/v3"
)

const (
	maxSourceMapBytes    = 2 << 20
	maxSourceBundleBytes = 32 << 20
)

var sourceMapUploadOperation = operation{Method: http.MethodPut, Path: "/api/v1/projects/{id}/source-maps"}

func uploadSourceMapsCommand() *cli.Command {
	return &cli.Command{
		Name:        "upload-source-maps",
		Usage:       "Upload JavaScript source maps before deploying a build",
		Description: "Pass --build and --asset-base together for build and asset URL matching. Omit both for PostHog artifacts with injected chunk IDs.",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "id", Usage: "Project ID", Required: true},
			&cli.StringFlag{Name: "dir", Usage: "Directory containing the built source maps", Required: true},
			&cli.StringFlag{Name: "build", Usage: "Build ID reported by the browser SDK"},
			&cli.StringFlag{Name: "asset-base", Usage: "Public URL prefix corresponding to --dir"},
			&cli.StringFlag{Name: "release", Usage: "Release name"},
		},
		Action: uploadSourceMaps,
	}
}

func uploadSourceMaps(ctx context.Context, c *cli.Command) error {
	origin, err := instanceOrigin(c)
	if err != nil {
		return err
	}
	project := c.String("id")
	if project == "" || project == "." || project == ".." || strings.ContainsAny(project, "/\\") || strings.ContainsFunc(project, unicode.IsControl) {
		return fmt.Errorf("--id requires a single path segment")
	}
	build, release := c.String("build"), c.String("release")
	if !validSourceMapHeader(build) || !validSourceMapHeader(release) {
		return fmt.Errorf("--build and --release cannot contain control characters or surrounding whitespace")
	}
	assetBase := c.String("asset-base")
	if (build == "") != (assetBase == "") {
		return fmt.Errorf("--build and --asset-base must be supplied together")
	}
	var base *url.URL
	if assetBase != "" {
		base, err = sourceMapAssetBase(assetBase)
		if err != nil {
			return err
		}
	}
	token, err := configuredAPIKey(c)
	if err != nil {
		return err
	}
	root, err := os.OpenRoot(c.String("dir"))
	if err != nil {
		return fmt.Errorf("open source map directory: %w", err)
	}
	defer root.Close()

	maps, chunks, err := findSourceMaps(ctx, root, base == nil)
	if err != nil {
		return err
	}
	if len(maps) == 0 {
		return fmt.Errorf("no source maps found in --dir")
	}

	endpoint := origin + strings.Replace(sourceMapUploadOperation.Path, "{id}", url.PathEscape(project), 1)
	client := &http.Client{Timeout: 2 * time.Minute, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	count := 0
	for _, name := range maps {
		if err := ctx.Err(); err != nil {
			return err
		}
		raw, err := readSourceArtifact(root, name, maxSourceMapBytes)
		if err != nil {
			return err
		}
		var metadata struct {
			Version int    `json:"version"`
			ChunkID string `json:"chunk_id"`
			CamelID string `json:"chunkId"`
		}
		if json.Unmarshal(raw, &metadata) != nil || metadata.Version != 3 {
			return fmt.Errorf("%q is not a version 3 JSON source map", name)
		}
		headers := http.Header{}
		if release != "" {
			headers.Set("X-Source-Map-Release", release)
		}
		selectors := []string{""}
		if base != nil {
			assetName := strings.TrimSuffix(name, ".map")
			if assetName == "" || assetName != path.Clean(assetName) || !fs.ValidPath(assetName) || strings.Contains(assetName, "\\") {
				return fmt.Errorf("%q does not map to a valid asset path", name)
			}
			asset := *base
			asset.Path += assetName
			asset.RawPath += (&url.URL{Path: assetName}).EscapedPath()
			headers.Set("X-Source-Map-Build-ID", build)
			headers.Set("X-Source-Map-Asset-URL", asset.String())
		} else {
			selectors = chunks[name]
			if len(selectors) == 0 {
				if metadata.ChunkID != "" && metadata.CamelID != "" && metadata.ChunkID != metadata.CamelID {
					return fmt.Errorf("%q has conflicting PostHog chunk IDs", name)
				}
				chunk := metadata.ChunkID
				if chunk == "" {
					chunk = metadata.CamelID
				}
				if chunk == "" {
					continue
				}
				selectors = []string{chunk}
			}
		}
		for _, chunk := range selectors {
			if chunk != "" {
				if !validSourceMapHeader(chunk) {
					return fmt.Errorf("%q has an invalid PostHog chunk ID", name)
				}
				headers.Set("X-Source-Map-Chunk-ID", chunk)
			}
			if token == "" {
				token, err = accessToken(ctx, origin)
				if err != nil {
					return err
				}
			}
			if err := putSourceMap(ctx, client, endpoint, token, headers, raw); err != nil {
				return fmt.Errorf("%q: %w", name, err)
			}
			count++
		}
	}
	if count == 0 {
		return fmt.Errorf("no PostHog chunk IDs found in --dir")
	}
	_, err = fmt.Fprintf(c.Root().Writer, "Uploaded %d source map(s).\n", count)
	return err
}

func validSourceMapHeader(value string) bool {
	return value == strings.TrimSpace(value) && !strings.ContainsFunc(value, unicode.IsControl)
}

func sourceMapAssetBase(raw string) (*url.URL, error) {
	base, err := url.Parse(raw)
	if err != nil || base.Hostname() == "" || base.User != nil || base.Opaque != "" ||
		(base.Scheme != "http" && base.Scheme != "https") || strings.ContainsAny(raw, "?#") ||
		strings.Contains(base.Path, "\\") || strings.ContainsFunc(base.Path, unicode.IsControl) ||
		strings.Contains(strings.ToLower(base.EscapedPath()), "%2f") ||
		(base.Path != "" && base.Path != "/" && path.Clean(base.Path) != strings.TrimSuffix(base.Path, "/")) {
		return nil, fmt.Errorf("--asset-base requires an absolute HTTP or HTTPS URL without credentials, a query, or a fragment")
	}
	base.RawPath = strings.TrimSuffix(base.EscapedPath(), "/") + "/"
	base.Path = strings.TrimSuffix(base.Path, "/") + "/"
	return base, nil
}

func findSourceMaps(ctx context.Context, root *os.Root, posthog bool) ([]string, map[string][]string, error) {
	var maps []string
	mapNames := make(map[string]bool)
	type bundleChunk struct {
		name, referencedMap, chunk string
	}
	var bundles []bundleChunk
	err := fs.WalkDir(root.FS(), ".", func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if !entry.Type().IsRegular() {
			return nil
		}
		switch path.Ext(name) {
		case ".map":
			maps = append(maps, name)
			mapNames[name] = true
		case ".js", ".mjs", ".cjs":
			if !posthog {
				return nil
			}
			raw, err := readSourceArtifact(root, name, maxSourceBundleBytes)
			if err != nil {
				return err
			}
			if mapName, chunk := postHogSourcePair(name, raw); chunk != "" {
				bundles = append(bundles, bundleChunk{name, mapName, chunk})
			}
		}
		return nil
	})
	chunks := make(map[string][]string)
	for _, bundle := range bundles {
		name := bundle.referencedMap
		if !mapNames[name] {
			name = bundle.name + ".map"
		}
		if mapNames[name] && !slices.Contains(chunks[name], bundle.chunk) {
			chunks[name] = append(chunks[name], bundle.chunk)
		}
	}
	return maps, chunks, err
}

func postHogSourcePair(name string, raw []byte) (string, string) {
	var chunk, reference string
	for line := range bytes.SplitSeq(raw, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if value, ok := bytes.CutPrefix(line, []byte("//# chunkId=")); ok {
			chunk = string(bytes.TrimSpace(value))
		}
		for _, prefix := range []string{"//# sourceMappingURL=", "//@ sourceMappingURL="} {
			if value, ok := bytes.CutPrefix(line, []byte(prefix)); ok {
				reference = string(bytes.TrimSpace(value))
			}
		}
	}
	if chunk == "" || reference == "" {
		return "", chunk
	}
	ref, err := url.Parse(reference)
	if err != nil || ref.Host != "" || ref.IsAbs() || ref.Path == "" || strings.HasPrefix(ref.Path, "/") || strings.Contains(ref.Path, "\\") {
		return "", chunk
	}
	return path.Join(path.Dir(name), ref.Path), chunk
}

func readSourceArtifact(root *os.Root, name string, maxBytes int64) ([]byte, error) {
	info, err := root.Lstat(name)
	if err != nil {
		return nil, fmt.Errorf("inspect %q: %w", name, err)
	}
	if !info.Mode().IsRegular() || info.Size() > maxBytes {
		return nil, fmt.Errorf("%q must be a regular file no larger than %d bytes", name, maxBytes)
	}
	file, err := root.Open(name)
	if err != nil {
		return nil, fmt.Errorf("open %q: %w", name, err)
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, maxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read %q: %w", name, err)
	}
	if int64(len(raw)) > maxBytes {
		return nil, fmt.Errorf("%q exceeds %d bytes", name, maxBytes)
	}
	return raw, nil
}

func putSourceMap(ctx context.Context, client *http.Client, endpoint, token string, headers http.Header, raw []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header = headers.Clone()
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return responseError(sourceMapUploadOperation, resp, req.Header.Get("Authorization"))
	}
	const maxResponseBytes = 64 << 10
	result, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil || len(result) > maxResponseBytes || !json.Valid(result) {
		return fmt.Errorf("invalid source map upload response")
	}
	return nil
}
