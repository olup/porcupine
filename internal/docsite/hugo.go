package docsite

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

const HugoVersion = "0.170.0"

type hugoAsset struct{ Name, SHA256 string }

var hugoAssets = map[string]hugoAsset{
	"linux/amd64":   {"hugo_0.170.0_linux-amd64.tar.gz", "0930754ec22ad2dd1a252f8c31a5e57faccc9d15f1c3d381ddafcb96eeb8aef0"},
	"linux/arm64":   {"hugo_0.170.0_linux-arm64.tar.gz", "4ec3acd12ac17dbffb34fa453d3630dc0926a2314c8cd4964a550cfa9a4dbc73"},
	"darwin/amd64":  {"hugo_0.170.0_darwin-universal.pkg", "b9dfc437df19dfd44720caf2e0ba7f9ca5c2e61164f2f4c53456f74d6fa28d22"},
	"darwin/arm64":  {"hugo_0.170.0_darwin-universal.pkg", "b9dfc437df19dfd44720caf2e0ba7f9ca5c2e61164f2f4c53456f74d6fa28d22"},
	"windows/amd64": {"hugo_0.170.0_windows-amd64.zip", "56c2bc95f3fc0b6be060f0a4a23a0287fb3954c6595f6f170b49330366083df5"},
	"windows/arm64": {"hugo_0.170.0_windows-arm64.zip", "98dc85253cf260713a38121d4521af5c3657291950f438906b386543c9040dea"},
}

func hugoBinary(ctx context.Context) (string, error) {
	if binary := os.Getenv("HUGO_BINARY"); binary != "" {
		return binary, checkHugo(ctx, binary)
	}
	asset, ok := hugoAssets[runtime.GOOS+"/"+runtime.GOARCH]
	if !ok {
		return "", fmt.Errorf("no pinned Hugo binary for %s/%s; set HUGO_BINARY to Hugo %s", runtime.GOOS, runtime.GOARCH, HugoVersion)
	}
	cache, err := os.UserCacheDir()
	if err != nil {
		return "", err
	}
	cache = filepath.Join(cache, "porcupine", "hugo", HugoVersion, runtime.GOOS+"-"+runtime.GOARCH)
	binary := filepath.Join(cache, "hugo")
	if runtime.GOOS == "windows" {
		binary += ".exe"
	}
	if checkHugo(ctx, binary) == nil {
		return binary, nil
	}
	client := &http.Client{Timeout: 2 * time.Minute}
	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, "https://github.com/gohugoio/hugo/releases/download/v"+HugoVersion+"/"+asset.Name, nil)
	response, err := client.Do(request)
	if err != nil {
		return "", fmt.Errorf("download Hugo: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("download Hugo: HTTP %d", response.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, 80<<20))
	if err != nil {
		return "", err
	}
	if fmt.Sprintf("%x", sha256.Sum256(data)) != asset.SHA256 {
		return "", fmt.Errorf("Hugo archive checksum does not match pinned SHA256")
	}
	payload, err := extractHugo(ctx, asset.Name, data)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(cache, 0755); err != nil {
		return "", err
	}
	pattern := ".hugo-*"
	if runtime.GOOS == "windows" {
		pattern += ".exe"
	}
	file, err := os.CreateTemp(cache, pattern)
	if err != nil {
		return "", err
	}
	defer os.Remove(file.Name())
	if _, err = file.Write(payload); err != nil {
		file.Close()
		return "", err
	}
	if err = file.Chmod(0755); err != nil {
		file.Close()
		return "", err
	}
	if err = file.Close(); err != nil {
		return "", err
	}
	if err = checkHugo(ctx, file.Name()); err != nil {
		return "", err
	}
	if err = os.Rename(file.Name(), binary); err != nil {
		return "", err
	}
	return binary, nil
}

func checkHugo(ctx context.Context, binary string) error {
	output, err := exec.CommandContext(ctx, binary, "version").Output()
	if err != nil {
		return err
	}
	if !strings.HasPrefix(string(output), "hugo v"+HugoVersion+" ") && !strings.HasPrefix(string(output), "hugo v"+HugoVersion+"-") {
		return fmt.Errorf("expected Hugo %s, got %s", HugoVersion, strings.TrimSpace(string(output)))
	}
	return nil
}

func extractHugo(ctx context.Context, name string, data []byte) ([]byte, error) {
	switch {
	case strings.HasSuffix(name, ".tar.gz"):
		gz, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, err
		}
		defer gz.Close()
		archive := tar.NewReader(gz)
		for {
			header, err := archive.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
			if header.Typeflag == tar.TypeReg && header.Name == "hugo" {
				return io.ReadAll(io.LimitReader(archive, 150<<20))
			}
		}
	case strings.HasSuffix(name, ".zip"):
		archive, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			return nil, err
		}
		for _, file := range archive.File {
			if file.Name == "hugo.exe" {
				reader, err := file.Open()
				if err != nil {
					return nil, err
				}
				defer reader.Close()
				return io.ReadAll(io.LimitReader(reader, 150<<20))
			}
		}
	case strings.HasSuffix(name, ".pkg"):
		directory, err := os.MkdirTemp("", "porcupine-hugo-*")
		if err != nil {
			return nil, err
		}
		defer os.RemoveAll(directory)
		archive := filepath.Join(directory, name)
		if err := os.WriteFile(archive, data, 0600); err != nil {
			return nil, err
		}
		unpacked := filepath.Join(directory, "expanded")
		if output, err := exec.CommandContext(ctx, "pkgutil", "--expand-full", archive, unpacked).CombinedOutput(); err != nil {
			return nil, fmt.Errorf("extract Hugo with macOS pkgutil: %w: %s", err, output)
		}
		return os.ReadFile(filepath.Join(unpacked, "Payload", "hugo"))
	}
	return nil, fmt.Errorf("Hugo binary missing from %s", name)
}
