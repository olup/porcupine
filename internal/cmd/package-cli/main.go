// Command package-cli builds the standalone CLI archives without server or C dependencies.
package main

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
)

var releaseVersion = regexp.MustCompile(`^(v[0-9]+\.[0-9]+\.[0-9]+|dev)$`)

func main() {
	if len(os.Args) != 4 {
		fatal(fmt.Errorf("usage: package-cli darwin|linux|windows vMAJOR.MINOR.PATCH|dev output"))
	}
	if err := run(os.Args[1], os.Args[2], os.Args[3]); err != nil {
		fatal(err)
	}
}
func fatal(err error) { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
func archiveName(target, arch, version string) (string, error) {
	if target != "darwin" && target != "linux" && target != "windows" {
		return "", fmt.Errorf("unsupported target %q", target)
	}
	if arch != "amd64" && arch != "arm64" {
		return "", fmt.Errorf("unsupported architecture %q", arch)
	}
	if !releaseVersion.MatchString(version) {
		return "", fmt.Errorf("expected a vMAJOR.MINOR.PATCH tag or dev")
	}
	suffix := ".tar.gz"
	if target == "windows" {
		suffix = ".zip"
	}
	return fmt.Sprintf("porcupine-cli-%s-%s-%s%s", version, target, arch, suffix), nil
}
func run(target, version, output string) error {
	if _, err := archiveName(target, "amd64", version); err != nil {
		return err
	}
	if err := os.MkdirAll(output, 0755); err != nil {
		return err
	}
	for _, arch := range []string{"amd64", "arm64"} {
		if err := buildArchive(target, arch, version, output); err != nil {
			return err
		}
	}
	return nil
}
func buildArchive(target, arch, version, output string) error {
	dir, err := os.MkdirTemp("", "porcupine-cli-package-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	name := "porcupine"
	if target == "windows" {
		name += ".exe"
	}
	binary := filepath.Join(dir, name)
	cmd := exec.Command("go", "build", "-trimpath", "-buildvcs=false", "-ldflags", "-s -w -X main.version="+version, "-o", binary, "./cmd/porcupine")
	cmd.Dir = "cli"
	cmd.Env = append(os.Environ(), "GOOS="+target, "GOARCH="+arch, "CGO_ENABLED=0")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return err
	}
	filename, _ := archiveName(target, arch, version)
	archive := filepath.Join(output, filename)
	if err := writeArchive(binary, archive, target == "windows"); err != nil {
		return err
	}
	// Run the extracted artifact, so packaging failures cannot hide behind a working build.
	if err := os.Remove(binary); err != nil {
		return err
	}
	if err := extractArchive(archive, binary, target == "windows"); err != nil {
		return err
	}
	if target == runtime.GOOS && arch == runtime.GOARCH {
		data, err := exec.Command(binary, "--version").CombinedOutput()
		if err != nil {
			return fmt.Errorf("archive version: %w: %s", err, data)
		}
		if strings.TrimSpace(string(data)) != "porcupine version "+version {
			return fmt.Errorf("unexpected archive version: %s", data)
		}
		if data, err := exec.Command(binary, "list-projects", "--help").CombinedOutput(); err != nil {
			return fmt.Errorf("archive help: %w: %s", err, data)
		}
	}
	fmt.Println(archive)
	return nil
}
func writeArchive(binary, path string, windows bool) (err error) {
	input, err := os.Open(binary)
	if err != nil {
		return err
	}
	defer input.Close()
	info, err := input.Stat()
	if err != nil {
		return err
	}
	output, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() {
		if e := output.Close(); err == nil {
			err = e
		}
	}()
	if windows {
		writer := zip.NewWriter(output)
		defer func() {
			if e := writer.Close(); err == nil {
				err = e
			}
		}()
		header := &zip.FileHeader{Name: filepath.Base(binary), Method: zip.Deflate}
		header.SetMode(0755)
		entry, e := writer.CreateHeader(header)
		if e != nil {
			return e
		}
		_, err = io.Copy(entry, input)
		return err
	}
	compressed := gzip.NewWriter(output)
	defer func() {
		if e := compressed.Close(); err == nil {
			err = e
		}
	}()
	writer := tar.NewWriter(compressed)
	defer func() {
		if e := writer.Close(); err == nil {
			err = e
		}
	}()
	if err = writer.WriteHeader(&tar.Header{Name: filepath.Base(binary), Size: info.Size(), Mode: 0755, Typeflag: tar.TypeReg}); err != nil {
		return err
	}
	_, err = io.Copy(writer, input)
	return err
}
func extractArchive(path, binary string, windows bool) (err error) {
	var reader io.Reader
	var closer io.Closer
	if windows {
		archive, e := zip.OpenReader(path)
		if e != nil {
			return e
		}
		defer archive.Close()
		if len(archive.File) != 1 || archive.File[0].Name != filepath.Base(binary) {
			return fmt.Errorf("invalid archive entries")
		}
		entry, e := archive.File[0].Open()
		if e != nil {
			return e
		}
		reader, closer = entry, entry
	} else {
		file, e := os.Open(path)
		if e != nil {
			return e
		}
		defer file.Close()
		compressed, e := gzip.NewReader(file)
		if e != nil {
			return e
		}
		closer = compressed
		archive := tar.NewReader(compressed)
		header, e := archive.Next()
		if e != nil {
			return e
		}
		if header.Name != filepath.Base(binary) || header.Mode != 0755 {
			return fmt.Errorf("invalid archive executable")
		}
		reader = archive
	}
	defer closer.Close()
	output, err := os.OpenFile(binary, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0755)
	if err != nil {
		return err
	}
	defer func() {
		if e := output.Close(); err == nil {
			err = e
		}
	}()
	_, err = io.Copy(output, reader)
	return err
}
