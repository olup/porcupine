package main

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestArchives(t *testing.T) {
	t.Parallel()
	for _, target := range []string{"darwin", "linux", "windows"} {
		t.Run(target, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			name := "porcupine"
			suffix := ".tar.gz"
			if target == "windows" {
				name += ".exe"
				suffix = ".zip"
			}
			binary := filepath.Join(dir, name)
			content := []byte("standalone executable fixture\x00\xff")
			if err := os.WriteFile(binary, content, 0755); err != nil {
				t.Fatal(err)
			}
			for _, arch := range []string{"amd64", "arm64"} {
				filename, err := archiveName(target, arch, "v1.2.3")
				if err != nil {
					t.Fatal(err)
				}
				if want := "porcupine-cli-v1.2.3-" + target + "-" + arch + suffix; filename != want {
					t.Fatalf("archive filename = %q, want %q", filename, want)
				}
				archive := filepath.Join(dir, filename)
				if err := writeArchive(binary, archive, target == "windows"); err != nil {
					t.Fatal(err)
				}
				if data := readArchivedBinary(t, archive, name); !bytes.Equal(data, content) {
					t.Fatal("archive changed binary contents")
				}
			}
		})
	}
}

func readArchivedBinary(t *testing.T, path, name string) []byte {
	t.Helper()
	if filepath.Ext(path) == ".zip" {
		zipped, err := zip.OpenReader(path)
		if err != nil {
			t.Fatal(err)
		}
		defer zipped.Close()
		if len(zipped.File) != 1 {
			t.Fatalf("zip has %d entries, want 1", len(zipped.File))
		}
		file := zipped.File[0]
		if file.Name != name || file.Mode() != 0755 {
			t.Fatalf("unexpected zip entry %q with mode %v", file.Name, file.Mode())
		}
		entry, err := file.Open()
		if err != nil {
			t.Fatal(err)
		}
		defer entry.Close()
		data, err := io.ReadAll(entry)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}

	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	compressed, err := gzip.NewReader(file)
	if err != nil {
		t.Fatal(err)
	}
	defer compressed.Close()
	archived := tar.NewReader(compressed)
	header, err := archived.Next()
	if err != nil {
		t.Fatal(err)
	}
	if header.Name != name || header.Mode != 0755 || header.Typeflag != tar.TypeReg {
		t.Fatalf("unexpected tar entry %q with mode %o and type %q", header.Name, header.Mode, header.Typeflag)
	}
	data, err := io.ReadAll(archived)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := archived.Next(); err != io.EOF {
		t.Fatalf("expected end of tar archive, got %v", err)
	}
	if _, err := io.Copy(io.Discard, compressed); err != nil {
		t.Fatal(err)
	}
	return data
}

func TestRejectInvalidRelease(t *testing.T) {
	t.Parallel()
	for _, version := range []string{"1.2.3", "v1.2", "v1.2.3 -X other=bad", "../../output"} {
		if _, err := archiveName("linux", "amd64", version); err == nil {
			t.Fatalf("accepted %q", version)
		}
	}
	if _, err := archiveName("linux", "invalid", "dev"); err == nil {
		t.Fatal("accepted architecture")
	}
	if _, err := archiveName("plan9", "amd64", "dev"); err == nil {
		t.Fatal("accepted target")
	}
}
