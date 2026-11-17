package main

import (
	"crypto/sha256"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestArchives(t *testing.T) {
	t.Parallel()
	for _, target := range []string{"darwin", "linux", "windows"} {
		t.Run(target, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			name := "porcupine"
			if target == "windows" {
				name += ".exe"
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
				if !strings.HasPrefix(filename, "porcupine-cli-v1.2.3-"+target+"-"+arch) {
					t.Fatal(filename)
				}
				archive := filepath.Join(dir, filename)
				if err := writeArchive(binary, archive, target == "windows"); err != nil {
					t.Fatal(err)
				}
				out := filepath.Join(t.TempDir(), name)
				if err := extractArchive(archive, out, target == "windows"); err != nil {
					t.Fatal(err)
				}
				data, err := os.ReadFile(out)
				if err != nil {
					t.Fatal(err)
				}
				if sha256.Sum256(data) != sha256.Sum256(content) {
					t.Fatal("archive changed binary checksum")
				}
				stat, err := os.Stat(out)
				if err != nil {
					t.Fatal(err)
				}
				if runtime.GOOS != "windows" && stat.Mode().Perm() != 0755 {
					t.Fatalf("mode %v", stat.Mode())
				}
			}
		})
	}
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
