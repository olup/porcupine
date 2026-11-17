package importer

import (
	"crypto/sha256"
	"encoding"
	"encoding/hex"
	"errors"
	stdhash "hash"
	"path"
	"sort"
	"strings"
	"unicode"
)

func NormalizeManifest(files []ImportUploadFileSpec) ([]ImportUploadFileSpec, error) {
	if len(files) == 0 || len(files) > MaxFiles {
		return nil, errors.New("manifest must contain 1 to 4096 files")
	}
	files = append([]ImportUploadFileSpec(nil), files...)
	seen := map[string]bool{}
	for i := range files {
		file := &files[i]
		if file.Path == "" || len(file.Path) > 1024 || strings.Contains(file.Path, "\\") || path.IsAbs(file.Path) || path.Clean(file.Path) != file.Path || file.Path == "." || strings.HasPrefix(file.Path, "../") || file.Path == ".." || strings.ContainsFunc(file.Path, unicode.IsControl) {
			return nil, errors.New("manifest paths must be clean relative file labels")
		}
		if file.Kind != "events" && file.Kind != "persons" || file.SizeBytes < 0 || !ValidSHA256(file.SHA256) || seen[file.Path] {
			return nil, errors.New("invalid file kind, size, SHA-256 or duplicate path")
		}
		file.SHA256 = strings.ToLower(file.SHA256)
		seen[file.Path] = true
	}
	sort.Slice(files, func(i, j int) bool {
		if files[i].Kind != files[j].Kind {
			return files[i].Kind == "persons"
		}
		return files[i].Path < files[j].Path
	})
	return files, nil
}

func ValidSHA256(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(value) == 64 && len(decoded) == 32
}

func HashState(h stdhash.Hash) ([]byte, error) {
	return h.(encoding.BinaryMarshaler).MarshalBinary()
}
func RestoreHash(state []byte) (stdhash.Hash, error) {
	h := sha256.New()
	if err := h.(encoding.BinaryUnmarshaler).UnmarshalBinary(state); err != nil {
		return nil, err
	}
	return h, nil
}
