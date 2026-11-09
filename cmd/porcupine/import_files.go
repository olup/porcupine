package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

const (
	importChunkBytes = 256 << 10
	importChunkRows  = 1000
	importMaxFiles   = 4096
)

type importFile struct {
	Path   string `json:"path"`
	Kind   string `json:"kind"`
	Size   int64  `json:"size_bytes"`
	SHA256 string `json:"sha256"`
	local  string
}

type cancelableImportReader struct {
	ctx context.Context
	io.Reader
}

func (r cancelableImportReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.Reader.Read(p)
}

func (f importFile) open() (io.Reader, func(), error) {
	file, err := os.Open(f.local)
	if err != nil {
		return nil, nil, err
	}
	closeFile := func() { file.Close() }
	if strings.HasSuffix(strings.ToLower(f.Path), ".gz") {
		r, err := gzip.NewReader(file)
		if err != nil {
			closeFile()
			return nil, nil, err
		}
		return r, func() { r.Close(); closeFile() }, nil
	}
	return file, closeFile, nil
}

func importManifest(ctx context.Context, input, kind string) ([]importFile, error) {
	if kind != "auto" && kind != "events" && kind != "persons" {
		return nil, errors.New("--kind must be auto, events, or persons")
	}
	input = filepath.Clean(input)
	info, err := os.Lstat(input)
	if err != nil {
		return nil, err
	}
	var files []importFile
	err = filepath.WalkDir(input, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path != input && strings.HasPrefix(d.Name(), ".") {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if d.IsDir() {
			return nil
		}
		if !d.Type().IsRegular() {
			return fmt.Errorf("%s is not a regular file", path)
		}
		name := filepath.Base(path)
		if info.IsDir() {
			rel, err := filepath.Rel(input, path)
			if err != nil {
				return err
			}
			name = filepath.ToSlash(rel)
		}
		ext := filepath.Ext(strings.TrimSuffix(strings.ToLower(name), ".gz"))
		switch ext {
		case ".json", ".ndjson", ".jsonl", ".jsonlines":
		default:
			return fmt.Errorf("unsupported file %s: select a JSON Lines export (optionally gzip-compressed)", name)
		}
		if len(files) >= importMaxFiles {
			return fmt.Errorf("too many files (limit %d)", importMaxFiles)
		}
		f := importFile{Path: name, local: path, Kind: kind}
		if err := f.inspect(ctx); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		files = append(files, f)
		return nil
	})
	if err == nil && len(files) == 0 {
		return nil, errors.New("no export files found")
	}
	return files, err
}

func (f *importFile) inspect(ctx context.Context) error {
	r, closeFile, err := f.open()
	if err != nil {
		return err
	}
	defer closeFile()
	r = cancelableImportReader{ctx, r}
	h := sha256.New()
	if f.Kind == "auto" {
		f.Kind = "events"
		for _, part := range strings.Split(strings.ToLower(f.Path), "/") {
			if strings.SplitN(part, ".", 2)[0] == "persons" {
				f.Kind = "persons"
				break
			}
		}
		prefix, err := io.ReadAll(io.LimitReader(r, (1<<20)+1))
		if err != nil {
			return err
		}
		h.Write(prefix)
		f.Size = int64(len(prefix))
		for len(prefix) > 0 {
			line, rest, _ := bytes.Cut(prefix, []byte{'\n'})
			prefix = rest
			var row struct {
				Event      string `json:"event"`
				PersonID   string `json:"person_id"`
				DistinctID string `json:"distinct_id"`
			}
			if json.Unmarshal(line, &row) != nil {
				continue
			}
			if row.Event != "" {
				f.Kind = "events"
				break
			}
			if row.PersonID != "" && row.DistinctID != "" {
				f.Kind = "persons"
				break
			}
		}
	}
	n, err := io.Copy(h, r)
	if err != nil {
		return err
	}
	f.Size += n
	f.SHA256 = hex.EncodeToString(h.Sum(nil))
	return nil
}

// Fill one request while respecting the server's limits. Extra bytes remain in
// the buffered reader, including when a line spans several requests.
func readImportChunk(r *bufio.Reader, buffer []byte) (int, error) {
	n, lines := 0, 0
	for n < len(buffer) && lines < importChunkRows {
		if r.Buffered() == 0 {
			if _, err := r.Peek(1); err != nil {
				if errors.Is(err, io.EOF) {
					return n, nil
				}
				return 0, err
			}
		}
		b, _ := r.Peek(min(r.Buffered(), len(buffer)-n))
		take := len(b)
		for i, c := range b {
			if c == '\n' {
				lines++
				if lines == importChunkRows {
					take = i + 1
					break
				}
			}
		}
		copy(buffer[n:], b[:take])
		n += take
		_, _ = r.Discard(take)
	}
	return n, nil
}
