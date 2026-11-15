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
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

func prepareImportFiles(ctx context.Context, input, format, kind string) ([]importLocalFile, error) {
	info, err := os.Lstat(input)
	if err != nil {
		return nil, err
	}
	var files []importLocalFile
	add := func(filename, label string) error {
		if len(files) >= importFileLimit {
			return fmt.Errorf("import supports at most %d files", importFileLimit)
		}
		fileKind := kind
		if fileKind == "auto" {
			fileKind = "events"
			if format == "posthog" {
				for _, part := range strings.Split(strings.ToLower(filepath.ToSlash(label)), "/") {
					if part == "persons" || strings.HasPrefix(part, "persons.") || strings.HasPrefix(part, "persons-") {
						fileKind = "persons"
					}
				}
			}
		}
		file := importLocalFile{importFileSpec: importFileSpec{Path: filepath.ToSlash(label), Kind: fileKind}, filename: filename}
		if err := preflightImportFile(ctx, &file, format); err != nil {
			return err
		}
		files = append(files, file)
		return nil
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("import paths must not be symbolic links")
	}
	if info.IsDir() {
		err = filepath.WalkDir(input, func(filename string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if entry.Type()&os.ModeSymlink != 0 {
				return fmt.Errorf("symbolic link %q is not imported", filename)
			}
			if entry.IsDir() {
				return nil
			}
			name := strings.TrimSuffix(strings.ToLower(entry.Name()), ".gz")
			if !strings.HasSuffix(name, ".jsonl") && !strings.HasSuffix(name, ".ndjson") && !strings.HasSuffix(name, ".json") {
				return nil
			}
			label, err := filepath.Rel(input, filename)
			if err != nil {
				return err
			}
			return add(filename, label)
		})
	} else if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("import requires regular files")
	} else {
		err = add(input, filepath.Base(input))
	}
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no JSONL, NDJSON or JSON data files found")
	}
	sort.Slice(files, func(i, j int) bool {
		if files[i].Kind != files[j].Kind {
			return files[i].Kind == "persons"
		}
		return files[i].Path < files[j].Path
	})
	return files, nil
}

type importReadCloser struct {
	io.Reader
	close func() error
}

func (r *importReadCloser) Close() error { return r.close() }
func openImportFile(filename string) (io.ReadCloser, error) {
	info, err := os.Lstat(filename)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("import requires a regular file")
	}
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	current, err := file.Stat()
	if err != nil || !os.SameFile(info, current) {
		file.Close()
		return nil, fmt.Errorf("file changed while opening")
	}
	if !strings.HasSuffix(strings.ToLower(filename), ".gz") {
		return file, nil
	}
	reader, err := gzip.NewReader(file)
	if err != nil {
		file.Close()
		return nil, err
	}
	return &importReadCloser{Reader: reader, close: func() error { return errors.Join(reader.Close(), file.Close()) }}, nil
}

type importByteCounter struct{ n int64 }

func (w *importByteCounter) Write(p []byte) (int, error) { w.n += int64(len(p)); return len(p), nil }
func preflightImportFile(ctx context.Context, file *importLocalFile, format string) error {
	reader, err := openImportFile(file.filename)
	if err != nil {
		return err
	}
	defer reader.Close()
	digest := sha256.New()
	var counter importByteCounter
	scanner := bufio.NewScanner(io.TeeReader(reader, io.MultiWriter(digest, &counter)))
	scanner.Buffer(make([]byte, 64<<10), importRowLimit+2)
	var line int64
	for scanner.Scan() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		line++
		raw := bytes.TrimSpace(scanner.Bytes())
		if len(raw) == 0 {
			continue
		}
		if len(raw) > importRowLimit {
			return fmt.Errorf("%q line %d exceeds the 1 MiB row limit", file.Path, line)
		}
		if err := validateImportLine(raw, format, file.Kind); err != nil {
			return fmt.Errorf("%q line %d: %w", file.Path, line, err)
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("cannot validate %q near line %d: %w", file.Path, line+1, err)
	}
	file.Size = counter.n
	file.SHA256 = hex.EncodeToString(digest.Sum(nil))
	return nil
}
func validateImportLine(raw []byte, format, kind string) error {
	var object map[string]json.RawMessage
	if json.Unmarshal(raw, &object) != nil || object == nil {
		return fmt.Errorf("expected one JSON object per line; convert arrays or CSV to JSONL first")
	}
	required := []string{"uuid", "event", "distinct_id", "timestamp"}
	if kind == "persons" {
		required = []string{"person_id", "distinct_id"}
	} else if format == "native" {
		required = []string{"type", "message_id", "timestamp"}
	} else if format == "segment" {
		required = []string{"type", "messageId", "timestamp"}
	}
	for _, key := range required {
		var text string
		if json.Unmarshal(object[key], &text) != nil || strings.TrimSpace(text) == "" {
			return fmt.Errorf("%s must be a nonempty string", key)
		}
	}
	if kind == "events" {
		var timestamp string
		json.Unmarshal(object["timestamp"], &timestamp)
		_, err := time.Parse(time.RFC3339Nano, timestamp)
		if err != nil && format == "posthog" {
			for _, layout := range []string{"2006-01-02 15:04:05.999999999Z07:00", "2006-01-02 15:04:05.999999999"} {
				if _, e := time.Parse(layout, timestamp); e == nil {
					err = nil
					break
				}
			}
		}
		if err != nil {
			return fmt.Errorf("timestamp must contain the original occurrence time in RFC 3339 with a timezone")
		}
	}
	return nil
}
