package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/urfave/cli/v3"
)

type importClient struct {
	origin string
	client *http.Client
}
type importHTTPError struct {
	status int
	delay  time.Duration
	err    error
}

func (e *importHTTPError) Error() string { return e.err.Error() }
func (i *importClient) call(ctx context.Context, id string, parameters map[string]string, headers map[string]string, body []byte, result *importJob) error {
	var op operation
	for _, candidate := range operations {
		if candidate.ID == id {
			op = candidate
			break
		}
	}
	if op.ID == "" {
		return fmt.Errorf("unknown API operation %s", id)
	}
	path := op.Path
	for key, value := range parameters {
		if !importPathSegment(value) {
			return fmt.Errorf("invalid import path parameter")
		}
		path = strings.ReplaceAll(path, "{"+key+"}", url.PathEscape(value))
	}
	if strings.Contains(path, "{") {
		return fmt.Errorf("missing import path parameter")
	}
	var last error
	for attempt := 0; attempt < 8; attempt++ {
		token := os.Getenv("PORCUPINE_API_KEY")
		var err error
		if token == "" {
			token, err = accessToken(ctx, i.origin)
			if err != nil {
				return err
			}
		}
		req, err := http.NewRequestWithContext(ctx, op.Method, i.origin+path, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Accept", "application/json")
		if len(body) > 0 {
			req.Header.Set("Content-Type", op.ContentType)
		}
		for name, value := range headers {
			req.Header.Set(name, value)
		}
		response, err := i.client.Do(req)
		delay := min(time.Duration(1<<attempt)*200*time.Millisecond, 5*time.Second)
		if err == nil {
			if response.StatusCode >= 200 && response.StatusCode < 300 {
				raw, readErr := io.ReadAll(io.LimitReader(response.Body, 8<<20+1))
				response.Body.Close()
				if readErr == nil && len(raw) <= 8<<20 {
					if err := json.Unmarshal(raw, result); err != nil {
						return fmt.Errorf("invalid import response")
					}
					return nil
				}
				err = fmt.Errorf("incomplete or oversized import response")
			} else {
				failure := &importHTTPError{status: response.StatusCode, err: responseError(op, response, req.Header.Get("Authorization"))}
				if seconds, e := strconv.Atoi(response.Header.Get("Retry-After")); e == nil && seconds > 0 {
					failure.delay = time.Duration(seconds) * time.Second
				}
				response.Body.Close()
				if failure.status != 408 && failure.status != 429 && failure.status != 500 && failure.status != 502 && failure.status != 503 && failure.status != 504 {
					return failure
				}
				if failure.delay > 30*time.Second {
					return failure
				}
				if failure.delay > delay {
					delay = failure.delay
				}
				err = failure
			}
		}
		last = err
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if attempt == 7 {
			break
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return last
}

func (i *importClient) upload(ctx context.Context, c *cli.Command, parameters map[string]string, job *importJob, local importLocalFile, file importFileProgress) error {
	// Each invocation reopens the original stream and skips the authoritative
	// decompressed byte offset. No local cursor or credential sidecar is needed.
	reader, err := openImportFile(local.filename)
	if err != nil {
		return err
	}
	defer reader.Close()
	if _, err = io.CopyN(io.Discard, reader, file.Offset); err != nil {
		return fmt.Errorf("local file is shorter than its acknowledged offset")
	}
	buffered := bufio.NewReaderSize(reader, job.Limits.ChunkBytes)
	parameters["file_id"] = file.ID
	defer delete(parameters, "file_id")
	for offset := file.Offset; offset < file.Size; {
		chunk, err := readImportChunk(buffered, job.Limits.ChunkBytes, job.Limits.ChunkRows)
		if err != nil {
			return err
		}
		if len(chunk) == 0 {
			return fmt.Errorf("local file changed after validation")
		}
		sum := sha256.Sum256(chunk)
		headers := map[string]string{"Upload-Offset": strconv.FormatInt(offset, 10), "X-Chunk-SHA256": hex.EncodeToString(sum[:])}
		err = i.call(ctx, "upload-import-chunk", parameters, headers, chunk, job)
		if err != nil {
			return err
		}
		expected := offset + int64(len(chunk))
		acknowledged := false
		for _, remote := range job.Files {
			if remote.ID == file.ID && remote.Offset == expected {
				acknowledged = true
			}
		}
		if !acknowledged {
			return fmt.Errorf("server acknowledgement differs from the submitted chunk; rerun to reload its authoritative offset")
		}
		offset = expected
		fmt.Fprintf(c.Root().ErrWriter, "%q: %d/%d bytes accepted\n", file.Path, offset, file.Size)
	}
	return i.call(ctx, "complete-import-file", parameters, nil, nil, job)
}
func readImportChunk(reader *bufio.Reader, maxBytes, maxRows int) ([]byte, error) {
	data, err := reader.Peek(maxBytes)
	if err != nil && err != io.EOF {
		return nil, err
	}
	end := len(data)
	rows := 0
	for pos, value := range data {
		if value == '\n' {
			rows++
			if rows == maxRows {
				end = pos + 1
				break
			}
		}
	}
	chunk := bytes.Clone(data[:end])
	_, err = reader.Discard(end)
	return chunk, err
}
func (i *importClient) wait(ctx context.Context, c *cli.Command, parameters map[string]string, job *importJob) error {
	wait, cancel := context.WithTimeout(ctx, c.Duration("wait-timeout"))
	defer cancel()
	for {
		if job.Delivery == nil {
			return fmt.Errorf("import accepted, but this server does not report analytical readiness")
		}
		if !job.Delivery.Enabled {
			return fmt.Errorf("import accepted, but analytical storage is not configured; use --no-wait for acceptance only")
		}
		if job.Delivery.Quarantined > 0 {
			return fmt.Errorf("import accepted, but analytical delivery is blocked by quarantined events")
		}
		if job.Delivery.Ready && job.Delivery.Pending == 0 {
			fmt.Fprintln(c.Root().ErrWriter, "Import is queryable.")
			return nil
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-wait.Done():
			timer.Stop()
			return fmt.Errorf("import accepted; analytical delivery is still pending. Rerun with --resume %s to check again", job.ID)
		case <-timer.C:
		}
		if err := i.call(wait, "get-import", parameters, nil, nil, job); err != nil {
			return err
		}
	}
}
