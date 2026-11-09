package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type importRemoteFile struct {
	importFile
	ID     string `json:"id"`
	State  string `json:"state"`
	Offset int64  `json:"offset_bytes"`
}

type importJob struct {
	ID      string             `json:"id"`
	Project string             `json:"project_id"`
	State   string             `json:"state"`
	Files   []importRemoteFile `json:"files"`
	Counts  map[string]int64   `json:"counts"`
	Samples []json.RawMessage  `json:"samples,omitempty"`
}

type importHTTP struct {
	origin           string
	client           *http.Client
	token            string
	clientID, secret string
}

func runImport(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 || args[0] != "posthog" {
		return errors.New("usage: porcupine import posthog PATH --project ID --url ORIGIN")
	}
	flags := flag.NewFlagSet("porcupine import posthog", flag.ContinueOnError)
	flags.SetOutput(stderr)
	project := flags.String("project", "", "target project ID")
	origin := flags.String("url", os.Getenv("PORCUPINE_URL"), "Porcupine origin")
	resume := flags.String("resume", "", "existing import ID to verify and resume")
	kind := flags.String("kind", "auto", "events, persons or auto")
	args = args[1:]
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		args = append(append([]string{}, args[1:]...), args[0])
	}
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if flags.NArg() != 1 || !safeImportID(*project) || (*resume != "" && !safeImportID(*resume)) {
		return errors.New("provide one file or directory, --project ID, and a valid --resume ID if applicable")
	}
	u, err := url.Parse(*origin)
	if err != nil || u.User != nil || u.Host == "" || u.Opaque != "" || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("--url or PORCUPINE_URL must be a server origin")
	}
	host := u.Hostname()
	if u.Scheme != "https" && !(u.Scheme == "http" && (host == "localhost" || net.ParseIP(host).IsLoopback())) {
		return errors.New("use HTTPS or a loopback HTTP origin")
	}
	h := importHTTP{origin: strings.TrimSuffix(*origin, "/"), client: &http.Client{Timeout: 60 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, token: os.Getenv("PORCUPINE_ACCESS_TOKEN"), clientID: os.Getenv("PORCUPINE_CLIENT_ID"), secret: os.Getenv("PORCUPINE_CLIENT_SECRET")}
	if h.token == "" && (h.clientID == "" || h.secret == "") {
		return errors.New("set PORCUPINE_ACCESS_TOKEN or PORCUPINE_CLIENT_ID and PORCUPINE_CLIENT_SECRET")
	}
	files, err := importManifest(ctx, flags.Arg(0), *kind)
	if err != nil {
		return err
	}
	base := "/api/v1/projects/" + *project + "/imports"
	var job importJob
	if *resume != "" {
		err = h.jobRequest(ctx, "GET", base+"/"+*resume, nil, nil, &job)
	} else {
		raw, _ := json.Marshal(files)
		sum := sha256.Sum256(raw)
		body, _ := json.Marshal(map[string]any{"files": files, "idempotency_key": "cli-" + hex.EncodeToString(sum[:])})
		err = h.jobRequest(ctx, "POST", base, body, nil, &job)
	}
	if err != nil {
		return err
	}
	if !safeImportID(job.ID) || job.Project != *project {
		return errors.New("invalid import job response")
	}
	fmt.Fprintf(stderr, "Import %s in project %s\n", job.ID, *project)
	base += "/" + job.ID
	local := map[string]importFile{}
	for _, f := range files {
		local[f.Path] = f
	}
	if len(job.Files) != len(files) {
		return errors.New("import manifest differs from local input")
	}
	seen := map[string]bool{}
	for _, remote := range job.Files {
		f, ok := local[remote.Path]
		if !ok || f.Kind != remote.Kind || f.Size != remote.Size || f.SHA256 != remote.SHA256 || !safeImportID(remote.ID) || remote.Offset < 0 || remote.Offset > f.Size {
			return errors.New("server manifest or offset differs from local input")
		}
		if seen[remote.Path] {
			return errors.New("unexpected repeated path in import response")
		}
		seen[remote.Path] = true
	}
	if job.State == "failed" || job.State == "cancelled" {
		return fmt.Errorf("import %s is %s", job.ID, job.State)
	}
	if job.State != "completed" {
		// Snapshot the required order; responses also carry the updated job.
		order := append([]importRemoteFile(nil), job.Files...)
		for _, remote := range order {
			if remote.State == "completed" {
				continue
			}
			if err := h.upload(ctx, base, remote, local[remote.Path], &job); err != nil {
				return fmt.Errorf("file %s: %w", remote.Path, err)
			}
		}
		if err := h.jobRequest(ctx, "POST", base+"/complete", nil, nil, &job); err != nil {
			return err
		}
	}
	if job.State != "completed" {
		return fmt.Errorf("unexpected final import state %q", job.State)
	}
	if job.Counts["invalid"] > 0 || job.Counts["unresolved"] > 0 {
		fmt.Fprintf(stderr, "Import completed with %d invalid rows and %d unresolved identities; see the result for samples.\n", job.Counts["invalid"], job.Counts["unresolved"])
	}
	return json.NewEncoder(stdout).Encode(job)
}

func safeImportID(s string) bool {
	if len(s) == 0 || len(s) > 128 {
		return false
	}
	for _, c := range s {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '-') {
			return false
		}
	}
	return true
}

func (h *importHTTP) upload(ctx context.Context, base string, remote importRemoteFile, f importFile, job *importJob) error {
	r, closeFile, err := f.open()
	if err != nil {
		return err
	}
	defer closeFile()
	r = cancelableImportReader{ctx, r}
	if _, err = io.CopyN(io.Discard, r, remote.Offset); err != nil {
		return fmt.Errorf("cannot resume local file: %w", err)
	}
	reader := bufio.NewReaderSize(r, 64<<10)
	buf := make([]byte, importChunkBytes)
	path := base + "/files/" + remote.ID
	offset := remote.Offset
	for offset < f.Size {
		n, err := readImportChunk(reader, buf)
		if err != nil {
			return err
		}
		if n == 0 || int64(n) > f.Size-offset {
			return errors.New("file changed during upload")
		}
		sum := sha256.Sum256(buf[:n])
		headers := map[string]string{"Upload-Offset": strconv.FormatInt(offset, 10), "X-Chunk-SHA256": hex.EncodeToString(sum[:])}
		if err = h.jobRequest(ctx, "PUT", path, buf[:n], headers, job); err != nil {
			return err
		}
		offset += int64(n)
		var ack bool
		for _, update := range job.Files {
			if update.ID == remote.ID && update.Offset == offset {
				ack = true
				break
			}
		}
		if !ack {
			return errors.New("server did not confirm the uploaded offset; resume the job")
		}
	}
	if _, err = reader.Peek(1); !errors.Is(err, io.EOF) {
		return errors.New("file changed or failed validation during upload")
	}
	return h.jobRequest(ctx, "POST", path+"/complete", nil, nil, job)
}

func (h *importHTTP) refresh(ctx context.Context) error {
	form := url.Values{"grant_type": {"client_credentials"}, "resource": {h.origin + "/api/v1"}, "scope": {"imports:write events:write"}}
	req, err := http.NewRequestWithContext(ctx, "POST", h.origin+"/oauth/token", strings.NewReader(form.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth(url.QueryEscape(h.clientID), url.QueryEscape(h.secret))
	res, err := h.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return fmt.Errorf("OAuth token exchange returned HTTP %d", res.StatusCode)
	}
	var token struct {
		Access string `json:"access_token"`
	}
	if err = json.NewDecoder(io.LimitReader(res.Body, 64<<10)).Decode(&token); err != nil || token.Access == "" {
		return errors.New("invalid OAuth token response")
	}
	h.token = token.Access
	return nil
}

func (h *importHTTP) jobRequest(ctx context.Context, method, path string, body []byte, headers map[string]string, job *importJob) error {
	refreshed := false
	if h.token == "" {
		if err := h.refresh(ctx); err != nil {
			return err
		}
		refreshed = true
	}
	for attempt := 0; ; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, h.origin+path, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+h.token)
		if method == "PUT" {
			req.Header.Set("Content-Type", "application/octet-stream")
		} else if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		res, err := h.client.Do(req)
		if err == nil {
			if res.StatusCode >= 200 && res.StatusCode < 300 {
				var result importJob
				err = json.NewDecoder(io.LimitReader(res.Body, 8<<20)).Decode(&result)
				res.Body.Close()
				if err != nil {
					return fmt.Errorf("invalid import response: %w", err)
				}
				*job = result
				return nil
			}
			// Errors expose status only; the body could contain credentials or row data.
			res.Body.Close()
			if res.StatusCode == 401 && !refreshed && h.clientID != "" && h.secret != "" {
				if err = h.refresh(ctx); err != nil {
					return err
				}
				refreshed = true
				continue
			}
			err = fmt.Errorf("%s %s returned HTTP %d", method, path, res.StatusCode)
			if res.StatusCode != 429 && res.StatusCode != 503 && res.StatusCode != 502 && res.StatusCode != 504 {
				return err
			}
		}
		if attempt >= 5 {
			return fmt.Errorf("%w; rerun with the same input to resume", err)
		}
		timer := time.NewTimer(time.Second << min(attempt, 3))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
