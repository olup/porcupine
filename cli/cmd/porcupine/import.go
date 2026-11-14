package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
	"unicode"

	"github.com/urfave/cli/v3"
)

const importRowLimit = 1 << 20
const importFileLimit = 4096

type importCounts struct {
	Rows       int64 `json:"rows"`
	Accepted   int64 `json:"accepted"`
	Duplicates int64 `json:"duplicates"`
	Ignored    int64 `json:"ignored"`
	Unresolved int64 `json:"unresolved"`
	Invalid    int64 `json:"invalid"`
}
type importFileSpec struct {
	Path   string `json:"path"`
	Kind   string `json:"kind"`
	Size   int64  `json:"size_bytes"`
	SHA256 string `json:"sha256"`
}
type importFileProgress struct {
	importFileSpec
	ID     string `json:"id"`
	Offset int64  `json:"offset_bytes"`
	State  string `json:"state"`
}
type importJob struct {
	ID      string               `json:"id"`
	Project string               `json:"project_id"`
	Format  string               `json:"format"`
	State   string               `json:"state"`
	Counts  importCounts         `json:"counts"`
	Files   []importFileProgress `json:"files"`
	Samples []struct {
		FileID string `json:"file_id"`
		Row    int64  `json:"row_number"`
		Reason string `json:"reason"`
	} `json:"samples,omitempty"`
	Limits struct {
		ChunkBytes int `json:"max_chunk_bytes"`
		ChunkRows  int `json:"max_rows_per_chunk"`
	} `json:"limits"`
	Delivery *struct {
		Enabled     bool  `json:"enabled"`
		Pending     int64 `json:"pending"`
		Quarantined int64 `json:"quarantined"`
		Ready       bool  `json:"ready"`
	} `json:"delivery,omitempty"`
}
type importLocalFile struct {
	importFileSpec
	filename string
}

func importCommand() *cli.Command {
	return &cli.Command{Name: "import", Usage: "Import historical JSONL files with automatic upload, resume and analytical delivery checks", ArgsUsage: "FILE_OR_DIRECTORY", Flags: []cli.Flag{
		&cli.StringFlag{Name: "project", Required: true, Usage: "Destination project ID"},
		&cli.StringFlag{Name: "format", Required: true, Usage: "native, posthog or segment"},
		&cli.StringFlag{Name: "kind", Value: "auto", Usage: "auto, events or persons; auto recognizes PostHog persons paths"},
		&cli.StringFlag{Name: "resume", Usage: "Resume a server import ID with the same manifest; otherwise the file manifest selects a stable job"},
		&cli.BoolFlag{Name: "no-wait", Usage: "Finish after durable acceptance without waiting for analytical delivery"},
		&cli.DurationFlag{Name: "wait-timeout", Value: 5 * time.Minute, Usage: "Maximum wait for accepted events to become queryable"},
	}, Action: runImport}
}

func runImport(ctx context.Context, c *cli.Command) error {
	if c.NArg() != 1 {
		return fmt.Errorf("provide one JSONL file or directory, see import --help")
	}
	origin, err := parseInstanceOrigin(c.Root().String("url"))
	if err != nil {
		return err
	}
	project, format, kind := c.String("project"), c.String("format"), c.String("kind")
	if !importPathSegment(project) {
		return fmt.Errorf("--project requires a single path segment")
	}
	if format != "native" && format != "posthog" && format != "segment" {
		return fmt.Errorf("--format must be native, posthog or segment")
	}
	if kind != "auto" && kind != "events" && kind != "persons" {
		return fmt.Errorf("--kind must be auto, events or persons")
	}
	if kind == "persons" && format != "posthog" {
		return fmt.Errorf("persons files require --format posthog")
	}
	if c.Duration("wait-timeout") <= 0 {
		return fmt.Errorf("--wait-timeout must be positive")
	}
	fmt.Fprintln(c.Root().ErrWriter, "Validating files and computing checksums…")
	files, err := prepareImportFiles(ctx, c.Args().First(), format, kind)
	if err != nil {
		return err
	}
	specs := make([]importFileSpec, 0, len(files))
	for _, file := range files {
		specs = append(specs, file.importFileSpec)
	}
	manifest := struct {
		Format string           `json:"format"`
		Files  []importFileSpec `json:"files"`
	}{format, specs}
	canonical, _ := json.Marshal(manifest)
	sum := sha256.Sum256(canonical)
	client := importClient{origin: origin, client: &http.Client{Timeout: 2 * time.Minute, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
	parameters := map[string]string{"id": project}
	var job importJob
	if id := c.String("resume"); id != "" {
		parameters["import_id"] = id
		err = client.call(ctx, "get-import", parameters, nil, nil, &job)
	} else {
		body, _ := json.Marshal(struct {
			Format string           `json:"format"`
			Files  []importFileSpec `json:"files"`
			Key    string           `json:"idempotency_key"`
		}{format, specs, "cli-v1-" + hex.EncodeToString(sum[:])})
		err = client.call(ctx, "create-import", parameters, nil, body, &job)
	}
	if err != nil {
		return err
	}
	if err = validateImportJob(job, project, format, specs); err != nil {
		return err
	}
	parameters["import_id"] = job.ID
	fmt.Fprintf(c.Root().ErrWriter, "Import %s: %d files. Rerun the same command to resume.\n", job.ID, len(job.Files))
	for _, file := range job.Files {
		if file.State == "completed" {
			continue
		}
		var local importLocalFile
		for _, candidate := range files {
			if candidate.Path == file.Path {
				local = candidate
				break
			}
		}
		if err = client.upload(ctx, c, parameters, &job, local, file); err != nil {
			return fmt.Errorf("import %s paused; rerun the same command or use --resume %s: %w", job.ID, job.ID, err)
		}
	}
	if err = client.call(ctx, "complete-import", parameters, nil, nil, &job); err != nil {
		return fmt.Errorf("import %s: %w", job.ID, err)
	}
	if job.State != "completed" {
		return fmt.Errorf("server did not confirm import completion; inspect get-import for %s", job.ID)
	}
	fmt.Fprintf(c.Root().ErrWriter, "Accepted: %d; duplicates: %d; invalid: %d; unresolved identity: %d.\n", job.Counts.Accepted, job.Counts.Duplicates, job.Counts.Invalid, job.Counts.Unresolved)
	var finishErr error
	if job.Counts.Invalid > 0 {
		finishErr = fmt.Errorf("import completed with %d invalid rows; inspect samples, correct the files and rerun", job.Counts.Invalid)
	} else if !c.Bool("no-wait") {
		finishErr = client.wait(ctx, c, parameters, &job)
	}
	if err = json.NewEncoder(c.Root().Writer).Encode(job); err != nil {
		return err
	}
	return finishErr
}

func importPathSegment(value string) bool {
	return value != "" && value != "." && value != ".." && !strings.ContainsAny(value, "/\\") && !strings.ContainsFunc(value, unicode.IsControl)
}

func validateImportJob(job importJob, project, format string, files []importFileSpec) error {
	if !importPathSegment(job.ID) || job.Project != project || job.Format != format || len(job.Files) != len(files) {
		return fmt.Errorf("server import does not match the project, format and manifest; check the server version and --resume ID")
	}
	if job.State != "uploading" && job.State != "completed" {
		return fmt.Errorf("import %s is %s; inspect get-import before creating a new job", job.ID, job.State)
	}
	seen := map[string]bool{}
	for _, remote := range job.Files {
		matched := false
		for _, local := range files {
			if remote.importFileSpec == local {
				matched = true
				break
			}
		}
		if !matched || seen[remote.Path] || !importPathSegment(remote.ID) || remote.Offset < 0 || remote.Offset > remote.Size || remote.State == "completed" && remote.Offset != remote.Size || remote.State != "uploading" && remote.State != "completed" {
			return fmt.Errorf("server import manifest or progress differs from local files")
		}
		seen[remote.Path] = true
	}
	if job.Limits.ChunkBytes < 1 || job.Limits.ChunkBytes > 256<<10 || job.Limits.ChunkRows < 1 || job.Limits.ChunkRows > 1000 {
		return fmt.Errorf("server returned unsupported import limits")
	}
	return nil
}
