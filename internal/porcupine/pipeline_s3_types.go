package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"net"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	chsql "github.com/AfterShip/clickhouse-sql-parser/parser"
)

type S3PipelineSpec struct {
	SQL             string                `json:"sql" minLength:"1" maxLength:"16384"`
	Parameters      map[string]string     `json:"parameters,omitempty"`
	Format          string                `json:"format,omitempty" enum:"jsonl,csv"`
	IntervalSeconds int64                 `json:"interval_seconds" minimum:"60" maximum:"86400"`
	StartAt         string                `json:"start_at" doc:"UTC RFC3339 completed-window cursor, aligned to interval_seconds since Unix epoch."`
	Destination     S3PipelineDestination `json:"destination"`
}
type S3PipelineDestination struct {
	Bucket       string `json:"bucket"`
	Region       string `json:"region"`
	Prefix       string `json:"prefix,omitempty" doc:"Destination directory; defaults to porcupine."`
	Endpoint     string `json:"endpoint,omitempty"`
	UsePathStyle bool   `json:"use_path_style,omitempty"`
}
type S3PipelineSecret struct {
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
	SessionToken    string `json:"session_token,omitempty"`
}

var s3PipelineSegment = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)
var s3PipelineBucket = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)
var s3PipelineRegion = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,63}$`)

func validateS3PipelineSecret(c S3PipelineSecret) error {
	if strings.TrimSpace(c.AccessKeyID) == "" || strings.TrimSpace(c.SecretAccessKey) == "" || len(c.AccessKeyID) > 1024 || len(c.SecretAccessKey) > 4096 || len(c.SessionToken) > 16384 {
		return errors.New("explicit S3 credentials are required")
	}
	return nil
}
func normalizeS3PipelineSpec(c S3PipelineSpec) (S3PipelineSpec, error) {
	fail := func() (S3PipelineSpec, error) { return c, errors.New("invalid S3 pipeline specification") }
	if c.Format == "" {
		c.Format = "jsonl"
	}
	if c.Format != "jsonl" && c.Format != "csv" {
		return fail()
	}
	if c.IntervalSeconds < 60 || c.IntervalSeconds > 86400 {
		return fail()
	}
	start, err := time.Parse(time.RFC3339Nano, c.StartAt)
	if err != nil || start.UnixMilli() < 0 || start.Nanosecond() != 0 || start.Unix()%c.IntervalSeconds != 0 {
		return fail()
	}
	c.StartAt = start.UTC().Format(time.RFC3339)
	d := &c.Destination
	if d.Prefix == "" {
		d.Prefix = "porcupine"
	}
	d.Prefix = strings.TrimSuffix(d.Prefix, "/")
	if !s3PipelineRegion.MatchString(d.Region) || !s3PipelineBucket.MatchString(d.Bucket) || strings.Contains(d.Bucket, "..") || net.ParseIP(d.Bucket) != nil || d.Prefix == "" || len(d.Prefix) > 600 {
		return fail()
	}
	for _, part := range strings.Split(d.Prefix, "/") {
		if !s3PipelineSegment.MatchString(part) || part == "." || part == ".." {
			return fail()
		}
	}
	if d.Endpoint != "" {
		origin, e := pipelineS3Origin(d.Endpoint)
		if e != nil {
			return fail()
		}
		d.Endpoint = origin
		d.UsePathStyle = true
	}
	return c, nil
}

func validateS3PipelineSpec(c S3PipelineSpec) (S3PipelineSpec, error) {
	c, err := normalizeS3PipelineSpec(c)
	if err != nil {
		return c, err
	}
	fail := func() (S3PipelineSpec, error) { return c, errors.New("invalid S3 pipeline specification") }
	sources := map[*chsql.TableIdentifier]bool{}
	declared := map[string]string{}
	canonical, err := validateSQLWithReferences(c.SQL, sqlReferences{eventSources: sources, parameters: declared})
	if err != nil || len(sources) == 0 {
		return fail()
	}
	c.SQL = canonical
	if validateQueryParameters(c.Parameters) != nil {
		return fail()
	}
	for _, name := range []string{"window_start", "window_end"} {
		if _, ok := c.Parameters[name]; ok {
			return fail()
		}
	}
	bindings := map[string]string{}
	for k, v := range c.Parameters {
		bindings[k] = v
	}
	for _, name := range []string{"window_start", "window_end"} {
		if typ, used := declared[name]; used {
			if typ != "Int64" {
				return fail()
			}
			bindings[name] = "0"
		}
	}
	if matchSQLParameters(declared, bindings) != nil {
		return fail()
	}
	return c, nil
}

// Every validated physical source is replaced, including sources nested in CTEs,
// joins and set operations. Qualified columns remain bound to the same alias.
func pipelineWindowSQL(raw, namespace string) (string, error) {
	canonical, err := validateSQL(raw)
	if err != nil || !clickHouseIdentifier.MatchString(namespace) {
		return "", errors.New("invalid pipeline query")
	}
	sources := map[*chsql.TableIdentifier]bool{}
	if _, err := validateSQLWithReferences(canonical, sqlReferences{eventSources: sources}); err != nil {
		return "", err
	}
	type replacement struct {
		start, end int
		text       string
	}
	var edits []replacement
	source := "(SELECT * FROM " + namespace + "." + identitySourceName(canonical) + " AS _porcupine_window WHERE _porcupine_window.received_at_ms >= {window_start:Int64} AND _porcupine_window.received_at_ms < {window_end:Int64} AND _porcupine_window.is_imported=0)"
	for table, aliased := range sources {
		text := source
		if !aliased {
			text += " AS events"
		}
		edits = append(edits, replacement{int(table.Pos()), int(table.End()), text})
	}
	sort.Slice(edits, func(i, j int) bool { return edits[i].start > edits[j].start })
	for _, e := range edits {
		if e.start < 0 || e.end > len(canonical) || e.start >= e.end {
			return "", errors.New("invalid source span")
		}
		canonical = canonical[:e.start] + e.text + canonical[e.end:]
	}
	if len(edits) == 0 {
		return "", errors.New("pipeline requires events source")
	}
	return canonical, nil
}

func (s *Service) initializeS3Pipelines(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS pipeline_s3_state(
 pipeline_id TEXT PRIMARY KEY REFERENCES pipelines(id) ON DELETE CASCADE,cursor_ms INTEGER NOT NULL,last_attempt_ms INTEGER NOT NULL DEFAULT 0);
 CREATE TABLE IF NOT EXISTS pipeline_s3_runs(
 id TEXT PRIMARY KEY,pipeline_id TEXT NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
 window_start_ms INTEGER NOT NULL,window_end_ms INTEGER NOT NULL,state TEXT NOT NULL,
 attempts INTEGER NOT NULL DEFAULT 0,created_at_ms INTEGER NOT NULL,completed_at_ms INTEGER NOT NULL DEFAULT 0,
 next_attempt_ms INTEGER NOT NULL DEFAULT 0,error_code TEXT NOT NULL DEFAULT '',attempt_id TEXT NOT NULL DEFAULT '',
 event_watermark INTEGER NOT NULL DEFAULT 0,mutation_watermark INTEGER NOT NULL DEFAULT 0,
 manifest TEXT NOT NULL DEFAULT '',row_count INTEGER NOT NULL DEFAULT 0,byte_count INTEGER NOT NULL DEFAULT 0,UNIQUE(pipeline_id,window_start_ms));
 CREATE TABLE IF NOT EXISTS pipeline_s3_files(
 run_id TEXT NOT NULL REFERENCES pipeline_s3_runs(id) ON DELETE CASCADE,object_key TEXT NOT NULL,upload_id TEXT NOT NULL DEFAULT '',
 PRIMARY KEY(run_id,object_key));
 UPDATE pipeline_s3_runs SET state='queued',error_code='interrupted' WHERE state='running';`)
	return err
}
func (s *Service) createS3PipelineState(ctx context.Context, tx *sql.Tx, id string, c S3PipelineSpec) error {
	start, err := time.Parse(time.RFC3339, c.StartAt)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO pipeline_s3_state(pipeline_id,cursor_ms) VALUES(?,?)`, id, start.UnixMilli())
	return err
}
func pipelineWindowParameters(spec S3PipelineSpec, start, end int64) map[string]string {
	p := make(map[string]string, len(spec.Parameters)+2)
	for k, v := range spec.Parameters {
		p[k] = v
	}
	p["window_start"] = strconv.FormatInt(start, 10)
	p["window_end"] = strconv.FormatInt(end, 10)
	return p
}

func (s *Service) validateS3PipelineSpec(ctx context.Context, project string, c S3PipelineSpec) (S3PipelineSpec, error) {
	if s.lake == nil {
		return validateS3PipelineSpec(c)
	}
	c, err := normalizeS3PipelineSpec(c)
	if err != nil {
		return c, err
	}
	c.SQL, err = s.lake.ValidateExport(ctx, project, c.SQL, c.Parameters, true)
	return c, err
}
