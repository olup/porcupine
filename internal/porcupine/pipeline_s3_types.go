package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"net"
	"regexp"
	"strings"
	"time"
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
	_, err = tx.ExecContext(ctx, `INSERT INTO pipeline_s3_state(pipeline_id,cursor_ms) VALUES($1,$2)`, id, start.UnixMilli())
	return err
}
func (s *Service) validateS3PipelineSpec(ctx context.Context, project string, c S3PipelineSpec) (S3PipelineSpec, error) {
	if s.lake == nil {
		return c, apiError(503, "analytical storage is not configured")
	}
	c, err := normalizeS3PipelineSpec(c)
	if err != nil {
		return c, err
	}
	c.SQL, err = s.lake.ValidateExport(ctx, project, c.SQL, c.Parameters, true)
	return c, err
}
