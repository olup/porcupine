package main

import (
	"errors"
	"strconv"

	"github.com/olup/porcupine/internal/exportstore"
	"github.com/olup/porcupine/internal/porcupine"
)

func (s settings) exportConfiguration() (*porcupine.ExportConfig, error) {
	for _, name := range []string{"PORCUPINE_EXPORT_MAX_READ_BYTES", "PORCUPINE_EXPORT_MEMORY_BYTES", "PORCUPINE_EXPORT_TEMPORARY_BYTES"} {
		if s.get(name) != "" {
			return nil, errors.New(name + " is not enforced by DuckDB and is no longer supported")
		}
	}

	bucket := s.get("PORCUPINE_EXPORT_S3_BUCKET")
	if bucket == "" {
		for _, name := range []string{"PORCUPINE_EXPORT_S3_PREFIX", "PORCUPINE_EXPORT_S3_ENDPOINT", "PORCUPINE_EXPORT_S3_ACCESS_KEY_ID", "PORCUPINE_EXPORT_S3_SECRET_ACCESS_KEY"} {
			if s.get(name) != "" {
				return nil, errors.New("PORCUPINE_EXPORT_S3_BUCKET is required when exports are configured")
			}
		}
		return nil, nil
	}
	c := &porcupine.ExportConfig{S3: exportstore.Config{
		Bucket: bucket, Prefix: s.get("PORCUPINE_EXPORT_S3_PREFIX"), Region: s.get("PORCUPINE_EXPORT_S3_REGION"),
		Endpoint: s.get("PORCUPINE_EXPORT_S3_ENDPOINT"), AccessKeyID: s.get("PORCUPINE_EXPORT_S3_ACCESS_KEY_ID"),
		SecretAccessKey: s.get("PORCUPINE_EXPORT_S3_SECRET_ACCESS_KEY"), SessionToken: s.get("PORCUPINE_EXPORT_S3_SESSION_TOKEN"),
	}}
	if c.S3.Region == "" {
		c.S3.Region = s.get("AWS_REGION")
	}
	if c.S3.Region == "" {
		c.S3.Region = "us-east-1"
	}
	if raw := s.get("PORCUPINE_EXPORT_S3_PATH_STYLE"); raw != "" {
		value, err := strconv.ParseBool(raw)
		if err != nil {
			return nil, errors.New("invalid PORCUPINE_EXPORT_S3_PATH_STYLE")
		}
		c.S3.UsePathStyle = value
	}
	if err := s.readExportLimits(c); err != nil {
		return nil, err
	}
	return c, nil
}

func (s settings) readExportLimits(c *porcupine.ExportConfig) error {

	var seconds, queued, hours int64
	fields := []struct {
		name  string
		value *int64
	}{
		{"PORCUPINE_EXPORT_MAX_EXECUTION_SECONDS", &seconds},
		{"PORCUPINE_EXPORT_MAX_OUTPUT_BYTES", &c.MaxOutputBytes},
		{"PORCUPINE_EXPORT_FILE_BYTES", &c.FileBytes},
		{"PORCUPINE_EXPORT_MAX_ROW_BYTES", &c.MaxRowBytes},
		{"PORCUPINE_EXPORT_BANDWIDTH_BYTES_PER_SECOND", &c.BandwidthBytesPerSecond},
		{"PORCUPINE_EXPORT_QUEUE_THRESHOLD", &c.QueueThreshold},
		{"PORCUPINE_EXPORT_MAX_QUEUED_JOBS", &queued},
		{"PORCUPINE_EXPORT_RETENTION_HOURS", &hours},
	}
	for _, field := range fields {
		if raw := s.get(field.name); raw != "" {
			value, err := strconv.ParseInt(raw, 10, 64)
			if err != nil || value < 1 {
				return errors.New("invalid " + field.name)
			}
			*field.value = value
		}
	}
	c.MaxExecutionSeconds = int(seconds)
	c.MaxQueuedJobs = int(queued)
	c.RetentionHours = int(hours)
	return nil
}
