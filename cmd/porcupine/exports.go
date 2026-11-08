package main

import (
	"errors"
	"os"
	"strconv"

	"github.com/olup/porcupine/internal/exportstore"
	"github.com/olup/porcupine/internal/porcupine"
)

func exportConfiguration() (*porcupine.ExportConfig, error) {
	bucket := os.Getenv("PORCUPINE_EXPORT_S3_BUCKET")
	if bucket == "" {
		for _, name := range []string{"PORCUPINE_EXPORT_S3_PREFIX", "PORCUPINE_EXPORT_S3_ENDPOINT", "PORCUPINE_EXPORT_S3_ACCESS_KEY_ID", "PORCUPINE_EXPORT_S3_SECRET_ACCESS_KEY"} {
			if os.Getenv(name) != "" {
				return nil, errors.New("PORCUPINE_EXPORT_S3_BUCKET is required when exports are configured")
			}
		}
		return nil, nil
	}
	c := &porcupine.ExportConfig{S3: exportstore.Config{
		Bucket: bucket, Prefix: os.Getenv("PORCUPINE_EXPORT_S3_PREFIX"), Region: os.Getenv("PORCUPINE_EXPORT_S3_REGION"),
		Endpoint: os.Getenv("PORCUPINE_EXPORT_S3_ENDPOINT"), AccessKeyID: os.Getenv("PORCUPINE_EXPORT_S3_ACCESS_KEY_ID"),
		SecretAccessKey: os.Getenv("PORCUPINE_EXPORT_S3_SECRET_ACCESS_KEY"), SessionToken: os.Getenv("PORCUPINE_EXPORT_S3_SESSION_TOKEN"),
	}}
	if c.S3.Region == "" {
		c.S3.Region = os.Getenv("AWS_REGION")
	}
	if c.S3.Region == "" {
		c.S3.Region = "us-east-1"
	}
	if raw := os.Getenv("PORCUPINE_EXPORT_S3_PATH_STYLE"); raw != "" {
		value, err := strconv.ParseBool(raw)
		if err != nil {
			return nil, errors.New("invalid PORCUPINE_EXPORT_S3_PATH_STYLE")
		}
		c.S3.UsePathStyle = value
	}
	if err := readExportLimits(c); err != nil {
		return nil, err
	}
	return c, nil
}

func readExportLimits(c *porcupine.ExportConfig) error {
	var seconds, queued, hours int64
	fields := []struct {
		name  string
		value *int64
	}{
		{"PORCUPINE_EXPORT_MAX_EXECUTION_SECONDS", &seconds},
		{"PORCUPINE_EXPORT_MAX_READ_BYTES", &c.MaxReadBytes},
		{"PORCUPINE_EXPORT_MAX_OUTPUT_BYTES", &c.MaxOutputBytes},
		{"PORCUPINE_EXPORT_FILE_BYTES", &c.FileBytes},
		{"PORCUPINE_EXPORT_MAX_ROW_BYTES", &c.MaxRowBytes},
		{"PORCUPINE_EXPORT_MEMORY_BYTES", &c.MemoryBytes},
		{"PORCUPINE_EXPORT_BANDWIDTH_BYTES_PER_SECOND", &c.BandwidthBytesPerSecond},
		{"PORCUPINE_EXPORT_TEMPORARY_BYTES", &c.TemporaryBytes},
		{"PORCUPINE_EXPORT_QUEUE_THRESHOLD", &c.QueueThreshold},
		{"PORCUPINE_EXPORT_MAX_QUEUED_JOBS", &queued},
		{"PORCUPINE_EXPORT_RETENTION_HOURS", &hours},
	}
	for _, field := range fields {
		if raw := os.Getenv(field.name); raw != "" {
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
