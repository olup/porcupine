package main

import (
	"os"
	"strings"
	"testing"
)

func cleanExportEnvironment(t *testing.T) {
	t.Helper()
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(key, "PORCUPINE_EXPORT_") {
			t.Setenv(key, "")
		}
	}
}

func TestExportConfiguration(t *testing.T) {
	cleanExportEnvironment(t)
	c, err := exportConfiguration()
	if err != nil || c != nil {
		t.Fatalf("exports should be opt-in: %+v %v", c, err)
	}
	t.Setenv("PORCUPINE_EXPORT_S3_PREFIX", "exports/test")
	if _, err := exportConfiguration(); err == nil {
		t.Fatal("partial export config silently ignored")
	}
	t.Setenv("PORCUPINE_EXPORT_S3_BUCKET", "private-test-bucket")
	t.Setenv("PORCUPINE_EXPORT_MAX_EXECUTION_SECONDS", "3600")
	t.Setenv("PORCUPINE_EXPORT_BANDWIDTH_BYTES_PER_SECOND", "1048576")
	c, err = exportConfiguration()
	if err != nil || c.MaxExecutionSeconds != 3600 || c.BandwidthBytesPerSecond != 1048576 || c.S3.Prefix != "exports/test" {
		t.Fatalf("config: %+v %v", c, err)
	}
	t.Setenv("PORCUPINE_EXPORT_MEMORY_BYTES", "not-a-number")
	if _, err := exportConfiguration(); err == nil || !strings.Contains(err.Error(), "PORCUPINE_EXPORT_MEMORY_BYTES") {
		t.Fatalf("invalid budget: %v", err)
	}
}
