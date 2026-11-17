package main

import (
	"bytes"
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"
)

func TestPipelineKeyConfiguration(t *testing.T) {
	t.Setenv("PORCUPINE_PIPELINE_KEY", "")
	t.Setenv("PORCUPINE_PIPELINE_KEY_FILE", "")
	t.Setenv("PORCUPINE_PIPELINE_S3_ENDPOINTS", "")
	cfg, err := (settings{}).pipelineConfiguration()
	if cfg != nil || err != nil {
		t.Fatal("pipelines should be opt-in")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "pipeline-key")
	key := bytes.Repeat([]byte{1}, 32)
	encoded := base64.StdEncoding.EncodeToString(key)
	if err = os.WriteFile(path, []byte(encoded+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PORCUPINE_PIPELINE_KEY_FILE", path)
	t.Setenv("PORCUPINE_PIPELINE_S3_ENDPOINTS", "https://s3.example.org")
	t.Setenv("PORCUPINE_EXPORT_MAX_OUTPUT_BYTES", "1048576")
	cfg, err = (settings{}).pipelineConfiguration()
	if err != nil || !bytes.Equal(cfg.Key, key) || len(cfg.AllowedS3Endpoints) != 1 || cfg.ExportLimits.MaxOutputBytes != 1048576 {
		t.Fatal("file config not loaded", err)
	}
	t.Setenv("PORCUPINE_PIPELINE_KEY", encoded)
	if _, err = (settings{}).pipelineConfiguration(); err == nil {
		t.Fatal("ambiguous key configuration accepted")
	}
	t.Setenv("PORCUPINE_PIPELINE_KEY_FILE", "")
	t.Setenv("PORCUPINE_PIPELINE_KEY", "unstable")
	if _, err = (settings{}).pipelineConfiguration(); err == nil {
		t.Fatal("invalid key accepted")
	}
}
