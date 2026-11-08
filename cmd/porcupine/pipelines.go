package main

import (
	"encoding/base64"
	"errors"
	"io"
	"os"
	"strings"

	"github.com/olup/porcupine/internal/porcupine"
)

func pipelineConfiguration() (*porcupine.PipelineConfig, error) {
	key := os.Getenv("PORCUPINE_PIPELINE_KEY")
	path := os.Getenv("PORCUPINE_PIPELINE_KEY_FILE")
	endpoints := os.Getenv("PORCUPINE_PIPELINE_S3_ENDPOINTS")
	if path != "" {
		if key != "" {
			return nil, errors.New("configure just one pipeline encryption key source")
		}
		f, err := os.Open(path)
		if err != nil {
			return nil, errors.New("cannot open pipeline encryption key file")
		}
		defer f.Close()
		b, err := io.ReadAll(io.LimitReader(f, 257))
		if err != nil || len(b) > 256 || len(strings.TrimSpace(string(b))) == 0 {
			return nil, errors.New("invalid pipeline encryption key file")
		}
		key = string(b)
	}
	if key == "" {
		if endpoints != "" {
			return nil, errors.New("S3 pipeline endpoints require an encryption key")
		}
		return nil, nil
	}
	decoded, err := base64.StdEncoding.Strict().DecodeString(strings.TrimSpace(key))
	if err != nil || len(decoded) != 32 {
		return nil, errors.New("pipeline encryption key must be 32 base64-encoded bytes")
	}
	cfg := &porcupine.PipelineConfig{Key: decoded}
	if endpoints != "" {
		cfg.AllowedS3Endpoints = strings.Split(endpoints, ",")
	}
	if err = readExportLimits(&cfg.ExportLimits); err != nil {
		return nil, err
	}
	return cfg, nil
}
