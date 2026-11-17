package main

import (
	"errors"
	"os"
	"strconv"
	"time"

	"github.com/olup/porcupine/internal/porcupine"
)

func analyticsConfiguration() (*porcupine.DuckLakeConfig, error) {
	directory := os.Getenv("PORCUPINE_DUCKLAKE_DIR")
	remote, err := duckLakeS3Configuration()
	if err != nil {
		return nil, err
	}
	if directory == "" {
		directory = defaultAnalyticsDirectory(os.Getenv("PORCUPINE_DB"))
	}
	config := &porcupine.DuckLakeConfig{Directory: directory, S3: remote}
	config.PostgresURL, err = sharedSecretSetting("PORCUPINE_DUCKLAKE_POSTGRES_URL")
	if err != nil {
		return nil, err
	}
	for _, field := range []struct {
		name     string
		target   *int
		min, max int
	}{
		{"PORCUPINE_DUCKLAKE_MEMORY_MB", &config.MemoryLimitMB, 64, 32768},
		{"PORCUPINE_DUCKLAKE_THREADS", &config.Threads, 1, 4},
		{"PORCUPINE_DUCKLAKE_MAX_OPEN_PROJECTS", &config.MaxOpenProjects, 1, 64},
		{"PORCUPINE_DUCKLAKE_METADATA_TIMEOUT_SECONDS", &config.MetadataTimeoutSeconds, 2, 60},
	} {
		value, err := analyticsInteger(field.name, 0, field.min, field.max)
		if err != nil {
			return nil, err
		}
		*field.target = value
	}
	if raw := os.Getenv("PORCUPINE_DUCKLAKE_MAINTENANCE_INTERVAL"); raw != "" {
		interval, err := time.ParseDuration(raw)
		if err != nil || interval < time.Second || interval > 24*time.Hour {
			return nil, errors.New("PORCUPINE_DUCKLAKE_MAINTENANCE_INTERVAL must be between 1s and 24h")
		}
		config.MaintenanceInterval = interval
	}
	for _, field := range []struct {
		name   string
		target *time.Duration
	}{
		{"PORCUPINE_DUCKLAKE_SNAPSHOT_RETENTION", &config.SnapshotRetention},
		{"PORCUPINE_DUCKLAKE_CLEANUP_GRACE", &config.CleanupGrace},
		{"PORCUPINE_DUCKLAKE_CLEANUP_INTERVAL", &config.CleanupInterval},
	} {
		if raw := os.Getenv(field.name); raw != "" {
			duration, err := time.ParseDuration(raw)
			if err != nil || duration < time.Second {
				return nil, errors.New(field.name + " must be a duration of at least 1s")
			}
			*field.target = duration
		}
	}
	if raw := os.Getenv("PORCUPINE_DUCKLAKE_CLEANUP_ENABLED"); raw != "" {
		enabled, err := strconv.ParseBool(raw)
		if err != nil {
			return nil, errors.New("invalid PORCUPINE_DUCKLAKE_CLEANUP_ENABLED")
		}
		config.DisableCleanup = !enabled
	}
	return config, nil
}

func analyticsInteger(name string, fallback, min, max int) (int, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < min || value > max {
		return 0, errors.New("invalid " + name)
	}
	return value, nil
}
