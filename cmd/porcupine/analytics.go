package main

import (
	"errors"
	"os"
	"strconv"
	"time"

	"github.com/olup/porcupine/internal/porcupine"
)

func analyticsConfiguration() (*porcupine.ClickHouseConfig, *porcupine.DuckLakeConfig, error) {
	endpoint, directory := os.Getenv("PORCUPINE_CLICKHOUSE_URL"), os.Getenv("PORCUPINE_DUCKLAKE_DIR")
	if endpoint != "" && directory != "" {
		return nil, nil, errors.New("configure either PORCUPINE_DUCKLAKE_DIR or PORCUPINE_CLICKHOUSE_URL")
	}
	if endpoint != "" {
		hotDays, err := analyticsInteger("PORCUPINE_HOT_STORAGE_DAYS", 0, 1, 36500)
		if err != nil {
			return nil, nil, err
		}
		return &porcupine.ClickHouseConfig{URL: endpoint, Database: os.Getenv("PORCUPINE_CLICKHOUSE_DATABASE"),
			User: os.Getenv("PORCUPINE_CLICKHOUSE_USER"), Password: os.Getenv("PORCUPINE_CLICKHOUSE_PASSWORD"),
			StoragePolicy: os.Getenv("PORCUPINE_CLICKHOUSE_STORAGE_POLICY"), HotStorageDays: hotDays,
			ColdVolume: os.Getenv("PORCUPINE_CLICKHOUSE_COLD_VOLUME")}, nil, nil
	}
	if directory == "" {
		directory = defaultAnalyticsDirectory(os.Getenv("PORCUPINE_DB"))
	}
	if directory == "" {
		return nil, nil, nil
	}
	config := &porcupine.DuckLakeConfig{Directory: directory}
	for _, field := range []struct {
		name     string
		target   *int
		min, max int
	}{
		{"PORCUPINE_DUCKLAKE_MEMORY_MB", &config.MemoryLimitMB, 64, 32768},
		{"PORCUPINE_DUCKLAKE_THREADS", &config.Threads, 1, 4},
		{"PORCUPINE_DUCKLAKE_MAX_OPEN_PROJECTS", &config.MaxOpenProjects, 1, 64},
	} {
		value, err := analyticsInteger(field.name, 0, field.min, field.max)
		if err != nil {
			return nil, nil, err
		}
		*field.target = value
	}
	if raw := os.Getenv("PORCUPINE_DUCKLAKE_MAINTENANCE_INTERVAL"); raw != "" {
		interval, err := time.ParseDuration(raw)
		if err != nil || interval < time.Second || interval > 24*time.Hour {
			return nil, nil, errors.New("PORCUPINE_DUCKLAKE_MAINTENANCE_INTERVAL must be between 1s and 24h")
		}
		config.MaintenanceInterval = interval
	}
	return nil, config, nil
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
