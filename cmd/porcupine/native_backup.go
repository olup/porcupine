package main

import (
	"errors"
	"os"
	"strconv"
	"time"

	"github.com/olup/porcupine/internal/porcupine"
)

func nativeBackupConfiguration() (*porcupine.NativeBackupConfig, error) {
	directory := os.Getenv("PORCUPINE_BACKUP_DIR")
	if directory == "" {
		for _, name := range []string{"PORCUPINE_BACKUP_INTERVAL", "PORCUPINE_BACKUP_TIMEOUT", "PORCUPINE_BACKUP_RETAIN"} {
			if os.Getenv(name) != "" {
				return nil, errors.New("configure PORCUPINE_BACKUP_DIR to enable native backups")
			}
		}
		return nil, nil
	}
	config := &porcupine.NativeBackupConfig{Directory: directory}
	for _, field := range []struct {
		name     string
		target   *time.Duration
		min, max time.Duration
	}{
		{"PORCUPINE_BACKUP_INTERVAL", &config.Interval, time.Minute, 365 * 24 * time.Hour},
		{"PORCUPINE_BACKUP_TIMEOUT", &config.Timeout, time.Second, time.Hour},
	} {
		if raw := os.Getenv(field.name); raw != "" {
			duration, err := time.ParseDuration(raw)
			if err != nil || duration < field.min || duration > field.max {
				return nil, errors.New("invalid " + field.name)
			}
			*field.target = duration
		}
	}
	if raw := os.Getenv("PORCUPINE_BACKUP_RETAIN"); raw != "" {
		retain, err := strconv.Atoi(raw)
		if err != nil || retain < 1 || retain > 1000 {
			return nil, errors.New("PORCUPINE_BACKUP_RETAIN must be between 1 and 1000")
		}
		config.Retain = retain
	}
	return config, nil
}
