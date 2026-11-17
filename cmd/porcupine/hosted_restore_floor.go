package main

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/olup/porcupine/internal/porcupine"
)

func (s settings) hostedRecoveryFloor() (porcupine.HostedRecoveryFloor, error) {
	path := s.get("PORCUPINE_DUCKLAKE_RESTORE_FLOOR_FILE")
	if path == "" {
		return nil, nil
	}
	if s.get("PORCUPINE_PROFILE") != string(porcupine.ProfileHosted) || !filepath.IsAbs(path) {
		return nil, errors.New("PORCUPINE_DUCKLAKE_RESTORE_FLOOR_FILE requires the hosted profile and an absolute path")
	}
	return func(ctx context.Context) (time.Time, error) {
		if err := ctx.Err(); err != nil {
			return time.Time{}, err
		}
		return readHostedRecoveryFloor(path)
	}, nil
}

func readHostedRecoveryFloor(path string) (time.Time, error) {
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() {
		return time.Time{}, errors.New("hosted restore floor file unavailable")
	}
	file, err := os.Open(path)
	if err != nil {
		return time.Time{}, errors.New("hosted restore floor file unavailable")
	}
	defer file.Close()
	const maxBytes = 128
	data, err := io.ReadAll(io.LimitReader(file, maxBytes+1))
	if err != nil || len(data) > maxBytes {
		return time.Time{}, errors.New("hosted restore floor file unreadable")
	}
	floor, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(string(data)))
	if err != nil || floor.IsZero() {
		return time.Time{}, errors.New("hosted restore floor file requires one RFC3339 timestamp")
	}
	return floor, nil
}
