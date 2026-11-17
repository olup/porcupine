package main

import (
	"errors"
	"strconv"
	"time"

	lakeapi "github.com/olup/porcupine/internal/lake"
)

func (s settings) nativeBackupConfiguration() (*lakeapi.NativeBackupConfig, error) {
	directory := s.get("PORCUPINE_BACKUP_DIR")
	remote, err := s.nativeBackupS3Configuration()
	if err != nil {
		return nil, err
	}
	if directory != "" && remote != nil {
		return nil, errors.New("configure PORCUPINE_BACKUP_DIR or PORCUPINE_BACKUP_S3_BUCKET, not both")
	}
	if directory == "" && remote == nil {
		for _, name := range []string{"PORCUPINE_BACKUP_INTERVAL", "PORCUPINE_BACKUP_TIMEOUT", "PORCUPINE_BACKUP_RETAIN"} {
			if s.get(name) != "" {
				return nil, errors.New("configure PORCUPINE_BACKUP_DIR or PORCUPINE_BACKUP_S3_BUCKET to enable native backups")
			}
		}
		return nil, nil
	}
	config := &lakeapi.NativeBackupConfig{Directory: directory, S3: remote}
	for _, field := range []struct {
		name     string
		target   *time.Duration
		min, max time.Duration
	}{
		{"PORCUPINE_BACKUP_INTERVAL", &config.Interval, time.Minute, 365 * 24 * time.Hour},
		{"PORCUPINE_BACKUP_TIMEOUT", &config.Timeout, time.Second, time.Hour},
	} {
		if raw := s.get(field.name); raw != "" {
			duration, err := time.ParseDuration(raw)
			if err != nil || duration < field.min || duration > field.max {
				return nil, errors.New("invalid " + field.name)
			}
			*field.target = duration
		}
	}
	if raw := s.get("PORCUPINE_BACKUP_RETAIN"); raw != "" {
		retain, err := strconv.Atoi(raw)
		if err != nil || retain < 1 || retain > 1000 {
			return nil, errors.New("PORCUPINE_BACKUP_RETAIN must be between 1 and 1000")
		}
		config.Retain = retain
	}
	return config, nil
}

func (s settings) nativeBackupS3Configuration() (*lakeapi.DuckLakeS3Config, error) {
	const prefix = "PORCUPINE_BACKUP_S3_"
	configured := false
	for _, suffix := range analyticalS3Environment {
		configured = configured || s.get(prefix+suffix) != ""
	}
	if !configured {
		return nil, nil
	}
	c := &lakeapi.DuckLakeS3Config{Bucket: s.get(prefix + "BUCKET"), Prefix: s.get(prefix + "PREFIX"), Region: s.get(prefix + "REGION"), Endpoint: s.get(prefix + "ENDPOINT"), AccessKeyID: s.get(prefix + "ACCESS_KEY_ID"), SecretAccessKey: s.get(prefix + "SECRET_ACCESS_KEY"), SessionToken: s.get(prefix + "SESSION_TOKEN"), CACertFile: s.get(prefix + "CA_CERT_FILE")}
	if c.Bucket == "" || c.Prefix == "" || c.Region == "" {
		return nil, errors.New("backup S3 requires PORCUPINE_BACKUP_S3_BUCKET, PREFIX and REGION")
	}
	if raw := s.get(prefix + "PATH_STYLE"); raw != "" {
		v, err := strconv.ParseBool(raw)
		if err != nil {
			return nil, errors.New("invalid PORCUPINE_BACKUP_S3_PATH_STYLE")
		}
		c.UsePathStyle = v
	}
	timeout, err := s.analyticsInteger(prefix+"REQUEST_TIMEOUT_SECONDS", 0, 1, 30)
	if err != nil {
		return nil, err
	}
	c.RequestTimeoutSeconds = timeout
	return c, nil
}
