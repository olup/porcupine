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
	remote, err := nativeBackupS3Configuration()
	if err != nil {
		return nil, err
	}
	if directory != "" && remote != nil {
		return nil, errors.New("configure PORCUPINE_BACKUP_DIR or PORCUPINE_BACKUP_S3_BUCKET, not both")
	}
	if directory == "" && remote == nil {
		for _, name := range []string{"PORCUPINE_BACKUP_INTERVAL", "PORCUPINE_BACKUP_TIMEOUT", "PORCUPINE_BACKUP_RETAIN"} {
			if os.Getenv(name) != "" {
				return nil, errors.New("configure PORCUPINE_BACKUP_DIR or PORCUPINE_BACKUP_S3_BUCKET to enable native backups")
			}
		}
		return nil, nil
	}
	config := &porcupine.NativeBackupConfig{Directory: directory, S3: remote}
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

func nativeBackupS3Configuration() (*porcupine.DuckLakeS3Config, error) {
	const prefix = "PORCUPINE_BACKUP_S3_"
	configured := false
	for _, suffix := range analyticalS3Environment {
		configured = configured || os.Getenv(prefix+suffix) != ""
	}
	if !configured {
		return nil, nil
	}
	c := &porcupine.DuckLakeS3Config{Bucket: os.Getenv(prefix + "BUCKET"), Prefix: os.Getenv(prefix + "PREFIX"), Region: os.Getenv(prefix + "REGION"), Endpoint: os.Getenv(prefix + "ENDPOINT"), AccessKeyID: os.Getenv(prefix + "ACCESS_KEY_ID"), SecretAccessKey: os.Getenv(prefix + "SECRET_ACCESS_KEY"), SessionToken: os.Getenv(prefix + "SESSION_TOKEN"), CACertFile: os.Getenv(prefix + "CA_CERT_FILE")}
	if c.Bucket == "" || c.Prefix == "" || c.Region == "" {
		return nil, errors.New("backup S3 requires PORCUPINE_BACKUP_S3_BUCKET, PREFIX and REGION")
	}
	if raw := os.Getenv(prefix + "PATH_STYLE"); raw != "" {
		v, err := strconv.ParseBool(raw)
		if err != nil {
			return nil, errors.New("invalid PORCUPINE_BACKUP_S3_PATH_STYLE")
		}
		c.UsePathStyle = v
	}
	timeout, err := analyticsInteger(prefix+"REQUEST_TIMEOUT_SECONDS", 0, 1, 30)
	if err != nil {
		return nil, err
	}
	c.RequestTimeoutSeconds = timeout
	return c, nil
}
