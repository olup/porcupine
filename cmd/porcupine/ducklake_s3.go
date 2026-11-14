package main

import (
	"errors"
	"os"
	"strconv"
	"time"

	"github.com/olup/porcupine/internal/porcupine"
)

var analyticalS3Environment = []string{"BUCKET", "PREFIX", "REGION", "ENDPOINT", "ACCESS_KEY_ID", "SECRET_ACCESS_KEY", "SESSION_TOKEN", "PATH_STYLE", "CA_CERT_FILE", "REQUEST_TIMEOUT_SECONDS"}

func duckLakeS3Configuration() (*porcupine.DuckLakeS3Config, error) {
	configured := false
	for _, suffix := range analyticalS3Environment {
		if os.Getenv("PORCUPINE_DUCKLAKE_S3_"+suffix) != "" {
			configured = true
		}
	}
	if !configured {
		return nil, nil
	}
	c := &porcupine.DuckLakeS3Config{Bucket: os.Getenv("PORCUPINE_DUCKLAKE_S3_BUCKET"), Prefix: os.Getenv("PORCUPINE_DUCKLAKE_S3_PREFIX"), Region: os.Getenv("PORCUPINE_DUCKLAKE_S3_REGION"), Endpoint: os.Getenv("PORCUPINE_DUCKLAKE_S3_ENDPOINT"), AccessKeyID: os.Getenv("PORCUPINE_DUCKLAKE_S3_ACCESS_KEY_ID"), SecretAccessKey: os.Getenv("PORCUPINE_DUCKLAKE_S3_SECRET_ACCESS_KEY"), SessionToken: os.Getenv("PORCUPINE_DUCKLAKE_S3_SESSION_TOKEN"), CACertFile: os.Getenv("PORCUPINE_DUCKLAKE_S3_CA_CERT_FILE")}
	if c.Bucket == "" || c.Prefix == "" || c.Region == "" {
		return nil, errors.New("analytical S3 requires PORCUPINE_DUCKLAKE_S3_BUCKET, PREFIX and REGION")
	}
	if raw := os.Getenv("PORCUPINE_DUCKLAKE_S3_PATH_STYLE"); raw != "" {
		value, err := strconv.ParseBool(raw)
		if err != nil {
			return nil, errors.New("invalid PORCUPINE_DUCKLAKE_S3_PATH_STYLE")
		}
		c.UsePathStyle = value
	}
	requestTimeout, err := analyticsInteger("PORCUPINE_DUCKLAKE_S3_REQUEST_TIMEOUT_SECONDS", 0, 1, 30)
	if err != nil {
		return nil, err
	}
	c.RequestTimeoutSeconds = requestTimeout
	return c, nil
}

func analyticsHTTPWriteTimeout(lake *porcupine.DuckLakeConfig) time.Duration {
	timeout := 30 * time.Second
	if lake != nil && lake.S3 != nil {
		requestTimeout := lake.S3.RequestTimeoutSeconds
		if requestTimeout == 0 {
			requestTimeout = 5
		}
		timeout = max(timeout, time.Duration(15+requestTimeout)*time.Second)
	}
	return timeout
}
