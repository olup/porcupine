package main

import (
	"github.com/olup/porcupine/internal/porcupine"
	"testing"
	"time"
)

func TestDuckLakeS3Environment(t *testing.T) {
	for _, suffix := range analyticalS3Environment {
		t.Setenv("PORCUPINE_DUCKLAKE_S3_"+suffix, "")
	}
	c, err := duckLakeS3Configuration()
	if err != nil || c != nil {
		t.Fatal("S3 must be opt-in")
	}
	t.Setenv("PORCUPINE_DUCKLAKE_S3_PREFIX", "test")
	if _, err := duckLakeS3Configuration(); err == nil {
		t.Fatal("partial analytical S3 configuration ignored")
	}
	t.Setenv("PORCUPINE_DUCKLAKE_S3_BUCKET", "analytics-tests")
	t.Setenv("PORCUPINE_DUCKLAKE_S3_REGION", "eu-west-3")
	t.Setenv("PORCUPINE_DUCKLAKE_S3_ENDPOINT", "http://127.0.0.1:9000")
	t.Setenv("PORCUPINE_DUCKLAKE_S3_PATH_STYLE", "true")
	t.Setenv("PORCUPINE_DUCKLAKE_S3_ACCESS_KEY_ID", "fixture-access")
	t.Setenv("PORCUPINE_DUCKLAKE_S3_SECRET_ACCESS_KEY", "fixture-secret")
	c, err = duckLakeS3Configuration()
	if err != nil || c == nil || !c.UsePathStyle || c.Bucket != "analytics-tests" || c.SecretAccessKey != "fixture-secret" {
		t.Fatal("S3 environment not parsed", err)
	}
	t.Setenv("PORCUPINE_CLICKHOUSE_URL", "http://localhost:8123")
	t.Setenv("PORCUPINE_DUCKLAKE_DIR", "")
	if _, _, err := analyticsConfiguration(); err == nil {
		t.Fatal("ClickHouse silently ignored native S3 configuration")
	}
	t.Setenv("PORCUPINE_DUCKLAKE_S3_PATH_STYLE", "definitely")
	if _, err := duckLakeS3Configuration(); err == nil {
		t.Fatal("invalid path style flag accepted")
	}
}

func TestAnalyticalObjectStorageHTTPResponseBudget(t *testing.T) {
	if analyticsHTTPWriteTimeout(nil) != 30*time.Second {
		t.Fatal("local response budget changed")
	}
	for _, pair := range []struct{ io, wanted int }{{0, 30}, {1, 30}, {5, 30}, {30, 45}} {
		lake := &porcupine.DuckLakeConfig{S3: &porcupine.DuckLakeS3Config{RequestTimeoutSeconds: pair.io}}
		if got := analyticsHTTPWriteTimeout(lake); got != time.Duration(pair.wanted)*time.Second {
			t.Fatalf("I/O timeout %d produced response budget %s", pair.io, got)
		}
	}
}
