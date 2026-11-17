package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const sharedParserSecret = "fixture-routing-secret-private-32-bytes"
const sharedParserControlURL = "postgres://fixture:private-control-password@control.internal/porcupine?sslmode=require"
const sharedParserAnalyticsURL = "postgresql://fixture:private-lake-password@lake.internal/metadata?sslmode=require"

func clearSharedParserEnvironment(t *testing.T) {
	t.Helper()
	// In particular, a developer's *_FILE settings must not affect default tests.
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(name, "PORCUPINE_") {
			t.Setenv(name, "")
		}
	}
}

func validSharedParserEnvironment(t *testing.T) {
	t.Helper()
	t.Setenv("PORCUPINE_CONTROL_DATABASE_URL", sharedParserControlURL)
	t.Setenv("PORCUPINE_WORKER_ID", "worker-one")
	t.Setenv("PORCUPINE_WORKER_URL", "https://worker-one.internal")
	t.Setenv("PORCUPINE_SHARED_ROUTING_SECRET", sharedParserSecret)
}

func sharedParserFile(t *testing.T, name, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func assertSharedParserError(t *testing.T, err error, private ...string) {
	t.Helper()
	if err == nil {
		t.Fatal("invalid shared configuration accepted")
	}
	for _, value := range append([]string{sharedParserSecret, sharedParserControlURL, sharedParserAnalyticsURL, "private-control-password", "private-lake-password"}, private...) {
		if value != "" && strings.Contains(err.Error(), value) {
			t.Fatal("configuration error exposed a secret or private filename")
		}
	}
}

func TestSharedConfigurationDefaultAndComplete(t *testing.T) {
	clearSharedParserEnvironment(t)
	shared, err := (settings{}).sharedConfiguration()
	if err != nil || shared != nil {
		t.Fatal("shared mode enabled without configuration", err)
	}
	lake, err := (settings{}).analyticsConfiguration()
	if err != nil || lake == nil || lake.PostgresURL != "" || lake.S3 != nil {
		t.Fatal("default native storage changed", err)
	}
	validSharedParserEnvironment(t)
	t.Setenv("PORCUPINE_DUCKLAKE_POSTGRES_URL", sharedParserAnalyticsURL)
	t.Setenv("PORCUPINE_DUCKLAKE_DIR", "/srv/worker-one/lake")
	t.Setenv("PORCUPINE_DUCKLAKE_S3_BUCKET", "fixture-data")
	t.Setenv("PORCUPINE_DUCKLAKE_S3_PREFIX", "porcupine")
	t.Setenv("PORCUPINE_DUCKLAKE_S3_REGION", "eu-west-3")
	shared, err = (settings{}).sharedConfiguration()
	if err != nil || shared == nil || shared.DatabaseURL != sharedParserControlURL || shared.WorkerID != "worker-one" || shared.RouteURL != "https://worker-one.internal" || string(shared.RoutingSecret) != sharedParserSecret {
		t.Fatal("complete shared settings were not parsed", err)
	}
	lake, err = (settings{}).analyticsConfiguration()
	if err != nil || lake.PostgresURL != sharedParserAnalyticsURL || lake.Directory != "/srv/worker-one/lake" || lake.S3 == nil || lake.S3.Bucket != "fixture-data" || lake.S3.Prefix != "porcupine" || lake.S3.Region != "eu-west-3" {
		t.Fatal("shared analytical settings were not parsed", err)
	}
}

func TestSharedConfigurationRequiresEveryField(t *testing.T) {
	fields := []string{"PORCUPINE_CONTROL_DATABASE_URL", "PORCUPINE_WORKER_ID", "PORCUPINE_WORKER_URL", "PORCUPINE_SHARED_ROUTING_SECRET"}
	for mask := 1; mask < (1<<len(fields))-1; mask++ {
		t.Run(fmt.Sprintf("fields-%04b", mask), func(t *testing.T) {
			clearSharedParserEnvironment(t)
			values := []string{sharedParserControlURL, "worker-one", "https://worker-one.internal", sharedParserSecret}
			for i, name := range fields {
				if mask&(1<<i) != 0 {
					t.Setenv(name, values[i])
				}
			}
			config, err := (settings{}).sharedConfiguration()
			assertSharedParserError(t, err)
			if config != nil {
				t.Fatal("partial shared configuration returned a usable config")
			}
		})
	}
	t.Run("short routing secret", func(t *testing.T) {
		clearSharedParserEnvironment(t)
		validSharedParserEnvironment(t)
		t.Setenv("PORCUPINE_SHARED_ROUTING_SECRET", strings.Repeat("x", 31))
		_, err := (settings{}).sharedConfiguration()
		assertSharedParserError(t, err)
	})
}

func TestSharedSecretFileSourcesAndExclusivity(t *testing.T) {
	names := []string{"PORCUPINE_CONTROL_DATABASE_URL", "PORCUPINE_SHARED_ROUTING_SECRET", "PORCUPINE_DUCKLAKE_POSTGRES_URL"}
	values := []string{sharedParserControlURL, sharedParserSecret, sharedParserAnalyticsURL}
	for i, name := range names {
		t.Run(name, func(t *testing.T) {
			clearSharedParserEnvironment(t)
			path := sharedParserFile(t, "private-secret-file", values[i]+"\n")
			t.Setenv(name+"_FILE", path)
			value, err := (settings{}).sharedSecretSetting(name)
			if err != nil || value != values[i] {
				t.Fatal("mounted secret file not read", err)
			}
			t.Setenv(name, values[i])
			_, err = (settings{}).sharedSecretSetting(name)
			assertSharedParserError(t, err, path)
		})
	}
	t.Run("complete file-backed configuration", func(t *testing.T) {
		clearSharedParserEnvironment(t)
		validSharedParserEnvironment(t)
		for i, name := range names {
			t.Setenv(name, "")
			t.Setenv(name+"_FILE", sharedParserFile(t, "mounted-secret", values[i]+"\n"))
		}
		shared, err := (settings{}).sharedConfiguration()
		if err != nil || shared.DatabaseURL != sharedParserControlURL || string(shared.RoutingSecret) != sharedParserSecret {
			t.Fatal("file-backed shared mode failed", err)
		}
		lake, err := (settings{}).analyticsConfiguration()
		if err != nil || lake.PostgresURL != sharedParserAnalyticsURL {
			t.Fatal("file-backed analytical DSN failed", err)
		}
	})
}

func TestSharedSecretRejectsInvalidFilesAndValues(t *testing.T) {
	for _, name := range []string{"PORCUPINE_CONTROL_DATABASE_URL", "PORCUPINE_SHARED_ROUTING_SECRET", "PORCUPINE_DUCKLAKE_POSTGRES_URL"} {
		for _, tc := range []struct{ name, content string }{
			{"empty", ""}, {"whitespace", " \r\n\t "}, {"oversize", strings.Repeat("x", (16<<10)+1)},
			{"NUL", sharedParserSecret + "\x00hidden"}, {"embedded LF", sharedParserSecret + "\nhidden"}, {"embedded CR", sharedParserSecret + "\rhidden"},
		} {
			t.Run(name+"/"+tc.name, func(t *testing.T) {
				clearSharedParserEnvironment(t)
				path := sharedParserFile(t, "private-secret-path", tc.content)
				t.Setenv(name+"_FILE", path)
				_, err := (settings{}).sharedSecretSetting(name)
				assertSharedParserError(t, err, path, tc.content)
			})
		}
		for _, kind := range []string{"missing", "directory"} {
			t.Run(name+"/"+kind, func(t *testing.T) {
				clearSharedParserEnvironment(t)
				path := t.TempDir()
				if kind == "missing" {
					path = filepath.Join(path, "private-missing-secret")
				}
				t.Setenv(name+"_FILE", path)
				_, err := (settings{}).sharedSecretSetting(name)
				assertSharedParserError(t, err, path)
			})
		}
		for _, value := range []string{sharedParserSecret + "\nhidden", sharedParserSecret + "\rhidden"} {
			t.Run(name+"/environment control characters", func(t *testing.T) {
				clearSharedParserEnvironment(t)
				t.Setenv(name, value)
				_, err := (settings{}).sharedSecretSetting(name)
				assertSharedParserError(t, err, value)
			})
		}
	}
	// Explicitly selecting an empty file must not silently select local mode.
	t.Run("empty file cannot disable shared mode", func(t *testing.T) {
		clearSharedParserEnvironment(t)
		t.Setenv("PORCUPINE_CONTROL_DATABASE_URL_FILE", sharedParserFile(t, "empty-control-dsn", ""))
		_, err := (settings{}).sharedConfiguration()
		assertSharedParserError(t, err)
	})
}
