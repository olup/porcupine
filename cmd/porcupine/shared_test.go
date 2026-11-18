package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const testControlURL = "postgres://fixture:private-control-password@control.internal/porcupine?sslmode=require"
const testLakeURL = "postgresql://fixture:private-lake-password@lake.internal/metadata?sslmode=require"

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
		t.Fatal("invalid secret setting accepted")
	}
	for _, value := range append([]string{testControlURL, testLakeURL, "private-control-password", "private-lake-password"}, private...) {
		if value != "" && strings.Contains(err.Error(), value) {
			t.Fatal("configuration error exposed a secret or private filename")
		}
	}
}

func TestControlDatabaseConfiguration(t *testing.T) {
	t.Parallel()
	config, err := (settings{}).sharedConfiguration()
	if err != nil || config != nil {
		t.Fatal("PostgreSQL control configured by default", err)
	}
	config, err = (settings{"PORCUPINE_CONTROL_DATABASE_URL": testControlURL}).sharedConfiguration()
	if err != nil || config == nil || config.DatabaseURL != testControlURL {
		t.Fatal("control database setting was not parsed", err)
	}
}

func TestSecretFileSourcesAndExclusivity(t *testing.T) {
	t.Parallel()
	for name, want := range map[string]string{
		"PORCUPINE_CONTROL_DATABASE_URL":  testControlURL,
		"PORCUPINE_DUCKLAKE_POSTGRES_URL": testLakeURL,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			path := sharedParserFile(t, "private-secret-file", want+"\n")
			s := settings{name + "_FILE": path}
			value, err := s.secretSetting(name)
			if err != nil || value != want {
				t.Fatal("mounted secret file not read", err)
			}
			s[name] = want
			_, err = s.secretSetting(name)
			assertSharedParserError(t, err, path)
		})
	}
	t.Run("file-backed database configuration", func(t *testing.T) {
		t.Parallel()
		s := settings{
			"PORCUPINE_CONTROL_DATABASE_URL_FILE":  sharedParserFile(t, "mounted-control-dsn", testControlURL+"\n"),
			"PORCUPINE_DUCKLAKE_POSTGRES_URL_FILE": sharedParserFile(t, "mounted-catalog-dsn", testLakeURL+"\n"),
		}
		control, err := s.sharedConfiguration()
		if err != nil || control == nil || control.DatabaseURL != testControlURL {
			t.Fatal("file-backed control DSN failed", err)
		}
		lake, err := s.analyticsConfiguration()
		if err != nil || lake.PostgresURL != testLakeURL {
			t.Fatal("file-backed analytical DSN failed", err)
		}
	})
}

func TestSecretSettingRejectsInvalidFilesAndValues(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"PORCUPINE_CONTROL_DATABASE_URL", "PORCUPINE_DUCKLAKE_POSTGRES_URL"} {
		for _, tc := range []struct{ name, content string }{
			{"empty", ""}, {"whitespace", " \r\n\t "}, {"oversize", strings.Repeat("x", (16<<10)+1)},
			{"NUL", "private-value\x00hidden"}, {"embedded LF", "private-value\nhidden"}, {"embedded CR", "private-value\rhidden"},
		} {
			t.Run(name+"/"+tc.name, func(t *testing.T) {
				t.Parallel()
				path := sharedParserFile(t, "private-secret-path", tc.content)
				_, err := (settings{name + "_FILE": path}).secretSetting(name)
				assertSharedParserError(t, err, path, tc.content)
			})
		}
		for _, kind := range []string{"missing", "directory"} {
			t.Run(name+"/"+kind, func(t *testing.T) {
				t.Parallel()
				path := t.TempDir()
				if kind == "missing" {
					path = filepath.Join(path, "private-missing-secret")
				}
				_, err := (settings{name + "_FILE": path}).secretSetting(name)
				assertSharedParserError(t, err, path)
			})
		}
		for _, value := range []string{"private-value\nhidden", "private-value\rhidden"} {
			t.Run(name+"/environment control characters", func(t *testing.T) {
				t.Parallel()
				_, err := (settings{name: value}).secretSetting(name)
				assertSharedParserError(t, err, value)
			})
		}
	}
	// An explicitly configured empty control DSN file is an error.
	t.Run("empty file cannot disable PostgreSQL control", func(t *testing.T) {
		t.Parallel()
		s := settings{"PORCUPINE_CONTROL_DATABASE_URL_FILE": sharedParserFile(t, "empty-control-dsn", "")}
		_, err := s.sharedConfiguration()
		assertSharedParserError(t, err)
	})
}
