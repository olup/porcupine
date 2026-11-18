package porcupine

import (
	"strings"
	"testing"
	"time"

	lakeapi "github.com/olup/porcupine/internal/lake"
)

func sharedConfigurationFixture() Config {
	const dsn = "postgres://fixture:private-shared-dsn-password@control.internal/metadata?sslmode=require"
	return Config{Profile: ProfileHosted, AcceptanceMode: AcceptanceModeBuffered, Shared: &SharedConfig{DatabaseURL: dsn},
		DuckLake: &lakeapi.DuckLakeConfig{PostgresURL: dsn, S3: &lakeapi.DuckLakeS3Config{Bucket: "analytical-fixture", Prefix: "data", Region: "eu-west-3"},
			CleanupGrace: 4 * time.Hour, RestoreWindow: time.Hour, RestoreMaxReaderTime: time.Hour, RestoreSafetyMargin: time.Hour}}
}

func assertPrivateSharedConfigError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("invalid shared runtime configuration accepted")
	}
	for _, private := range []string{"private-shared-dsn-password", "private-lake-dsn-password"} {
		if strings.Contains(err.Error(), private) {
			t.Fatal("shared validation error exposed credentials")
		}
	}
}

func TestSharedRuntimeConfigurationBoundaries(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		config Config
	}{
		{"local default", Config{}},
		{"local analytics", Config{DuckLake: &lakeapi.DuckLakeConfig{Directory: "/srv/lake"}}},
		{"local S3", Config{DuckLake: &lakeapi.DuckLakeConfig{Directory: "/srv/lake", S3: &lakeapi.DuckLakeS3Config{Bucket: "fixture", Prefix: "analytics", Region: "eu-west-3"}}}},
		{"complete shared", sharedConfigurationFixture()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateConfig(tc.config); err != nil {
				t.Fatal("valid mode rejected", err)
			}
		})
	}
	for _, tc := range []struct {
		name   string
		change func(*Config)
	}{
		{"empty shared", func(c *Config) { c.Shared = &SharedConfig{} }},
		{"missing database", func(c *Config) { c.Shared.DatabaseURL = "" }},
		{"non PostgreSQL database", func(c *Config) { c.Shared.DatabaseURL = "https://private-shared-dsn-password@control.internal" }},
		{"PostgreSQL without host", func(c *Config) { c.Shared.DatabaseURL = "postgres:///metadata" }},
		{"malformed database", func(c *Config) { c.Shared.DatabaseURL = "postgres://%zz:private-shared-dsn-password@control.internal" }},
		{"NUL database", func(c *Config) {
			c.Shared.DatabaseURL = "postgres://control.internal/metadata\x00private-shared-dsn-password"
		}},
		{"missing lake PostgreSQL", func(c *Config) { c.DuckLake.PostgresURL = "" }},
		{"missing lake S3", func(c *Config) { c.DuckLake.S3 = nil }},
		{"local analytics in shared mode", func(c *Config) { c.DuckLake = &lakeapi.DuckLakeConfig{Directory: "/srv/local-only"} }},
		{"local backup", func(c *Config) { c.NativeBackup = &lakeapi.NativeBackupConfig{Directory: "/srv/backup"} }},
		{"S3 backup", func(c *Config) {
			c.NativeBackup = &lakeapi.NativeBackupConfig{S3: &lakeapi.DuckLakeS3Config{Bucket: "fixture-backup"}}
		}},
		{"hosted without shared control", func(c *Config) { c.Shared = nil }},
		{"PostgreSQL control in standalone mode", func(c *Config) {
			c.Profile = ProfileStandalone
			c.AcceptanceMode = AcceptanceModeDurable
			c.DuckLake = nil
		}},
		{"PostgreSQL analytics in standalone mode", func(c *Config) {
			c.Profile = ProfileStandalone
			c.AcceptanceMode = AcceptanceModeDurable
			c.Shared = nil
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := sharedConfigurationFixture()
			tc.change(&c)
			assertPrivateSharedConfigError(t, ValidateConfig(c))
		})
	}
}

func TestSharedNativeConfigurationValidationWithoutEngine(t *testing.T) {
	t.Parallel()
	c := sharedConfigurationFixture().DuckLake
	c.SharedSourceID = "12345678-1234-1234-1234-123456789abc"
	if err := lakeapi.ValidateSharedNativeConfig(c); err != nil {
		t.Fatal("valid shared native configuration rejected", err)
	}
	for _, tc := range []struct {
		name   string
		change func(*lakeapi.DuckLakeConfig)
	}{
		{"missing source", func(c *lakeapi.DuckLakeConfig) { c.SharedSourceID = "" }},
		{"invalid source", func(c *lakeapi.DuckLakeConfig) { c.SharedSourceID = strings.Repeat("/", 36) }},
		{"missing PostgreSQL", func(c *lakeapi.DuckLakeConfig) { c.PostgresURL = "" }},
		{"wrong scheme", func(c *lakeapi.DuckLakeConfig) { c.PostgresURL = "https://private-lake-dsn-password@lake.internal" }},
		{"missing host", func(c *lakeapi.DuckLakeConfig) { c.PostgresURL = "postgres:///metadata" }},
		{"missing S3", func(c *lakeapi.DuckLakeConfig) { c.S3 = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			copy := *c
			tc.change(&copy)
			assertPrivateSharedConfigError(t, lakeapi.ValidateSharedNativeConfig(&copy))
		})
	}
}
