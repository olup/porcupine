package porcupine

import (
	"strings"
	"testing"

	lakeapi "github.com/olup/porcupine/internal/lake"
)

func sharedConfigurationFixture() Config {
	return Config{Shared: &SharedConfig{
		DatabaseURL: "postgres://fixture:private-shared-dsn-password@control.internal/metadata?sslmode=require",
		WorkerID:    "worker-one", RouteURL: "https://worker-one.internal", RoutingSecret: []byte("private-routing-secret-at-least-32-bytes"),
	}, DuckLake: &lakeapi.DuckLakeConfig{Directory: "/srv/worker-one/lake", PostgresURL: "postgres://fixture:private-lake-dsn-password@lake.internal/catalogs?sslmode=require", S3: &lakeapi.DuckLakeS3Config{Bucket: "analytical-fixture", Prefix: "data", Region: "eu-west-3"}}}
}

func assertPrivateSharedConfigError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("invalid shared runtime configuration accepted")
	}
	for _, private := range []string{"private-shared-dsn-password", "private-lake-dsn-password", "private-routing-secret-at-least-32-bytes"} {
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
			if err := validateSharedConfiguration(tc.config); err != nil {
				t.Fatal("valid mode rejected", err)
			}
		})
	}
	t.Run("control only", func(t *testing.T) {
		c := sharedConfigurationFixture()
		c.DuckLake = nil
		if err := validateSharedConfiguration(c); err != nil {
			t.Fatal(err)
		}
	})
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
		{"missing worker", func(c *Config) { c.Shared.WorkerID = "" }},
		{"blank worker", func(c *Config) { c.Shared.WorkerID = " \t " }},
		{"oversized worker", func(c *Config) { c.Shared.WorkerID = strings.Repeat("x", 129) }},
		{"missing route", func(c *Config) { c.Shared.RouteURL = "" }},
		{"missing routing key", func(c *Config) { c.Shared.RoutingSecret = nil }},
		{"short routing key", func(c *Config) { c.Shared.RoutingSecret = make([]byte, 31) }},
		{"missing lake PostgreSQL", func(c *Config) { c.DuckLake.PostgresURL = "" }},
		{"missing lake S3", func(c *Config) { c.DuckLake.S3 = nil }},
		{"local analytics in shared mode", func(c *Config) { c.DuckLake = &lakeapi.DuckLakeConfig{Directory: "/srv/local-only"} }},
		{"local backup", func(c *Config) { c.NativeBackup = &lakeapi.NativeBackupConfig{Directory: "/srv/backup"} }},
		{"S3 backup", func(c *Config) {
			c.NativeBackup = &lakeapi.NativeBackupConfig{S3: &lakeapi.DuckLakeS3Config{Bucket: "fixture-backup"}}
		}},
		{"native PostgreSQL without shared control", func(c *Config) { c.Shared = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := sharedConfigurationFixture()
			tc.change(&c)
			assertPrivateSharedConfigError(t, validateSharedConfiguration(c))
		})
	}
	t.Run("32 byte routing key", func(t *testing.T) {
		c := sharedConfigurationFixture()
		c.Shared.RoutingSecret = make([]byte, 32)
		if err := validateSharedConfiguration(c); err != nil {
			t.Fatal("minimum routing key rejected", err)
		}
	})
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
