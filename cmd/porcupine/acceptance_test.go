package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/olup/porcupine/internal/porcupine"
)

func TestAcceptanceConfigurationContract(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"durable", "buffered", "unknown"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			settings, err := readSettings(strings.NewReader(`{"acceptance":{"mode":"` + mode + `"}}`))
			if err != nil {
				t.Fatal(err)
			}
			_, config, err := settings.serviceConfig()
			if err != nil {
				t.Fatal(err)
			}
			if config.AcceptanceMode != porcupine.AcceptanceMode(mode) {
				t.Fatal("mode lost by configuration", config.AcceptanceMode)
			}
			err = porcupine.ValidateConfig(config)
			if (err == nil) != (mode == "durable") {
				t.Fatal("unavailable mode was silently enabled", mode, err)
			}
		})
	}
}

func TestHostedConfigurationRefusesUnsafeInputs(t *testing.T) {
	t.Parallel()
	const candidate = `{"profile":"hosted","acceptance":{"mode":"buffered"},"shared":{"control_database_url":"postgres://fixture@127.0.0.1/porcupine?sslmode=disable"},"ducklake":{"s3":{"bucket":"profile-fixture","prefix":"scratch","region":"us-east-1"},"cleanup_grace":"240h","restore_window":"168h","restore_max_reader_time":"20m","restore_safety_margin":"1h"}}`
	parse := func(raw string) porcupine.Config {
		t.Helper()
		s, err := readSettings(strings.NewReader(raw))
		if err != nil {
			t.Fatal(err)
		}
		_, config, err := s.serviceConfig()
		if err != nil {
			t.Fatal(err)
		}
		return config
	}
	c := parse(candidate)
	if c.DuckLake.PostgresURL != c.Shared.DatabaseURL || c.DBPath != "" || c.DuckLake.Directory != "" {
		t.Fatal("hosted configuration supplied local state or split recovery authority")
	}
	if err := porcupine.ValidateConfig(c); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "porcupine.json")
	var output bytes.Buffer
	if err := run([]string{"config", "install", "--config", path}, strings.NewReader(candidate), &output); err != nil {
		t.Fatal(err)
	}
	output.Reset()
	if err := run([]string{"config", "check", "--config", path}, nil, &output); err != nil {
		t.Fatal(err)
	}
	for _, local := range []string{"PORCUPINE_DB", "PORCUPINE_CONTROL_DB", "PORCUPINE_DUCKLAKE_DIR"} {
		if strings.Contains(output.String(), `"`+local+`"`) {
			t.Fatalf("hosted config check showed local storage %s", local)
		}
	}
	for _, fragment := range []string{
		`,"storage":{"journal":"local.sqlite"}`,
		`,"storage":{"control":"control.sqlite"}`,
		`,"queue":{"receipt_retention":"24h"}`,
	} {
		if err := porcupine.ValidateConfig(parse(strings.TrimSuffix(candidate, "}") + fragment + "}")); err == nil {
			t.Fatalf("allowed incompatible configuration %s", fragment)
		}
	}
	for _, replace := range []struct{ before, after string }{
		{`"buffered"`, `"durable"`},
		{`"ducklake":{"s3":`, `"ducklake":{"postgres_url":"postgres://fixture@127.0.0.1/other?sslmode=disable","s3":`},
		{`"restore_safety_margin":"1h"`, `"restore_safety_margin":""`},
		{`"cleanup_grace":"240h"`, `"cleanup_grace":"168h"`},
	} {
		if err := porcupine.ValidateConfig(parse(strings.Replace(candidate, replace.before, replace.after, 1))); err == nil {
			t.Fatalf("allowed incompatible configuration %s", replace.after)
		}
	}
}

func TestHostedInitRequiresExplicitInfrastructure(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "porcupine.json")
	if err := run([]string{"init", "--hosted", "--config", path}, nil, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	s, err := loadSettings(path, true)
	if err != nil {
		t.Fatal(err)
	}
	if s.get("PORCUPINE_PROFILE") != "hosted" || s.get("PORCUPINE_ACCEPTANCE_MODE") != "buffered" {
		t.Fatal("hosted init did not select the hosted profile and buffered acceptance")
	}
	if s.get("PORCUPINE_DB") != "" || s.get("PORCUPINE_CONTROL_DB") != "" || s.get("PORCUPINE_DUCKLAKE_DIR") != "" {
		t.Fatal("hosted init supplied local storage")
	}
	if err := run([]string{"config", "check", "--config", path}, nil, &bytes.Buffer{}); err == nil {
		t.Fatal("incomplete hosted configuration accepted")
	}
}

func TestStandaloneConfigurationRefusesPostgresButAllowsS3(t *testing.T) {
	t.Parallel()
	const s3 = `"ducklake":{"s3":{"bucket":"standalone-fixture","prefix":"scratch","region":"us-east-1"}}`
	for _, tc := range []struct {
		name  string
		body  string
		valid bool
	}{
		{"default with S3", `{` + s3 + `}`, true},
		{"standalone with S3", `{"profile":"standalone",` + s3 + `}`, true},
		{"explicit owner profile", `{"profile":"shared"}`, false},
		{"implicit profile with PostgreSQL control", `{"shared":{"control_database_url":"` + testControlURL + `"},` + s3 + `}`, false},
		{"standalone with PostgreSQL catalog", `{"profile":"standalone","ducklake":{"postgres_url":"` + testLakeURL + `","s3":{"bucket":"standalone-fixture","prefix":"scratch","region":"us-east-1"}}}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "porcupine.json")
			if err := os.WriteFile(path, []byte(tc.body), 0600); err != nil {
				t.Fatal(err)
			}
			var output bytes.Buffer
			err := run([]string{"config", "check", "--config", path}, nil, &output)
			if (err == nil) != tc.valid {
				t.Fatalf("unexpected config check result: %v", err)
			}
			if tc.valid && !strings.Contains(output.String(), `"PORCUPINE_PROFILE": "standalone"`) {
				t.Fatal("config check did not show the effective standalone profile")
			}
			if !tc.valid {
				assertSharedParserError(t, err)
				if err := run([]string{"restore-native-backup", "--config", path}, strings.NewReader("{}"), &output); err == nil || !strings.Contains(err.Error(), "restore-native-backup") {
					t.Fatalf("mixed profile passed native restore validation: %v", err)
				}
			}
		})
	}
}
