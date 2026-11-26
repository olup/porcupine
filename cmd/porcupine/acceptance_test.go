package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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

const hostedConfigurationJSON = `{"profile":"hosted","acceptance":{"mode":"buffered"},"shared":{"control_database_url":"postgres://fixture@127.0.0.1/porcupine?sslmode=disable"},"ducklake":{"s3":{"bucket":"profile-fixture","prefix":"scratch","region":"us-east-1"},"cleanup_grace":"240h","restore_window":"168h","restore_max_reader_time":"20m","restore_safety_margin":"1h"}}`

func TestHostedFlushIntervalConfiguration(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		value string
		want  time.Duration
	}{
		{"1s", time.Second},
		{"10s", 10 * time.Second},
		{"30s", 30 * time.Second},
		{"0", 0},
		{"999ms", 0},
		{"30s1ns", 0},
		{"invalid", 0},
	} {
		t.Run(test.value, func(t *testing.T) {
			t.Parallel()
			raw := strings.Replace(hostedConfigurationJSON, `"mode":"buffered"`, `"mode":"buffered","flush_interval":"`+test.value+`"`, 1)
			s, err := readSettings(strings.NewReader(raw))
			if err != nil {
				t.Fatal(err)
			}
			_, config, err := s.serviceConfig()
			if err == nil {
				err = porcupine.ValidateConfig(config)
			}
			if test.want == 0 {
				if err == nil || !strings.Contains(strings.ToLower(err.Error()), "flush_interval") {
					t.Fatal("invalid flush interval was accepted or error lacked the setting name", err)
				}
				return
			}
			if err != nil || config.HostedFlushInterval != test.want {
				t.Fatal("hosted flush interval did not reach the service configuration", config.HostedFlushInterval, err)
			}
		})
	}
	for _, profile := range []porcupine.DeploymentProfile{"", porcupine.ProfileStandalone} {
		config := porcupine.Config{Profile: profile, HostedFlushInterval: 10 * time.Second}
		if err := porcupine.ValidateConfig(config); err == nil || !strings.Contains(err.Error(), "acceptance.flush_interval requires the hosted profile") {
			t.Fatal("flush interval was accepted outside the hosted profile", profile, err)
		}
	}
}

func TestHostedConfigurationRefusesUnsafeInputs(t *testing.T) {
	t.Parallel()
	const candidate = hostedConfigurationJSON
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

func TestHostedDurableJournalConfiguration(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		path string
		ok   bool
	}{
		{"", false}, {":memory:", false}, {"/var/lib/porcupine/live.sqlite", true},
	} {
		t.Run(tc.path, func(t *testing.T) {
			t.Parallel()
			raw := strings.Replace(hostedConfigurationJSON, `"mode":"buffered"`, `"mode":"durable"`, 1)
			raw = strings.TrimSuffix(raw, "}") + `,"storage":{"journal":"` + tc.path + `"}}`
			settings, err := readSettings(strings.NewReader(raw))
			if err != nil {
				t.Fatal(err)
			}
			_, config, err := settings.serviceConfig()
			if err != nil || config.DBPath != tc.path {
				t.Fatal("journal path did not reach service configuration", config.DBPath, err)
			}
			if err := porcupine.ValidateConfig(config); (err == nil) != tc.ok {
				t.Fatalf("unexpected validation for %q: %v", tc.path, err)
			}
		})
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
