package main

import (
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

func TestHostedConfigurationRefusesUnsafeInputsAndStaysGated(t *testing.T) {
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
	if _, err := porcupine.Open(c); err == nil || !strings.Contains(err.Error(), "startup is unavailable") {
		t.Fatalf("unexpected public hosted startup: %v", err)
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
		{`"shared":{"control_database_url":`, `"shared":{"worker_id":"owner","control_database_url":`},
		{`"ducklake":{"s3":`, `"ducklake":{"postgres_url":"postgres://fixture@127.0.0.1/other?sslmode=disable","s3":`},
		{`"restore_safety_margin":"1h"`, `"restore_safety_margin":""`},
		{`"cleanup_grace":"240h"`, `"cleanup_grace":"168h"`},
	} {
		if err := porcupine.ValidateConfig(parse(strings.Replace(candidate, replace.before, replace.after, 1))); err == nil {
			t.Fatalf("allowed incompatible configuration %s", replace.after)
		}
	}
}
