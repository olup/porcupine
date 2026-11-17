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
