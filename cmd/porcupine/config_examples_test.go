package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRecommendedConfigurationExamples(t *testing.T) {
	t.Parallel()
	examples, err := filepath.Glob("../../docs/examples/configuration/*.json")
	if err != nil || len(examples) != 3 {
		t.Fatalf("recommended examples: %v, %v", examples, err)
	}
	for _, example := range examples {
		for _, docker := range []bool{false, true} {
			name := filepath.Base(example) + "/native"
			if docker {
				name = filepath.Base(example) + "/docker"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				path := filepath.Join(t.TempDir(), "porcupine.json")
				if err := writeInitialConfig(path, docker); err != nil {
					t.Fatal(err)
				}
				initial, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				var original, config FileConfig
				if err := json.Unmarshal(initial, &original); err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal(initial, &config); err != nil {
					t.Fatal(err)
				}
				patch, err := os.ReadFile(example)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := readSettings(bytes.NewReader(patch)); err != nil {
					t.Fatalf("example keys: %v", err)
				}
				if err := json.Unmarshal(patch, &config); err != nil {
					t.Fatal(err)
				}
				if config.BootstrapToken != original.BootstrapToken || config.Pipeline.Key != original.Pipeline.Key || config.Storage != original.Storage || config.Listen != original.Listen {
					t.Fatal("example replaced generated secrets, storage paths, or listener")
				}
				body, err := json.Marshal(config)
				if err != nil {
					t.Fatal(err)
				}
				if err := installConfig(path, bytes.NewReader(body)); err != nil {
					t.Fatalf("example configuration: %v", err)
				}
				var output bytes.Buffer
				if err := run([]string{"config", "check", "--config", path}, nil, &output); err != nil {
					t.Fatal(err)
				}
				if strings.Contains(output.String(), original.BootstrapToken) || strings.Contains(output.String(), original.Pipeline.Key) {
					t.Fatal("configuration check exposed generated credentials")
				}
			})
		}
	}
}
