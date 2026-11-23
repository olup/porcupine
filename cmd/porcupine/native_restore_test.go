package main

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
)

func TestNativeRestoreCommandRejectsUnsafePrivateInput(t *testing.T) {
	secret := "a-private-token-that-must-not-be-in-errors"
	for _, body := range []string{
		`{"bootstrap_token":"` + secret + `","unknown_field":true}`,
		`{"bootstrap_token":"` + secret + `"} {}`,
		`{"bootstrap_token":"` + secret + `"`,
		`{}` + strings.Repeat(" ", 64<<10),
	} {
		var output bytes.Buffer
		err := (settings{}).runNativeRestore(context.Background(), strings.NewReader(body), &output)
		if err == nil {
			t.Fatal("unsafe restore request accepted")
		}
		if strings.Contains(err.Error(), secret) || output.Len() != 0 {
			t.Fatal("private restore request exposed in diagnostics")
		}
	}
}

func TestNativeRestoreCommandValidatesTargetResourceEnvironment(t *testing.T) {
	t.Setenv("PORCUPINE_DUCKLAKE_MEMORY_MB", "not-a-memory-budget")
	t.Setenv("PORCUPINE_DUCKLAKE_DIR", t.TempDir())
	var output bytes.Buffer
	err := (settings{}).runNativeRestore(context.Background(), strings.NewReader(`{}`), &output)
	if err == nil || !strings.Contains(err.Error(), "PORCUPINE_DUCKLAKE_MEMORY_MB") {
		t.Fatal("restore ignored target resource configuration", err)
	}
}

func TestNativeRestoreCommandInfersDestinationAuthMode(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, destination, request, want, provider string
	}{
		{name: "offline request requires a mode", want: "auth_mode is required"},
		{name: "offline external request fallback", request: "external", want: "local_bundle_path is required"},
		{name: "explicit builtin cannot adopt external", destination: "builtin", request: "external", want: "auth_mode differs"},
		{name: "builtin OAuth constrains destination", request: "external", provider: "PORCUPINE_OAUTH_ISSUER", want: "auth_mode differs"},
		{name: "builtin email constrains destination", request: "external", provider: "PORCUPINE_ADMIN_EMAIL", want: "auth_mode differs"},
		{name: "builtin OIDC constrains destination", request: "external", provider: "PORCUPINE_OIDC_ISSUER", want: "auth_mode differs"},
		{name: "external cannot adopt builtin", destination: "external", request: "builtin", want: "auth_mode differs"},
		{name: "external inferred without bootstrap", destination: "external", want: "local_bundle_path is required"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			request := map[string]any{"backup_id": "pc_backup_test", "target_db_path": filepath.Join(root, "journal.db"), "target_ducklake_directory": filepath.Join(root, "lake"), "reset_credentials_acknowledged": true}
			if tc.request != "" {
				request["auth_mode"] = tc.request
			}
			body, err := json.Marshal(request)
			if err != nil {
				t.Fatal(err)
			}
			var output bytes.Buffer
			config := settings{"PORCUPINE_AUTH_MODE": tc.destination}
			if tc.provider != "" {
				config[tc.provider] = "configured-provider"
			}
			err = config.runNativeRestore(t.Context(), bytes.NewReader(body), &output)
			if err == nil || !strings.Contains(err.Error(), tc.want) || output.Len() != 0 {
				t.Fatalf("destination auth mode was not enforced: %v %s", err, output.String())
			}
		})
	}
}
