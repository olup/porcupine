package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"

	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/porcupine"
)

func (s settings) runNativeRestore(ctx context.Context, input io.Reader, output io.Writer) error {
	// The request carries private recovery credentials. Never include decoder
	// input, the request, or generated ingestion keys in diagnostics.
	body, err := io.ReadAll(io.LimitReader(input, (64<<10)+1))
	if err != nil || len(body) > 64<<10 {
		return errors.New("private native restore request exceeds the 64 KiB limit")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var request porcupine.NativeRestoreRequest
	if err := decoder.Decode(&request); err != nil {
		return errors.New("invalid private native restore request")
	}
	var trailer any
	if decoder.Decode(&trailer) != io.EOF {
		return errors.New("native restore accepts exactly one private JSON request")
	}
	authConfig, err := s.authConfiguration()
	if err != nil {
		return err
	}
	if authConfig == nil {
		configured := s.get("PORCUPINE_AUTH_MODE") != ""
		configFields(reflect.ValueOf(FileConfig{}), "", func(name string, _ reflect.Value, _ bool) {
			if s.get(name) != "" && (strings.HasPrefix(name, "PORCUPINE_OAUTH_") || strings.HasPrefix(name, "PORCUPINE_OIDC_") || strings.HasPrefix(name, "PORCUPINE_SMTP_") || name == "PORCUPINE_ADMIN_EMAIL" || name == "PORCUPINE_BOOTSTRAP_TOKEN" || name == "PORCUPINE_RESTORE_IDENTITY_BOOTSTRAP") {
				configured = true
			}
		})
		if configured {
			authConfig = &porcupine.AuthConfig{Mode: "builtin"}
		}
	}
	backup, err := s.nativeBackupConfiguration()
	if err != nil {
		return err
	}
	backupConfig := lakeapi.NativeBackupConfig{}
	if backup != nil {
		backupConfig = *backup
	}
	target, err := s.analyticsConfiguration()
	if err != nil {
		return err
	}
	if target == nil {
		target = &lakeapi.DuckLakeConfig{}
	}
	target.Directory = request.TargetDuckLakeDirectory
	result, err := porcupine.RestoreNativeBackup(ctx, porcupine.NativeRestoreConfig{Request: request, Backup: backupConfig, DuckLake: *target, Auth: authConfig})
	if err != nil {
		return err
	}
	return json.NewEncoder(output).Encode(result)
}
