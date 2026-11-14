package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"

	"github.com/olup/porcupine/internal/porcupine"
)

func runNativeRestore(ctx context.Context, input io.Reader, output io.Writer) error {
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
	backup, err := nativeBackupConfiguration()
	if err != nil {
		return err
	}
	backupConfig := porcupine.NativeBackupConfig{}
	if backup != nil {
		backupConfig = *backup
	}
	clickhouse, target, err := analyticsConfiguration()
	if err != nil {
		return err
	}
	if clickhouse != nil {
		return errors.New("offline native restore requires DuckLake target configuration")
	}
	if target == nil {
		target = &porcupine.DuckLakeConfig{}
	}
	target.Directory = request.TargetDuckLakeDirectory
	result, err := porcupine.RestoreNativeBackup(ctx, porcupine.NativeRestoreConfig{Request: request, Backup: backupConfig, DuckLake: *target})
	if err != nil {
		return err
	}
	return json.NewEncoder(output).Encode(result)
}

func dispatchNativeRestore() bool {
	if len(os.Args) != 2 || os.Args[1] != "restore-native-backup" {
		return false
	}
	if err := runNativeRestore(context.Background(), os.Stdin, os.Stdout); err != nil {
		// Errors never contain the private JSON request or recovery token.
		_, _ = io.WriteString(os.Stderr, "native restore failed: "+err.Error()+"\n")
		os.Exit(1)
	}
	return true
}
