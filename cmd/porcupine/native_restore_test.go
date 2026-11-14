package main

import (
	"bytes"
	"context"
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
		err := runNativeRestore(context.Background(), strings.NewReader(body), &output)
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
	err := runNativeRestore(context.Background(), strings.NewReader(`{}`), &output)
	if err == nil || !strings.Contains(err.Error(), "PORCUPINE_DUCKLAKE_MEMORY_MB") {
		t.Fatal("restore ignored target resource configuration", err)
	}
}
