package main

import (
	"bytes"
	"testing"

	"github.com/olup/porcupine/internal/apischema"
)

func TestSnapshotMatchesAuthoritativeSchema(t *testing.T) {
	first, err := document()
	if err != nil {
		t.Fatal(err)
	}
	second, err := document()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("OpenAPI snapshot generation is nondeterministic")
	}
	if !bytes.Equal(first, apischema.Document()) {
		t.Fatal("embedded OpenAPI is stale; run go run ./internal/cmd/genapi")
	}
}
