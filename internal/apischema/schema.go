// Package apischema contains the generated offline management contract.
// Runtime authorization and dispatch use the live Huma registry, not this copy.
package apischema

import (
	"bytes"
	_ "embed"
)

//go:embed management.openapi.json
var document []byte

// Document returns an independent copy of the all-options OpenAPI snapshot.
func Document() []byte { return bytes.Clone(document) }
