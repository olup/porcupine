package apischema

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestDocumentIsAnIndependentOpenAPICopy(t *testing.T) {
	first, second := Document(), Document()
	var api struct {
		Version string         `json:"openapi"`
		Paths   map[string]any `json:"paths"`
	}
	if err := json.Unmarshal(first, &api); err != nil || api.Version != "3.1.0" || len(api.Paths) == 0 {
		t.Fatal("missing generated OpenAPI document", err)
	}
	first[0] = 'x'
	if bytes.Equal(first, second) || !json.Valid(Document()) {
		t.Fatal("caller mutated embedded contract")
	}
}
