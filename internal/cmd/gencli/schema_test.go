package main

import (
	"encoding/json"
	"testing"
)

func TestResolveLocalSchemaReferences(t *testing.T) {
	api := snapshot(t)
	api.Components.Schemas["A/B~C"] = &schema{Type: "object"}
	api.Components.Schemas["Alias"] = &schema{Ref: "#/components/schemas/A~1B~0C"}
	got, err := resolve(api, &schema{Ref: "#/components/schemas/Alias"})
	if err != nil || got.Type != "object" {
		t.Fatal(got, err)
	}
	for _, ref := range []string{"https://example.test/schema.json", "#/components/schemas/Missing", "#/components/schemas/A~2B", "#/not-schemas/Alias"} {
		if _, err := resolve(api, &schema{Ref: ref}); err == nil {
			t.Fatalf("accepted reference %q", ref)
		}
	}
	api.Components.Schemas["Loop"] = &schema{Ref: "#/components/schemas/Loop"}
	if _, err := resolve(api, &schema{Ref: "#/components/schemas/Loop"}); err == nil {
		t.Fatal("cyclic schema reference accepted")
	}
}

func TestNullableSerializedTypesPreserveFlagKinds(t *testing.T) {
	for _, raw := range []string{`"array"`, `["array","null"]`, `["null","array"]`} {
		var kind schemaType
		if err := json.Unmarshal([]byte(raw), &kind); err != nil || kind != "array" {
			t.Fatalf("%s: %q, %v", raw, kind, err)
		}
	}
	for _, raw := range []string{`[]`, `["null"]`, `["string","number"]`, `false`} {
		var kind schemaType
		if err := json.Unmarshal([]byte(raw), &kind); err == nil {
			t.Fatalf("accepted unsupported schema type %s", raw)
		}
	}
}

func TestRejectMalformedSerializedContract(t *testing.T) {
	for _, raw := range []string{`{`, `null`, `{"openapi":"3.0.0","paths":{"/":{}}}`, `{"openapi":"3.1.0","paths":{}}`, `{"openapi":"3.1.0","paths":{"/":{}}} {}`} {
		if _, err := parseDocument([]byte(raw)); err == nil {
			t.Fatalf("accepted invalid contract %s", raw)
		}
	}
}
