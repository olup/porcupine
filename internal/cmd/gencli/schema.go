package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"strings"
)

// These types describe only the OpenAPI subset used by the command catalog.
// Huma remains authoritative; the generator never reconstructs its handlers,
// authorization metadata, or runtime schema registry.
type document struct {
	Version    string               `json:"openapi"`
	Paths      map[string]*pathItem `json:"paths"`
	Components struct {
		Schemas map[string]*schema `json:"schemas"`
	} `json:"components"`
}
type pathItem struct {
	Ref        string       `json:"$ref"`
	Parameters []*parameter `json:"parameters"`
	Get        *operation   `json:"get"`
	Put        *operation   `json:"put"`
	Post       *operation   `json:"post"`
	Delete     *operation   `json:"delete"`
	Options    *operation   `json:"options"`
	Head       *operation   `json:"head"`
	Patch      *operation   `json:"patch"`
	Trace      *operation   `json:"trace"`
}
type operation struct {
	OperationID string       `json:"operationId"`
	Summary     string       `json:"summary"`
	Parameters  []*parameter `json:"parameters"`
	RequestBody *requestBody `json:"requestBody"`
}
type commandOperation struct {
	*operation
	Method string
	Path   string
}
type parameter struct {
	Ref           string  `json:"$ref"`
	Name          string  `json:"name"`
	In            string  `json:"in"`
	Description   string  `json:"description"`
	Required      bool    `json:"required"`
	AllowReserved bool    `json:"allowReserved"`
	Style         string  `json:"style"`
	Schema        *schema `json:"schema"`
}
type requestBody struct {
	Ref      string                `json:"$ref"`
	Required bool                  `json:"required"`
	Content  map[string]*mediaType `json:"content"`
}
type mediaType struct {
	Schema *schema `json:"schema"`
}
type schema struct {
	Ref         string             `json:"$ref"`
	Type        schemaType         `json:"type"`
	Description string             `json:"description"`
	Default     any                `json:"default"`
	Enum        []any              `json:"enum"`
	Properties  map[string]*schema `json:"properties"`
	Required    []string           `json:"required"`
	Items       *schema            `json:"items"`
}

// OpenAPI 3.1 serializes Huma's nullable types as [type, "null"].
// This notation does not change the CLI's underlying flag or help type.
type schemaType string

func (t *schemaType) UnmarshalJSON(raw []byte) error {
	var single string
	if json.Unmarshal(raw, &single) == nil {
		*t = schemaType(single)
		return nil
	}
	var union []string
	if err := json.Unmarshal(raw, &union); err != nil {
		return fmt.Errorf("unsupported schema type")
	}
	var kind string
	nulls := 0
	for _, item := range union {
		if item == "null" {
			nulls++
			continue
		}
		if kind != "" || item == "" {
			return fmt.Errorf("unsupported schema type union")
		}
		kind = item
	}
	if kind == "" || nulls > 1 || len(union) > 2 {
		return fmt.Errorf("unsupported schema type union")
	}
	*t = schemaType(kind)
	return nil
}

func parseDocument(raw []byte) (*document, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var api document
	if err := decoder.Decode(&api); err != nil {
		return nil, fmt.Errorf("decode OpenAPI snapshot: %w", err)
	}
	if decoder.Decode(new(any)) != io.EOF {
		return nil, fmt.Errorf("unexpected data after OpenAPI snapshot")
	}
	if api.Version != "3.1.0" || len(api.Paths) == 0 {
		return nil, fmt.Errorf("expected an OpenAPI 3.1.0 management document")
	}
	return &api, nil
}

func resolve(api *document, value *schema) (*schema, error) {
	seen := map[string]bool{}
	for value != nil && value.Ref != "" {
		ref := value.Ref
		const prefix = "#/components/schemas/"
		if !strings.HasPrefix(ref, prefix) || seen[ref] {
			return nil, fmt.Errorf("unresolved schema reference %q", ref)
		}
		seen[ref] = true
		name, err := url.PathUnescape(strings.TrimPrefix(ref, prefix))
		if err != nil {
			return nil, fmt.Errorf("unresolved schema reference %q", ref)
		}
		var decoded strings.Builder
		for pos := 0; pos < len(name); pos++ {
			if name[pos] != '~' {
				decoded.WriteByte(name[pos])
				continue
			}
			pos++
			if pos >= len(name) {
				return nil, fmt.Errorf("unresolved schema reference %q", ref)
			}
			switch name[pos] {
			case '0':
				decoded.WriteByte('~')
			case '1':
				decoded.WriteByte('/')
			default:
				return nil, fmt.Errorf("unresolved schema reference %q", ref)
			}
		}
		value = api.Components.Schemas[decoded.String()]
		if value == nil {
			return nil, fmt.Errorf("unresolved schema reference %q", ref)
		}
	}
	if value == nil {
		return nil, fmt.Errorf("missing request schema")
	}
	return value, nil
}
