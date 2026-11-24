package errtrack

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestSourceMapOriginal(t *testing.T) {
	t.Parallel()

	// For a generated line beginning with an emoji, the following character
	// is at column 3 in a browser stack and column 2 in the map.
	source := []byte(`{"version":3,"sourceRoot":"../src/","sources":["./app.ts"],"names":["render"],"mappings":"EAIIA,E;;KACD"}`)
	m, err := ParseSourceMap(source)
	if err != nil {
		t.Fatal(err)
	}

	for _, tt := range []struct {
		line, column int
		want         OriginalPosition
	}{
		{1, 2, OriginalPosition{}},
		{1, 3, OriginalPosition{Filename: "../src/app.ts", Line: 5, Column: 5, Function: "render"}},
		{1, 4, OriginalPosition{Filename: "../src/app.ts", Line: 5, Column: 5, Function: "render"}},
		{1, 5, OriginalPosition{}},
		{2, 3, OriginalPosition{}},
		{3, 5, OriginalPosition{}},
		{3, 6, OriginalPosition{Filename: "../src/app.ts", Line: 6, Column: 4}},
		{4, 6, OriginalPosition{}},
		{1, 0, OriginalPosition{}},
	} {
		got, ok := m.Original(tt.line, tt.column)
		if got != tt.want || ok != (tt.want != (OriginalPosition{})) {
			t.Errorf("Original(%d, %d) = %+v, %v, want %+v", tt.line, tt.column, got, ok, tt.want)
		}
	}

	otherBuild, err := ParseSourceMap([]byte(`{"version":3,"sources":["other.ts"],"mappings":"EAEE"}`))
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := otherBuild.Original(1, 3); !ok || got != (OriginalPosition{Filename: "other.ts", Line: 3, Column: 3}) {
		t.Fatalf("unexpected position for the other build: %+v, %v", got, ok)
	}
}

func TestPrepareSourceMap(t *testing.T) {
	t.Parallel()

	source := []byte(`{
		"version":3,"private":"top-level","sections":[
		{"offset":{"line":0,"column":3,"private":"offset"},"private":"section","map":{
			"version":3,"file":"bundle.js","sources":["src/first.ts"],"names":[],
			"sourcesContent":["private source code"],"mappings":"AAAA;EACA","private":"map"
		}},
		{"offset":{"line":2,"column":2},"map":{
			"version":3,"sources":["src/second.ts"],"mappings":"AAAA"
		}}
	]}`)
	prepared, err := PrepareSourceMap(source)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(prepared, []byte("private")) || bytes.Contains(prepared, []byte("sourcesContent")) {
		t.Fatalf("canonical source map retains omitted fields: %s", prepared)
	}
	second, err := PrepareSourceMap(prepared)
	if err != nil || !bytes.Equal(prepared, second) {
		t.Fatalf("preparing a canonical map changes it: %s, %v", second, err)
	}
	parsed, err := ParseSourceMap(prepared)
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		line, column int
		want         string
	}{
		{1, 3, ""},
		{1, 4, "src/first.ts"},
		{2, 2, ""},
		{2, 3, "src/first.ts"},
		{3, 2, ""},
		{3, 3, "src/second.ts"},
	} {
		got, ok := parsed.Original(tt.line, tt.column)
		if got.Filename != tt.want || ok != (tt.want != "") {
			t.Errorf("Original(%d, %d) = %+v, %v, want %q", tt.line, tt.column, got, ok, tt.want)
		}
	}
}

func TestSourceMapRejectsInvalidInput(t *testing.T) {
	t.Parallel()

	mapping := func(segment string) string {
		return fmt.Sprintf(`{"version":3,"sources":["private"],"names":["private"],"mappings":%q}`, segment)
	}
	for _, raw := range []string{
		`{"version":2,"sources":[],"mappings":""}`,
		`{"version":3,"sources":[]}`,
		`{"version":3,"sources":[{}],"mappings":"AAAA"}`,
		`{"version":3,"sections":[{"offset":{"line":0,"column":0},"map":null}]}`,
		`{"version":3,"sections":[{"offset":{"line":-1,"column":0},"map":{"version":3,"sources":[],"mappings":""}}]}`,
		`{"version":3,"sections":[{"offset":{"line":0},"map":{"version":3,"sources":[],"mappings":""}}]}`,
		`{"version":3,"sections":[{"offset":{"line":0,"column":0},"map":{"version":3,"sources":[null],"mappings":";AAAA"}},{"offset":{"line":1,"column":0},"map":{"version":3,"sources":[],"mappings":""}}]}`,
		mapping("ACAA"),      // Out-of-range source index.
		mapping("ADAA"),      // Negative source index.
		mapping("AAAAC"),     // Out-of-range name index.
		mapping("AADA"),      // Negative original line.
		mapping("AAAD"),      // Negative original column.
		mapping("AAAA,DAAA"), // Negative generated column.
		mapping("AAAA,"),
		mapping("AAAA,;"),
		mapping("AAA"),
		mapping("AAAAAA"),
		mapping("AA%A"),
		mapping("///////A"),
		strings.Repeat(" ", MaxSourceMapBytes+1),
	} {
		_, err := PrepareSourceMap([]byte(raw))
		if err == nil || strings.Contains(err.Error(), "private") {
			t.Fatalf("PrepareSourceMap(%q) returned %v", raw[:min(len(raw), 200)], err)
		}
		_, err = ParseSourceMap([]byte(raw))
		if err == nil {
			t.Fatalf("ParseSourceMap(%q) accepted invalid input", raw[:min(len(raw), 200)])
		}
	}
}

func TestSourceMapDoesNotFetchURLs(t *testing.T) {
	t.Parallel()

	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		http.Error(w, "unexpected request", http.StatusBadRequest)
	}))
	defer server.Close()

	for _, reference := range []string{server.URL + "/map", "file:///private/map", "other.map"} {
		source := []byte(fmt.Sprintf(`{"version":3,"sections":[{"offset":{"line":0,"column":0},"url":%q}]}`, reference))
		if _, err := PrepareSourceMap(source); err == nil {
			t.Fatalf("accepted external section %q", reference)
		}
	}
	combined := []byte(fmt.Sprintf(`{"version":3,"sections":[{"offset":{"line":0,"column":0},"url":%q,"map":{"version":3,"sources":[],"mappings":""}}]}`, server.URL))
	if _, err := PrepareSourceMap(combined); err == nil {
		t.Fatal("accepted a section with both external and inline maps")
	}
	source := []byte(fmt.Sprintf(`{"version":3,"sourceRoot":%q,"sources":["app.ts"],"mappings":"AAAA"}`, server.URL+"/src"))
	m, err := ParseSourceMap(source)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := m.Original(1, 1)
	if !ok || got.Filename != "/src/app.ts" {
		t.Fatalf("unexpected position: %+v, %v", got, ok)
	}
	if requests.Load() != 0 {
		t.Fatalf("source map parser issued %d HTTP requests", requests.Load())
	}
}

func TestSourceMapOutOfOrderColumnsAndNullSources(t *testing.T) {
	t.Parallel()

	m, err := ParseSourceMap([]byte(`{"version":3,"sources":["first.ts",null],"mappings":"KAAA,FAAC,CCAA"}`))
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := m.Original(4, 1); ok {
		t.Fatalf("unexpected mapping for missing line: %+v", got)
	}
	if got, ok := m.Original(1, 4); !ok || got.Column != 2 {
		t.Fatalf("unexpected mapping after negative delta: %+v, %v", got, ok)
	}
	if got, ok := m.Original(1, 5); ok {
		t.Fatalf("unexpected mapping for null source: %+v", got)
	}
}

func TestSourceMapOriginalPath(t *testing.T) {
	t.Parallel()

	m, err := ParseSourceMap([]byte(`{"version":3,"sourceRoot":"../src","sources":["./nested/../app name.ts?private=token#fragment"],"mappings":"AAAA"}`))
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := m.Original(1, 1); !ok || got.Filename != "../src/app%20name.ts" {
		t.Fatalf("unexpected original path: %+v, %v", got, ok)
	}
}
