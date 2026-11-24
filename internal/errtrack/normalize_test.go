package errtrack

import (
	"encoding/json"
	"strings"
	"testing"
)

func decodeInput(t *testing.T, input string) map[string]any {
	t.Helper()
	var properties map[string]any
	decoder := json.NewDecoder(strings.NewReader(input))
	decoder.UseNumber()
	if err := decoder.Decode(&properties); err != nil {
		t.Fatal(err)
	}
	return properties
}

func TestNormalizeKeepsBoundedExceptionFields(t *testing.T) {
	t.Parallel()
	properties := Normalize(decodeInput(t, `{
		"$exception_build_id":"prod-2026.11","$exception_release":"web-1.2",
		"$exception_fingerprint":"forged","$exception_degraded":true,
		"$current_url":"https://example.test/?secret=private-page", "arbitrary":"private-prop",
		"$exception_list":[{"type":"TypeError","value":"Failure for person@example.test, token=private-token calling https://api.example.test/?key=private-url","mechanism":{"input":"private-mechanism"},
			"stacktrace":{"frames":[{"filename":"https://name:private-user@CDN.example.test:443/app.js?token=private-query#private-fragment","lineno":4,"colno":9,"function":"render","chunk_id":"web.js","locals":{"input":"private-locals"}}]}}]
	}`))
	if properties.Degraded || properties.BuildID != "prod-2026.11" || properties.Release != "web-1.2" || len(properties.Fingerprint) != 64 || len(properties.List) != 1 {
		t.Fatalf("properties = %+v", properties)
	}
	e := properties.List[0]
	if e.Type != "TypeError" || e.Value != "Failure for [email], [redacted] calling [url]" {
		t.Fatalf("exception = %+v", e)
	}
	frame := e.Stacktrace.Frames[0]
	if frame != (Frame{Filename: "https://cdn.example.test/app.js", Line: 4, Column: 9, Function: "render", ChunkID: "web.js"}) {
		t.Fatalf("frame = %+v", frame)
	}
	raw, err := json.Marshal(properties)
	if err != nil || strings.Contains(string(raw), "private-") || strings.Contains(string(raw), "forged") {
		t.Fatalf("canonical JSON = %s, error = %v", raw, err)
	}
}

func TestFingerprintUsesBuildAndThrowSite(t *testing.T) {
	t.Parallel()
	input := decodeInput(t, `{"$exception_build_id":"v1","$exception_list":[{"type":"Error","value":"user 1","stacktrace":{"frames":[{"filename":"https://one.test/app.js?v=1","lineno":4,"colno":9,"function":"render"}]}}]}`)
	list := input["$exception_list"].([]any)
	exception := list[0].(map[string]any)
	stack := exception["stacktrace"].(map[string]any)
	frame := stack["frames"].([]any)[0].(map[string]any)
	first := Normalize(input).Fingerprint
	frame["filename"] = "https://ONE.test:443/app.js?v=2#frag"
	exception["value"] = "user 2"
	input["$exception_release"] = "v2"
	if got := Normalize(input).Fingerprint; got != first {
		t.Fatal("message, release, query, and fragment changed the fingerprint")
	}
	frame["colno"] = json.Number("10")
	if got := Normalize(input).Fingerprint; got == first {
		t.Fatal("a different throw location reused the fingerprint")
	}
	frame["colno"] = json.Number("9")
	input["$exception_build_id"] = "v2"
	if got := Normalize(input).Fingerprint; got == first {
		t.Fatal("a different build reused the fingerprint")
	}
}

func TestNormalizeDegradesMalformedAndExcessInput(t *testing.T) {
	t.Parallel()
	malformed := Normalize(decodeInput(t, `{"$exception_build_id":"bad/build","$exception_list":[null,7,{"type":"TypeError","value":false,"stacktrace":{"frames":[{"filename":"data:text/javascript,private"}]}}]}`))
	if !malformed.Degraded || malformed.BuildID != "" || len(malformed.Fingerprint) != 64 || len(malformed.List) != 1 || malformed.List[0].Stacktrace != nil {
		t.Fatalf("malformed = %+v", malformed)
	}

	frames := make([]any, maxFrames+5)
	for i := range frames {
		frames[i] = map[string]any{"filename": "/bundle.js", "lineno": i}
	}
	exception := map[string]any{"type": "RangeError", "stacktrace": map[string]any{"frames": frames}}
	list := make([]any, maxExceptions+2)
	for i := range list {
		list[i] = exception
	}
	bounded := Normalize(map[string]any{"$exception_list": list})
	if !bounded.Degraded || len(bounded.List) != maxExceptions {
		t.Fatalf("bounded = %+v", bounded)
	}
	got := bounded.List[0].Stacktrace.Frames
	if len(got) != maxFrames || got[0].Line != 5 || got[len(got)-1].Line != len(frames)-1 {
		t.Fatalf("retained frames = %+v", got)
	}
}

func TestAssetURL(t *testing.T) {
	t.Parallel()
	for _, input := range []string{"/bundle.js", "//example.test/bundle.js", "data:private", "https://", "https://example.test/%zz", "ftp://example.test/bundle.js", "https://example.test:99999/bundle.js"} {
		if _, ok := AssetURL(input); ok {
			t.Errorf("accepted %q as an asset URL", input)
		}
	}
	for input, want := range map[string]string{
		"HTTPS://person:pw@SITE.test:443/a%2Fb.js?secret#frag": "https://site.test/a%2Fb.js",
		"http://site.test:80":               "http://site.test/",
		"https://[2001:DB8::1]:8443/app.js": "https://[2001:db8::1]:8443/app.js",
	} {
		got, ok := AssetURL(input)
		if !ok || got != want {
			t.Errorf("AssetURL(%q) = %q, %v. Want %q, true", input, got, ok, want)
		}
	}
}
