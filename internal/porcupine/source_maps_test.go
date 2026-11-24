package porcupine

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/olup/porcupine/internal/query"
)

const exceptionTestMap = `{"version":3,"sources":["src/app.ts"],"names":["startApp"],"mappings":"AAAAA","sourcesContent":["embedded source must stay private"]}`

func uploadTestSourceMap(t *testing.T, s *Service, project, token, body string, headers map[string]string, status int) map[string]any {
	t.Helper()
	r := httptest.NewRequest(http.MethodPut, "/api/v1/projects/"+project+"/source-maps", strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set("Content-Type", "application/octet-stream")
	for name, value := range headers {
		r.Header.Set(name, value)
	}
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != status {
		t.Fatalf("source map upload returned %d, want %d: %s", w.Code, status, w.Body.String())
	}
	var result map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestSourceMapPermissionsAndLifecycle(t *testing.T) {
	t.Parallel()
	s := service(t, 20)
	a, public := project(t, s)
	b, _ := project(t, s)
	_, uploader := key(t, s, []string{"sourcemaps:write"}, []string{a})
	_, reader := key(t, s, []string{"events:read"}, []string{a})
	headers := map[string]string{"X-Source-Map-Chunk-ID": "chunk"}
	uploadTestSourceMap(t, s, a, public, exceptionTestMap, headers, 401)
	uploadTestSourceMap(t, s, a, reader, exceptionTestMap, headers, 403)
	uploadTestSourceMap(t, s, b, uploader, exceptionTestMap, headers, 403)
	uploadTestSourceMap(t, s, a, uploader, `{"version":2}`, headers, 400)
	first := uploadTestSourceMap(t, s, a, uploader, exceptionTestMap, headers, 200)
	retry := uploadTestSourceMap(t, s, a, uploader, strings.Replace(exceptionTestMap, "embedded source must stay private", "other source", 1), headers, 200)
	if first["sha256"] == "" || first["sha256"] != retry["sha256"] {
		t.Fatalf("source map retry changed digest: %v %v", first, retry)
	}
	uploadTestSourceMap(t, s, a, uploader, strings.Replace(exceptionTestMap, "app.ts", "other.ts", 1), headers, 409)
	uploadTestSourceMap(t, s, b, adminToken, exceptionTestMap, headers, 200)
	request(t, s, "DELETE", "/api/v1/projects/"+a, adminToken, "", 204)
	var count int
	if err := s.control.QueryRow(`SELECT count(*) FROM project_source_maps WHERE project_id = $1`, a).Scan(&count); err != nil || count != 0 {
		t.Fatalf("project deletion left %d private maps: %v", count, err)
	}
	if err := s.control.QueryRow(`SELECT count(*) FROM project_source_maps WHERE project_id = $1`, b).Scan(&count); err != nil || count != 1 {
		t.Fatalf("deletion affected the other project: %d %v", count, err)
	}
}

func TestErrorTrackingDuckLakeJourney(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	s := duckLakeJourneyService(t)
	a, public := project(t, s)
	b, _ := project(t, s)
	_, reader := key(t, s, []string{"events:read"}, []string{a})
	_, writer := key(t, s, []string{"sourcemaps:write"}, []string{a})
	asset := "https://cdn.test/app.js"
	payload := func(uuid, extra string) string {
		return `{"event":"$exception","uuid":"` + uuid + `","distinct_id":"browser","properties":{"$exception_list":[{"type":"Error","value":"failed token=private","stacktrace":{"frames":[{"filename":"` + asset + `?access_token=private","lineno":1,"colno":1,"function":"n"` + extra + `}]}}]}`
	}
	native := strings.TrimSuffix(payload("native-exception", ""), "}") + `,"$exception_build_id":"build-a","$exception_release":"release-a"}}`
	chunk := payload("posthog-exception", `,"chunk_id":"posthog-chunk"`) + "}"
	body := `{"api_key":"` + public + `","batch":[` + native + `,` + chunk + `,{"event":"$exception","uuid":"malformed","distinct_id":"browser","properties":{"$exception_list":"bad"}},{"event":"ordinary","uuid":"ordinary","distinct_id":"browser","properties":{"kept":"yes"}}]}`
	request(t, s, "POST", "/batch/", "", body, 200)
	duckLakeJourneyDrain(t, s)
	rows := duckLakeJourneyQuery(t, s, a, reader, "SELECT id, uuid, properties FROM events ORDER BY id", nil)
	if len(rows) != 4 {
		t.Fatalf("malformed exception affected mixed batch: %v", rows)
	}
	byUUID := map[string]string{}
	for _, item := range rows {
		row := item.(map[string]any)
		properties := row["properties"].(string)
		if strings.Contains(properties, "private") {
			t.Fatalf("raw secret survived normalization: %s", properties)
		}
		byUUID[row["uuid"].(string)] = row["id"].(string)
	}
	path := func(project, uuid string) string {
		return "/api/v1/projects/" + project + "/errors/" + byUUID[uuid] + "/stack"
	}
	stack := func(uuid string) map[string]any { return request(t, s, "GET", path(a, uuid), reader, "", 200) }
	frame := func(response map[string]any) map[string]any {
		t.Helper()
		return response["exceptions"].([]any)[0].(map[string]any)["frames"].([]any)[0].(map[string]any)
	}
	if frame(stack("native-exception"))["original"] != nil {
		t.Fatal("map resolved before upload")
	}
	uploadTestSourceMap(t, s, a, writer, exceptionTestMap, map[string]string{
		"X-Source-Map-Build-ID": "build-a", "X-Source-Map-Asset-URL": asset, "X-Source-Map-Release": "release-a",
	}, 200)
	uploadTestSourceMap(t, s, b, adminToken, exceptionTestMap, map[string]string{"X-Source-Map-Chunk-ID": "posthog-chunk"}, 200)
	if frame(stack("posthog-exception"))["original"] != nil {
		t.Fatal("lookup used another project's map")
	}
	uploadTestSourceMap(t, s, a, writer, exceptionTestMap, map[string]string{"X-Source-Map-Chunk-ID": "posthog-chunk"}, 200)
	for _, uuid := range []string{"native-exception", "posthog-exception"} {
		resolved := frame(stack(uuid))
		original, ok := resolved["original"].(map[string]any)
		if !ok || original["filename"] != "src/app.ts" || original["function"] != "startApp" || original["lineno"] != float64(1) {
			t.Fatalf("symbolication failed for %s: %v", uuid, resolved)
		}
	}
	if stack("malformed")["degraded"] != true {
		t.Fatal("malformed stack omitted degraded status")
	}
	request(t, s, "GET", path(a, "ordinary"), reader, "", 404)
	request(t, s, "GET", path(b, "native-exception"), reader, "", 403)
	request(t, s, "GET", path(a, "native-exception"), writer, "", 403)
}

func TestSharedSourceMapMigrationAndIsolation(t *testing.T) {
	t.Parallel()
	control, dsn := sharedControlFixture(t)
	const prior = 28
	for _, step := range sharedControlSteps[:prior] {
		if _, err := control.Exec(step); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := control.Exec(`INSERT INTO metadata(key,value) VALUES('schema_version','28')`); err != nil {
		t.Fatal(err)
	}
	limits, _ := query.Normalize(nil)
	if err := initializeSharedControl(t.Context(), control, adminToken, limits, false); err != nil {
		t.Fatal(err)
	}
	otherControl, err := openSharedControl(t.Context(), SharedConfig{DatabaseURL: dsn})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { otherControl.Close() })
	first := &Service{control: control, shared: &sharedRuntime{}, sourceMapSlots: make(chan struct{}, 2)}
	other := &Service{control: otherControl, shared: &sharedRuntime{}}
	const a, b = "map-project", "other-project"
	if _, err := control.Exec(`INSERT INTO projects(id,tenant_id,name,created_at) VALUES($1,'default',$1,$3),($2,'default',$2,$3)`, a, b, now()); err != nil {
		t.Fatal(err)
	}
	_, uploader := key(t, first, []string{"sourcemaps:write"}, []string{a})
	headers := map[string]string{"X-Source-Map-Chunk-ID": "test-chunk"}
	uploadTestSourceMap(t, first, a, uploader, exceptionTestMap, headers, 200)
	uploadTestSourceMap(t, first, b, uploader, exceptionTestMap, headers, 403)
	source, err := other.readSourceMap(t.Context(), a, "chunk:test-chunk")
	if err != nil || source == nil {
		t.Fatalf("map not available on another worker: %v", err)
	}
	if source, err := other.readSourceMap(t.Context(), b, "chunk:test-chunk"); err != nil || source != nil {
		t.Fatalf("map crossed project boundaries: %v", err)
	}
	if err := other.beginControlProjectDeletion(t.Context(), a); err != nil {
		t.Fatal(err)
	}
	uploadTestSourceMap(t, first, a, adminToken, exceptionTestMap, headers, 404)
	if err := other.completeControlProjectDeletion(t.Context(), a); err != nil {
		t.Fatal(err)
	}
	if source, err := first.readSourceMap(t.Context(), a, "chunk:test-chunk"); err != nil || source != nil {
		t.Fatalf("deletion left a private map: %v", err)
	}
}
