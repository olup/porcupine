package porcupine

import (
	"encoding/json"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestErrorGroupAgentJourney(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	s := duckLakeJourneyService(t)
	projectID, captureKey := project(t, s)
	other, _ := project(t, s)
	_, reader := key(t, s, []string{"events:read"}, []string{projectID})
	_, editor := key(t, s, []string{"events:edit", "events:delete"}, []string{projectID})
	const (
		from = "2026-11-01T12:00:00Z"
		at   = "2026-11-01T13:00:00Z"
		to   = "2026-11-01T15:00:00Z"
	)
	initial := `{"event":"$exception","api_key":"` + captureKey + `","uuid":"error-to-correct","distinct_id":"visitor","timestamp":"` + at + `","properties":{"$exception_list":[{"type":"Error","value":"Timeout"}]}}`
	request(t, s, "POST", "/capture/", "", initial, 200)
	duckLakeJourneyDrain(t, s)

	query := url.Values{"from": {from}, "to": {to}}
	listPath := "/api/v1/projects/" + projectID + "/error-groups"
	list := request(t, s, "GET", listPath+"?"+query.Encode(), reader, "", 200)
	groups := list["groups"].([]any)
	if len(groups) != 1 {
		t.Fatalf("unexpected groups: %v", groups)
	}
	initialGroup := groups[0].(map[string]any)["fingerprint"].(string)

	forged := strings.Repeat("a", 64)
	edit := `{"expected_revision":"0","idempotency_key":"correct-error","snapshot":{"event":"$exception","distinct_id":"visitor","timestamp":"` + at + `","properties":{"$exception_fingerprint":"` + forged + `","$exception_list":[{"type":"Error","value":"Connection closed"}],"component":"checkout"}}}`
	eventPath := "/api/v1/projects/" + projectID + "/events/error-to-correct"
	request(t, s, "PATCH", eventPath, editor, edit, 202)
	duckLakeJourneyDrain(t, s)
	request(t, s, "GET", listPath+"/"+initialGroup+"?"+query.Encode(), reader, "", 404)
	request(t, s, "GET", listPath+"/"+forged+"?"+query.Encode(), reader, "", 404)

	host := httptest.NewServer(s.Handler())
	t.Cleanup(host.Close)
	ctx, session := mcpTestSession(t, host.URL, reader, "")
	queryParams := map[string]string{"from": from, "to": to}
	read := mcpTestCall(t, ctx, session, mcpCallInput{
		OperationID: "list-error-groups", Path: map[string]string{"id": projectID}, Query: queryParams,
	}, 200)
	group := read["groups"].([]any)[0].(map[string]any)
	fingerprint := group["fingerprint"].(string)
	if fingerprint == initialGroup || fingerprint == forged || group["message"] != "Connection closed" || group["count"] != "1" {
		t.Fatalf("correction not reflected in the group: %v", group)
	}
	pathParams := map[string]string{"id": projectID, "fingerprint": fingerprint}
	detail := mcpTestCall(t, ctx, session, mcpCallInput{
		OperationID: "get-error-group", Path: pathParams, Query: queryParams,
	}, 200)
	buckets := detail["distribution"].([]any)
	if len(buckets) != 3 || buckets[0].(map[string]any)["count"] != "0" || buckets[1].(map[string]any)["count"] != "1" {
		t.Fatalf("MCP lost stable distribution: %v", buckets)
	}
	eventID := detail["occurrences"].([]any)[0].(map[string]any)["event_id"].(string)
	stack := mcpTestCall(t, ctx, session, mcpCallInput{
		OperationID: "get-error-stack", Path: map[string]string{"id": projectID, "event_id": eventID},
	}, 200)
	if stack["fingerprint"] != fingerprint {
		t.Fatalf("stack fingerprint %v disagrees with group %q", stack["fingerprint"], fingerprint)
	}
	mcpTestCall(t, ctx, session, mcpCallInput{
		OperationID: "get-error-group", Path: map[string]string{"id": other, "fingerprint": fingerprint}, Query: queryParams,
	}, 403)
	results := duckLakeJourneyQuery(t, s, projectID, reader, "SELECT json_extract_string(properties, '$.component') AS component FROM events", nil)
	if results[0].(map[string]any)["component"] != "checkout" {
		t.Fatal("correction lost an unrelated property")
	}

	binary := filepath.Join(t.TempDir(), "porcupine")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", binary, "./cmd/porcupine")
	build.Dir = "../../cli"
	build.Env = append(os.Environ(), "CGO_ENABLED=0")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build CLI: %s: %v", output, err)
	}
	command := exec.CommandContext(t.Context(), binary, "--url", host.URL, "get-error-group", "--id", projectID, "--fingerprint", fingerprint, "--from", from, "--to", to)
	command.Env = append(os.Environ(), "PORCUPINE_API_KEY="+reader)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("CLI returned %s: %v", output, err)
	}
	var viaCLI map[string]any
	if err := json.Unmarshal(output, &viaCLI); err != nil || !reflect.DeepEqual(viaCLI, detail) {
		t.Fatalf("CLI and MCP disagree: %s: %v", output, err)
	}

	request(t, s, "DELETE", eventPath, editor, `{"expected_revision":"1","idempotency_key":"delete-corrected-error"}`, 202)
	duckLakeJourneyDrain(t, s)
	request(t, s, "GET", listPath+"/"+fingerprint+"?"+query.Encode(), reader, "", 404)
}
