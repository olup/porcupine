package porcupine

import (
	"encoding/json"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/errtrack"
	"github.com/olup/porcupine/internal/event"
)

func TestErrorIssueTriageDuckLake(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	s := duckLakeJourneyService(t)
	base := time.Date(2026, 11, 3, 0, 0, 0, 123456789, time.UTC)
	clock := base
	s.clock = func() time.Time { return clock }
	projectID, _ := project(t, s)
	otherProject, _ := project(t, s)
	_, reader := key(t, s, []string{"events:read"}, []string{projectID})
	_, manager := key(t, s, []string{"management:write"}, []string{projectID})
	_, otherReader := key(t, s, []string{"events:read"}, []string{otherProject})
	group := func(build string) errtrack.Properties {
		return errtrack.Normalize(map[string]any{"$exception_build_id": build, "$exception_list": []any{map[string]any{
			"type": "TypeError", "value": "Cannot fetch",
			"stacktrace": map[string]any{"frames": []any{map[string]any{"filename": "app.js", "lineno": 1}}},
		}}})
	}
	first, second, third := group("build-a"), group("build-b"), group("build-c")
	makeEvent := func(id int64, project string, props errtrack.Properties, occurred, received time.Time, imported bool) event.Event {
		return event.Event{ID: id, UUID: "issue-fixture-" + strconv.FormatInt(id, 10), ProjectID: project,
			DistinctID: "actor", Event: "$exception", Properties: json.RawMessage(mustErrorGroupJSON(t, props)),
			Timestamp: occurred.Format(time.RFC3339Nano), ReceivedAt: received.Format(time.RFC3339Nano), Imported: imported}
	}
	apply := func(events ...event.Event) {
		t.Helper()
		if err := s.lake.Apply(t.Context(), duckLakeJourneyProjections(t, events)); err != nil {
			t.Fatal(err)
		}
	}
	before := base.Add(-time.Minute)
	apply(makeEvent(1, projectID, first, before, before, false), makeEvent(2, projectID, second, before, before, false),
		makeEvent(3, projectID, third, before, before, false), makeEvent(1, otherProject, first, before, before, false))
	groupsURL := "/api/v1/projects/" + projectID + "/error-groups"
	getGroup := func(fingerprint string) map[string]any {
		t.Helper()
		result := request(t, s, "GET", groupsURL+"/"+fingerprint, reader, "", 200)
		return result["group"].(map[string]any)
	}
	body := func(action string, revision int, target string) string {
		t.Helper()
		input := map[string]any{"action": action, "expected_revision": revision}
		if target != "" {
			input["issue_id"] = target
		}
		return mustErrorGroupJSON(t, input)
	}
	change := func(fingerprint, action string, revision int, target string, status int) map[string]any {
		t.Helper()
		return request(t, s, "POST", groupsURL+"/"+fingerprint+"/triage", manager, body(action, revision, target), status)
	}
	issue := func(output map[string]any) map[string]any {
		t.Helper()
		result, ok := output["issue"].(map[string]any)
		if !ok {
			t.Fatalf("missing issue in %v", output)
		}
		return result
	}
	assertState := func(value map[string]any, status string, muted bool, revision int) {
		t.Helper()
		if value["status"] != status || value["muted"] != muted || value["revision"] != float64(revision) {
			t.Fatalf("issue = %v, want status %s, muted %v, revision %d", value, status, muted, revision)
		}
	}

	assertState(issue(getGroup(first.Fingerprint)), "open", false, 0)
	request(t, s, "POST", groupsURL+"/"+first.Fingerprint+"/triage", reader, body("acknowledge", 0, ""), 403)
	change(strings.Repeat("c", 64), "mute", 0, "", 404)
	acknowledged := issue(change(first.Fingerprint, "acknowledge", 0, "", 200))
	assertState(acknowledged, "acknowledged", false, 1)
	id := acknowledged["id"].(string)
	issueURL := "/api/v1/projects/" + projectID + "/error-issues/" + id
	request(t, s, "GET", issueURL, manager, "", 403)
	request(t, s, "GET", "/api/v1/projects/"+otherProject+"/error-issues/"+id, otherReader, "", 404)
	change(first.Fingerprint, "mute", 0, "", 409)
	assertState(issue(change(second.Fingerprint, "link", 1, id, 200)), "acknowledged", false, 2)
	assertState(issue(change(first.Fingerprint, "mute", 2, "", 200)), "acknowledged", true, 3)
	only := request(t, s, "GET", groupsURL+"?muted=only&limit=1", reader, "", 200)
	visible := request(t, s, "GET", groupsURL+"?muted=exclude&limit=1", reader, "", 200)
	if len(only["groups"].([]any)) != 1 || only["next_offset"] != float64(1) ||
		len(visible["groups"].([]any)) != 1 || visible["next_offset"] != nil ||
		visible["groups"].([]any)[0].(map[string]any)["fingerprint"] != third.Fingerprint {
		t.Fatalf("mute filters did not apply before pagination: only=%v visible=%v", only, visible)
	}
	assertState(issue(change(second.Fingerprint, "resolve", 3, "", 200)), "resolved", true, 4)
	after := base.Add(time.Nanosecond)
	clock = base.Add(time.Second)
	apply(makeEvent(4, projectID, first, before, after, false),
		makeEvent(5, projectID, second, after, before, false),
		makeEvent(6, projectID, second, after, after, true),
		makeEvent(7, projectID, third, after, after, false),
		makeEvent(2, otherProject, first, after, after, false))
	assertState(issue(request(t, s, "GET", issueURL, reader, "", 200)), "resolved", true, 4)

	apply(makeEvent(9007199254740993, projectID, second, after, after, false))
	from, to := before.Add(-time.Minute).Format(time.RFC3339Nano), base.Add(-time.Second).Format(time.RFC3339Nano)
	old := request(t, s, "GET", groupsURL+"/"+first.Fingerprint+"?from="+from+"&to="+to, reader, "", 200)["group"].(map[string]any)
	assertState(issue(old), "seen_again", true, 5)
	if old["count"] != "2" || issue(old)["seen_again_event_id"] != "9007199254740993" || issue(old)["seen_again_at"] != after.Format(time.RFC3339Nano) {
		t.Fatalf("pinned group lost count or recurrence evidence: %v", old)
	}
	assertState(issue(request(t, s, "GET", issueURL, reader, "", 200)), "seen_again", true, 5)
	change(second.Fingerprint, "unmute", 4, "", 409)
	assertState(issue(change(second.Fingerprint, "unmute", 5, "", 200)), "seen_again", false, 6)
	assertState(issue(change(second.Fingerprint, "unlink", 6, "", 200)), "seen_again", false, 7)
	assertState(issue(getGroup(second.Fingerprint)), "open", false, 0)
	change(first.Fingerprint, "unlink", 7, "", 409)

	statuses := make(chan int, 2)
	for _, action := range []string{"reopen", "acknowledge"} {
		payload := body(action, 7, "")
		go func() {
			r := httptest.NewRequest("POST", groupsURL+"/"+first.Fingerprint+"/triage", strings.NewReader(payload))
			r.Header.Set("Authorization", "Bearer "+manager)
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, r)
			statuses <- w.Code
		}()
	}
	code, otherCode := <-statuses, <-statuses
	if !(code == 200 && otherCode == 409 || code == 409 && otherCode == 200) {
		t.Fatalf("concurrent action status codes %d and %d", code, otherCode)
	}
	var revision int
	if err := s.control.QueryRow(`SELECT revision FROM error_issues WHERE project_id=$1 AND id=$2`, projectID, id).Scan(&revision); err != nil || revision != 8 {
		t.Fatalf("persisted revision = %d, error = %v", revision, err)
	}

	otherGroupsURL := "/api/v1/projects/" + otherProject + "/error-groups"
	otherIssue := issue(request(t, s, "POST", otherGroupsURL+"/"+first.Fingerprint+"/triage", adminToken, body("mute", 0, ""), 200))
	if otherIssue["id"] == id {
		t.Fatalf("the same fingerprint in another project inherited issue %s", id)
	}
	request(t, s, "DELETE", "/api/v1/projects/"+projectID, adminToken, "", 204)
	deletion := `SELECT (SELECT count(*) FROM error_issues WHERE project_id=$1)+(SELECT count(*) FROM error_issue_fingerprints WHERE project_id=$1)`
	var left int
	if err := s.control.QueryRow(deletion, projectID).Scan(&left); err != nil || left != 0 {
		t.Fatalf("deleted project retains %d issue records: %v", left, err)
	}
	if err := s.control.QueryRow(deletion, otherProject).Scan(&left); err != nil || left != 2 {
		t.Fatalf("other project has %d issue records, want an issue and link: %v", left, err)
	}
}
