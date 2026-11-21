package porcupine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/acceptance"
	eventapi "github.com/olup/porcupine/internal/event"
	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/output"
)

func TestDuckLakeS3SharedPostgresHostedQueryDeleteRecovery(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	ctx := t.Context()
	fixture := nativeS3Fixture(t)
	dsn := sharedPostgresDSN(t)
	writer, writerHTTP, closeWriter := hostedCleanupWorker(t, hostedJourneyConfig(t, dsn, fixture))
	reader, readerHTTP, closeReader := hostedCleanupWorker(t, hostedJourneyConfig(t, dsn, fixture))
	project, _ := sharedJourneyProject(t, writerHTTP.URL, defaultTenantID, "query deletion")
	otherTenant := sharedJourneyHTTP(t, readerHTTP.URL, http.MethodPost, "/api/v1/tenants", adminToken,
		[]byte(`{"name":"Other query deletion tenant"}`), nil, http.StatusCreated)["tenant"].(map[string]any)["id"].(string)
	other, _ := sharedJourneyProject(t, readerHTTP.URL, otherTenant, "isolated query deletion")
	_, token := sharedJourneyKey(t, writerHTTP.URL, project, []string{"events:read", "events:delete"})
	_, readOnly := sharedJourneyKey(t, writerHTTP.URL, project, []string{"events:read"})
	_, otherToken := sharedJourneyKey(t, readerHTTP.URL, other, []string{"events:read", "events:delete"})

	from := time.Date(2026, 11, 17, 1, 0, 0, 0, time.UTC)
	to := from.Add(time.Hour)
	targets := []string{eventapi.NewID(), eventapi.NewID()}
	kept := []string{eventapi.NewID(), eventapi.NewID()}
	event := func(uuid, name string, at time.Time) acceptance.BufferedEvent {
		received := time.Now().UTC()
		return acceptance.BufferedEvent{Input: acceptance.Input{Event: eventapi.Event{
			UUID: uuid, Event: name, DistinctID: "actor", Properties: json.RawMessage(`{"cohort":"red"}`), Context: json.RawMessage(`{}`),
			Timestamp: at.Format(time.RFC3339Nano), ReceivedAt: received.Format(time.RFC3339Nano),
		}}, AdmittedAt: received}
	}
	flush := func(s *Service, project string, events ...acceptance.BufferedEvent) {
		t.Helper()
		for i := range events {
			events[i].Input.Event.ProjectID = project
		}
		if err := s.hosted.flusher.FlushLive(ctx, acceptance.FlushBatch{ID: eventapi.NewID(), ProjectID: project, Events: events}); err != nil {
			t.Fatal("flush hosted events", err)
		}
	}
	flush(writer, project, event(targets[0], "erase", from), event(targets[1], "erase", from.Add(time.Minute)),
		event(kept[0], "erase", to), event(kept[1], "keep", from))
	flush(reader, other, event(targets[0], "erase", from))

	base := "/api/v1/projects/" + project
	otherBase := "/api/v1/projects/" + other
	deletions := base + "/event-deletions"
	request := func(key, name string, dryRun bool, expected *int) []byte {
		t.Helper()
		body := map[string]any{
			"where":           "event = $name AND json_extract_string(properties, '$.cohort') = $cohort",
			"parameters":      map[string]string{"name": name, "cohort": "red"},
			"time_range":      map[string]time.Time{"from": from, "to": to},
			"idempotency_key": key,
			"dry_run":         dryRun,
		}
		if expected != nil {
			body["expected_count"] = *expected
		}
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	assertCount := func(origin, project, token, want string) {
		t.Helper()
		result := sharedJourneyQuery(t, origin, project, token, "SELECT count(*) AS total FROM events", http.StatusOK)
		rows := result["result"].(map[string]any)["data"].([]any)
		if len(rows) != 1 || rows[0].(map[string]any)["total"] != want {
			t.Fatalf("project %s event count = %v, want %s", project, rows, want)
		}
	}
	assertState := func(response map[string]any, state string, applied float64) {
		t.Helper()
		if response["project_id"] != project || response["state"] != state || response["matched_count"] != float64(len(targets)) ||
			response["applied_count"] != applied || response["failed_count"] != float64(0) {
			t.Fatalf("query deletion state = %v, want %s with %g applied", response, state, applied)
		}
	}

	previewBody := request("confirmed", "erase", true, nil)
	sharedJourneyHTTP(t, writerHTTP.URL, http.MethodPost, deletions, readOnly, previewBody, nil, http.StatusForbidden)
	preview := sharedJourneyHTTP(t, writerHTTP.URL, http.MethodPost, deletions, token, previewBody, nil, http.StatusOK)
	assertState(preview, "preview", 0)
	if _, exists := preview["mutation_id"]; exists {
		t.Fatal("preview returned a committed mutation ID", preview)
	}
	samples, ok := preview["sample_uuids"].([]any)
	if !ok || len(samples) != len(targets) || !slices.Contains(samples, any(targets[0])) || !slices.Contains(samples, any(targets[1])) {
		t.Fatal("preview sample did not contain the two targeted events", preview)
	}
	wrongCount, expectedCount := 1, len(targets)
	sharedJourneyHTTP(t, readerHTTP.URL, http.MethodPost, deletions, token, request("wrong-count", "erase", false, &wrongCount), nil, http.StatusConflict)
	assertCount(readerHTTP.URL, project, token, "4")

	body := request("confirmed", "erase", false, &expectedCount)
	sharedJourneyHTTP(t, readerHTTP.URL, http.MethodPost, otherBase+"/event-deletions", token, body, nil, http.StatusForbidden)
	accepted := sharedJourneyHTTP(t, writerHTTP.URL, http.MethodPost, deletions, token, body, nil, http.StatusAccepted)
	assertState(accepted, "pending", 0)
	id, ok := accepted["mutation_id"].(string)
	if !ok || id == "" {
		t.Fatal("accepted deletion did not return a mutation ID", accepted)
	}
	statusPath := deletions + "/" + id
	pending := sharedJourneyHTTP(t, readerHTTP.URL, http.MethodGet, statusPath, readOnly, nil, nil, http.StatusOK)
	assertState(pending, "pending", 0)
	replay := sharedJourneyHTTP(t, readerHTTP.URL, http.MethodPost, deletions, token, body, nil, http.StatusAccepted)
	assertState(replay, "pending", 0)
	if pending["mutation_id"] != id || replay["mutation_id"] != id {
		t.Fatal("other worker returned a different mutation", pending, replay)
	}
	sharedJourneyHTTP(t, readerHTTP.URL, http.MethodPost, deletions, token, request("confirmed", "keep", false, &expectedCount), nil, http.StatusConflict)
	sharedJourneyHTTP(t, readerHTTP.URL, http.MethodGet, otherBase+"/event-deletions/"+id, otherToken, nil, nil, http.StatusNotFound)

	for _, uuid := range targets {
		sharedJourneyHTTP(t, readerHTTP.URL, http.MethodGet, base+"/events/"+uuid, token, nil, nil, http.StatusGone)
	}
	for _, uuid := range kept {
		sharedJourneyHTTP(t, readerHTTP.URL, http.MethodGet, base+"/events/"+uuid, token, nil, nil, http.StatusOK)
	}
	sharedJourneyHTTP(t, readerHTTP.URL, http.MethodGet, otherBase+"/events/"+targets[0], otherToken, nil, nil, http.StatusOK)

	flusher := reader.hosted.flusher
	assertSenderGate := func(flusher *hostedLiveFlusher, project string, want bool) {
		t.Helper()
		err := flusher.withHostedMutationProject(ctx, project, func(ctx context.Context, session *lakeapi.ProjectWriteSession, scope lakeapi.HostedProjectScope, tenant string) error {
			return session.RunControlTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
				blocked, err := hostedOutputBlocked(ctx, tx, scope, tenant)
				if err == nil && blocked != want {
					t.Fatalf("sender gate for project %s = %v, want %v", project, blocked, want)
				}
				return err
			})
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	assertSenderGate(flusher, project, true)
	assertSenderGate(flusher, other, false)
	key := lakeapi.ProjectWriteKey{SourceID: flusher.native.Config.SharedSourceID, ProjectID: project}
	scope, err := flusher.native.LookupHostedProjectExisting(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	before, err := flusher.native.PendingNativeEffects(ctx, scope, 0, 10)
	if err != nil || before.NativeHighWater != 1 {
		t.Fatal("preview and acceptance should leave only the original native batch", before, err)
	}
	for _, uuid := range targets {
		projection, err := flusher.native.LoadHostedEvent(ctx, scope, uuid)
		if err != nil || projection.IsDeleted || projection.Revision != 0 {
			t.Fatal("accepted tombstone reached native rows before recovery", uuid, projection, err)
		}
	}

	publicationFailed := errors.New("injected webhook cancellation failure")
	flusher.publisher.cancelMutationWebhooks = func(_ context.Context, _ *sql.Tx, got output.PipelineScope, tenant string, uuids []string) error {
		if got != pipelineScope(scope) || tenant != defaultTenantID || len(uuids) != len(targets) ||
			!slices.Contains(uuids, targets[0]) || !slices.Contains(uuids, targets[1]) {
			t.Errorf("webhook cancellation got scope %v, tenant %s, UUIDs %v", got, tenant, uuids)
		}
		return publicationFailed
	}
	if err := flusher.withHostedMutationCut(ctx, project, func(context.Context, *lakeapi.ProjectWriteSession, lakeapi.HostedProjectScope, string, int64) error {
		t.Fatal("snapshot cut bypassed failed deletion publication")
		return nil
	}); !errors.Is(err, publicationFailed) {
		t.Fatal("did not inject publication failure after the native delete", err)
	}
	interrupted, err := flusher.native.PendingNativeEffects(ctx, scope, before.NativeHighWater, 10)
	if err != nil || interrupted.NativeHighWater != before.NativeHighWater+1 || interrupted.AcknowledgedThrough != before.NativeHighWater ||
		len(interrupted.Evidence) != 1 || interrupted.Evidence[0].OperationID != id {
		t.Fatal("expected one committed native deletion awaiting publication", interrupted, err)
	}
	assertSenderGate(flusher, project, true)
	assertState(sharedJourneyHTTP(t, readerHTTP.URL, http.MethodGet, statusPath, token, nil, nil, http.StatusOK), "pending", 0)

	closeWriter()
	closeReader()
	restarted, restartedHTTP, _ := hostedCleanupWorker(t, hostedJourneyConfig(t, dsn, fixture))
	recovery := restarted.hosted.flusher
	if err := recovery.withHostedMutationCut(ctx, project, func(ctx context.Context, _ *lakeapi.ProjectWriteSession, cutScope lakeapi.HostedProjectScope, _ string, cut int64) error {
		if cutScope != scope || cut != 1 {
			t.Fatal("snapshot cut omitted the Ready deletion", cutScope, cut)
		}
		for _, uuid := range targets {
			projection, err := recovery.native.LoadHostedEvent(ctx, scope, uuid)
			if err != nil || !projection.IsDeleted || projection.Revision == 0 {
				t.Fatal("snapshot cut preceded a native tombstone", uuid, projection, err)
			}
		}
		return nil
	}); err != nil {
		t.Fatal("settle deletion on a cold worker", err)
	}
	assertSenderGate(recovery, project, false)
	applied := sharedJourneyHTTP(t, restartedHTTP.URL, http.MethodGet, statusPath, token, nil, nil, http.StatusOK)
	assertState(applied, "applied", float64(len(targets)))
	if applied["mutation_id"] != id {
		t.Fatal("recovery returned a different mutation", applied)
	}
	assertCount(restartedHTTP.URL, project, token, "2")
	assertCount(restartedHTTP.URL, other, otherToken, "1")
	replay = sharedJourneyHTTP(t, restartedHTTP.URL, http.MethodPost, deletions, token, body, nil, http.StatusAccepted)
	assertState(replay, "applied", float64(len(targets)))
	if replay["mutation_id"] != id {
		t.Fatal("terminal replay returned a different mutation", replay)
	}
	after, err := recovery.native.PendingNativeEffects(ctx, scope, before.NativeHighWater, 10)
	if err != nil || after.NativeHighWater != interrupted.NativeHighWater || after.AcknowledgedThrough != after.NativeHighWater ||
		len(after.Evidence) != 1 || after.Evidence[0].ProofFingerprint != interrupted.Evidence[0].ProofFingerprint {
		t.Fatal("batch deletion did not publish exactly one native outcome", after, err)
	}

	emptyProject, _ := sharedJourneyProject(t, restartedHTTP.URL, defaultTenantID, "query deletion without ingestion")
	_, emptyToken := sharedJourneyKey(t, restartedHTTP.URL, emptyProject, []string{"events:read", "events:delete"})
	emptyPath := "/api/v1/projects/" + emptyProject + "/event-deletions"
	zero := 0
	empty := sharedJourneyHTTP(t, restartedHTTP.URL, http.MethodPost, emptyPath, emptyToken, request("empty", "erase", false, &zero), nil, http.StatusAccepted)
	emptyID, ok := empty["mutation_id"].(string)
	if !ok || emptyID == "" || empty["state"] != "applied" {
		t.Fatal("empty confirmation did not return an applied mutation", empty)
	}
	emptyStatus := sharedJourneyHTTP(t, restartedHTTP.URL, http.MethodGet, emptyPath+"/"+emptyID, emptyToken, nil, nil, http.StatusOK)
	if emptyStatus["project_id"] != emptyProject || emptyStatus["mutation_id"] != emptyID || emptyStatus["state"] != "applied" ||
		emptyStatus["matched_count"] != float64(0) || emptyStatus["applied_count"] != float64(0) || emptyStatus["failed_count"] != float64(0) {
		t.Fatal("empty deletion did not persist its applied result", emptyStatus)
	}
}
