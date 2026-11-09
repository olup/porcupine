package porcupine

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

func identityCapture(t *testing.T, s *Service, token string, events []map[string]any, status int) map[string]any {
	t.Helper()
	payload, err := json.Marshal(map[string]any{"api_key": token, "batch": events})
	if err != nil {
		t.Fatal(err)
	}
	return request(t, s, "POST", "/batch/", "", string(payload), status)
}

func identifyEvent(uuid, target, anonymous string, set, once map[string]any) map[string]any {
	properties := map[string]any{}
	if anonymous != "" {
		properties["$anon_distinct_id"] = anonymous
	}
	return map[string]any{"uuid": uuid, "event": "$identify", "distinct_id": target, "properties": properties, "$set": set, "$set_once": once}
}

func setIdentityEvent(uuid, target string, set, once map[string]any) map[string]any {
	return map[string]any{"uuid": uuid, "event": "$set", "distinct_id": target, "properties": map[string]any{"$set": set, "$set_once": once}}
}

func storedIdentityProfile(t *testing.T, s *Service, project, target string) (map[string]any, bool, int64) {
	t.Helper()
	var raw string
	var identified bool
	var version int64
	if err := s.db.QueryRow(`SELECT traits,is_identified,version FROM identity_profiles WHERE project_id=? AND identified_id=?`, project, target).Scan(&raw, &identified, &version); err != nil {
		t.Fatal(err)
	}
	var traits map[string]any
	if err := json.Unmarshal([]byte(raw), &traits); err != nil {
		t.Fatal(err)
	}
	return traits, identified, version
}

func storedIdentitySnapshot(t *testing.T, s *Service, uuid string) identitySnapshot {
	t.Helper()
	var raw string
	if err := s.db.QueryRow(`SELECT identity_snapshot FROM events WHERE uuid=?`, uuid).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var snapshot identitySnapshot
	if err := json.Unmarshal([]byte(raw), &snapshot); err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func TestIdentitySDKRootTraitsLinkingSetOnceAndRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "identity.db")
	s, err := Open(Config{DBPath: path, BootstrapToken: adminToken})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { s.Close() }()
	projectID, token := project(t, s)
	control := identifyEvent("identify", "account-a", "browser-anon", map[string]any{"plan": "pro", "nested": map[string]any{"locale": "fr"}}, map[string]any{"first": "initial", "nullable": nil})
	result := identityCapture(t, s, token, []map[string]any{
		{"uuid": "before", "event": "purchase", "distinct_id": "browser-anon"},
		control,
		{"uuid": "after", "event": "purchase", "distinct_id": "account-a"},
	}, 200)
	if result["accepted"] != float64(3) || result["identity_ignored"] != float64(0) {
		t.Fatal("valid SDK batch was not accepted", result)
	}
	traits, identified, version := storedIdentityProfile(t, s, projectID, "account-a")
	if !identified || traits["plan"] != "pro" || traits["first"] != "initial" || traits["nested"].(map[string]any)["locale"] != "fr" {
		t.Fatal("SDK root traits were not applied", traits)
	}
	var target string
	var linkVersion int64
	if err := s.db.QueryRow(`SELECT identified_id,version FROM identity_links WHERE project_id=? AND anonymous_id='browser-anon'`, projectID).Scan(&target, &linkVersion); err != nil || target != "account-a" || linkVersion != version {
		t.Fatal("anonymous link was not durable and versioned", err)
	}
	snapshot := storedIdentitySnapshot(t, s, "identify")
	if snapshot.IdentifiedID != "account-a" || len(snapshot.Links) != 1 || snapshot.Links[0].AnonymousID != "browser-anon" || snapshot.Version != fmt.Sprint(version) {
		t.Fatal("wrong canonical snapshot", snapshot)
	}
	for uuid, distinct := range map[string]string{"before": "browser-anon", "after": "account-a"} {
		if event := storedCookieless(t, s, uuid); event.DistinctID != distinct {
			t.Fatal("ordinary event original distinct_id was rewritten")
		}
	}
	changedRetry := identifyEvent("identify", "account-a", "browser-anon", map[string]any{"plan": "must-not-apply"}, map[string]any{})
	result = identityCapture(t, s, token, []map[string]any{changedRetry}, 200)
	if result["duplicates"] != float64(1) || result["accepted"] != float64(0) {
		t.Fatal("stable UUID retry reapplied identity", result)
	}
	identityCapture(t, s, token, []map[string]any{setIdentityEvent("set", "account-a", map[string]any{"plan": "enterprise"}, map[string]any{"first": "replacement", "nullable": "replacement", "new": "once"})}, 200)
	traits, identified, currentVersion := storedIdentityProfile(t, s, projectID, "account-a")
	if !identified || currentVersion <= version || traits["plan"] != "enterprise" || traits["first"] != "initial" || traits["nullable"] != nil || traits["new"] != "once" {
		t.Fatal("standalone set/set_once semantics changed", traits)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(Config{DBPath: path})
	if err != nil {
		t.Fatal(err)
	}
	traits, identified, restartedVersion := storedIdentityProfile(t, s, projectID, "account-a")
	if !identified || restartedVersion != currentVersion || traits["plan"] != "enterprise" {
		t.Fatal("restart lost canonical profile")
	}
	result = identityCapture(t, s, token, []map[string]any{changedRetry}, 200)
	if result["duplicates"] != float64(1) {
		t.Fatal("restart lost identity idempotence")
	}
}

func TestIdentityNoAnonymousTargetProfilesProjectScopeAndNoFlips(t *testing.T) {
	s := service(t, 100)
	projectA, tokenA := project(t, s)
	projectB, tokenB := project(t, s)
	identityCapture(t, s, tokenA, []map[string]any{identifyEvent("a", "user-a", "same-anon", map[string]any{"plan": "a"}, map[string]any{})}, 200)
	identityCapture(t, s, tokenB, []map[string]any{identifyEvent("a", "user-b", "same-anon", map[string]any{"plan": "b"}, map[string]any{})}, 200)
	for project, user := range map[string]string{projectA: "user-a", projectB: "user-b"} {
		traits, identified, _ := storedIdentityProfile(t, s, project, user)
		if !identified || len(traits) != 1 {
			t.Fatal("project identity boundary failed")
		}
	}
	// Node identify has no anonymous ID and still marks its target identified.
	identityCapture(t, s, tokenA, []map[string]any{identifyEvent("node", "backend-user", "", map[string]any{"role": "admin"}, map[string]any{})}, 200)
	_, identified, _ := storedIdentityProfile(t, s, projectA, "backend-user")
	if !identified {
		t.Fatal("target-only identify did not mark identified user")
	}
	controls := []map[string]any{
		identifyEvent("flip", "other-user", "same-anon", map[string]any{"poison": true}, map[string]any{}),
		identifyEvent("merge-users", "user-a", "backend-user", map[string]any{"poison": true}, map[string]any{}),
		identifyEvent("chain", "same-anon", "fresh-anon", map[string]any{"poison": true}, map[string]any{}),
	}
	controls = append(controls, map[string]any{"uuid": "ordinary", "event": "purchase", "distinct_id": "user-a"})
	result := identityCapture(t, s, tokenA, controls, 200)
	if result["accepted"] != float64(4) || result["identity_ignored"] != float64(3) {
		t.Fatal("conflict poisoned unrelated ordinary event", result)
	}
	traits, _, _ := storedIdentityProfile(t, s, projectA, "user-a")
	if traits["poison"] != nil || traits["plan"] != "a" {
		t.Fatal("ignored conflicting control overwrote valid profile")
	}
	for _, uuid := range []string{"flip", "merge-users", "chain"} {
		snapshot := storedIdentitySnapshot(t, s, uuid)
		if snapshot.IdentifiedID != "" || len(snapshot.Traits) != 0 || len(snapshot.Links) != 0 {
			t.Fatal("ignored conflict projected an invalid canonical link/profile")
		}
	}
	var profiles int
	s.db.QueryRow(`SELECT count(*) FROM identity_profiles WHERE project_id=?`, projectA).Scan(&profiles)
	if profiles != 2 {
		t.Fatal("ignored control created a competing identified profile")
	}
}

func TestIdentityAnonymousTraitsMergeWithoutDemotingIdentifiedTargets(t *testing.T) {
	s := service(t, 100)
	projectID, token := project(t, s)
	identityCapture(t, s, token, []map[string]any{setIdentityEvent("prelogin", "anon", map[string]any{"source": "anonymous", "shared": "anon"}, map[string]any{})}, 200)
	_, identified, _ := storedIdentityProfile(t, s, projectID, "anon")
	if identified {
		t.Fatal("setting anonymous traits falsely asserted identification")
	}
	identityCapture(t, s, token, []map[string]any{identifyEvent("target", "account", "", map[string]any{"shared": "account"}, map[string]any{})}, 200)
	identityCapture(t, s, token, []map[string]any{identifyEvent("link", "account", "anon", map[string]any{}, map[string]any{"once": "new"})}, 200)
	identityCapture(t, s, token, []map[string]any{setIdentityEvent("alias-set", "anon", map[string]any{"updated": true}, map[string]any{})}, 200)
	traits, identified, _ := storedIdentityProfile(t, s, projectID, "account")
	if !identified || traits["source"] != "anonymous" || traits["shared"] != "account" || traits["once"] != "new" || traits["updated"] != true {
		t.Fatal("anonymous trait merge or alias update was incorrect", traits)
	}
	result := identityCapture(t, s, token, []map[string]any{identifyEvent("cannot-demote", "next", "account", map[string]any{}, map[string]any{})}, 200)
	if result["identity_ignored"] != float64(1) {
		t.Fatal("standalone set demoted an identified target")
	}
}

func TestIdentityCookielessAndMalformedControlsAreIsolatedNoops(t *testing.T) {
	s := service(t, 100)
	projectID, token := project(t, s)
	controls := []map[string]any{
		identifyEvent("cookieless-target", "cookieless_deadbeef", "anon", map[string]any{}, map[string]any{}),
		identifyEvent("cookieless-source", "account", cookielessSentinel, map[string]any{}, map[string]any{}),
		identifyEvent("derived-source", "account", "cookieless_deadbeef", map[string]any{}, map[string]any{}),
		identifyEvent("empty-target", "", "anon", map[string]any{}, map[string]any{}),
		{"uuid": "bad-anon", "event": "$identify", "distinct_id": "account", "properties": map[string]any{"$anon_distinct_id": 42}},
		{"uuid": "bad-traits", "event": "$identify", "distinct_id": "account", "$set": []any{"invalid"}},
		{"uuid": "null-traits", "event": "$set", "distinct_id": "account", "properties": map[string]any{"$set": nil}},
		{"uuid": "ordinary-valid", "event": "purchase", "distinct_id": "visitor"},
	}
	result := identityCapture(t, s, token, controls, 200)
	if result["identity_ignored"] != float64(7) || result["accepted"] != float64(8) {
		t.Fatal("malformed semantic controls poisoned valid mixed batch", result)
	}
	var count int
	s.db.QueryRow(`SELECT count(*) FROM identity_profiles WHERE project_id=?`, projectID).Scan(&count)
	if count != 0 {
		t.Fatal("cookieless or invalid input created a profile")
	}
	result = identityCapture(t, s, token, []map[string]any{identifyEvent("daily-sentinel", cookielessSentinel, "anon", map[string]any{}, map[string]any{})}, 200)
	if result["identity_ignored"] != float64(1) {
		t.Fatal("shared sentinel linked daily identity")
	}
}

func TestIdentityTraitAndQueueLimitsRollbackCanonicalState(t *testing.T) {
	s := service(t, 100)
	projectID, token := project(t, s)
	identityCapture(t, s, token, []map[string]any{identifyEvent("initial", "account", "anon", map[string]any{"large": strings.Repeat("x", 30000)}, map[string]any{})}, 200)
	_, _, version := storedIdentityProfile(t, s, projectID, "account")
	// Simulate the normal delivery removal while retaining canonical state.
	if _, err := s.db.Exec(`DELETE FROM events`); err != nil {
		t.Fatal(err)
	}
	s.queueMaxBytes = 1000
	identityCapture(t, s, token, []map[string]any{setIdentityEvent("hidden-byte-budget", "account", map[string]any{"small": true}, map[string]any{})}, 503)
	traits, _, after := storedIdentityProfile(t, s, projectID, "account")
	if after != version || traits["small"] != nil || eventCount(t, s) != 0 {
		t.Fatal("private snapshot queue rejection partially committed identity")
	}
	var pendingBytes int64
	s.db.QueryRow(`SELECT pending_bytes FROM queue_usage`).Scan(&pendingBytes)
	if pendingBytes != 0 {
		t.Fatal("failed private snapshot byte accounting was not rolled back")
	}
	s.queueMaxBytes = DefaultQueueMaxBytes
	result := identityCapture(t, s, token, []map[string]any{setIdentityEvent("over-profile", "account", map[string]any{"extra": strings.Repeat("x", 3000)}, map[string]any{})}, 200)
	if result["identity_ignored"] != float64(1) {
		t.Fatal("oversized resulting profile was accepted")
	}
	traits, _, after = storedIdentityProfile(t, s, projectID, "account")
	if after != version || traits["extra"] != nil {
		t.Fatal("oversized traits changed prior profile")
	}
	many := map[string]any{}
	for i := range identityMaxTraitKeys + 1 {
		many[fmt.Sprint(i)] = true
	}
	result = identityCapture(t, s, token, []map[string]any{setIdentityEvent("too-many", "other", many, map[string]any{})}, 200)
	if result["identity_ignored"] != float64(1) {
		t.Fatal("excessive profile key count was accepted")
	}
}

func TestIdentityControlsPrivateImmutableAndNotWebhookEvents(t *testing.T) {
	s := service(t, 100)
	projectID, token := project(t, s)
	// Enable capture fanout without starting any destination worker.
	rt, err := newPipelineRuntime(&PipelineConfig{Key: make([]byte, 32)})
	if err != nil {
		t.Fatal(err)
	}
	s.pipelines = rt
	var actorID string
	s.db.QueryRow(`SELECT id FROM access_keys WHERE token_hash=?`, hash(adminToken)).Scan(&actorID)
	if _, err := s.db.Exec(`INSERT INTO pipelines(id,project_id,actor_id,kind,name,state,config,secret,created_at_ms) VALUES('all',?,?,'webhook','all','active','{"delivery_mode":"event"}',x'00',0)`, projectID, actorID); err != nil {
		t.Fatal(err)
	}
	identityCapture(t, s, token, []map[string]any{identifyEvent("private-control", "account", "anon", map[string]any{"email": "private@example.test"}, map[string]any{}), {"uuid": "ordinary", "event": "purchase", "distinct_id": "anon"}}, 200)
	var deliveries int
	s.db.QueryRow(`SELECT count(*) FROM webhook_deliveries`).Scan(&deliveries)
	if deliveries != 1 {
		t.Fatal("identity control was forwarded as an ordinary webhook event")
	}
	request(t, s, "GET", "/api/v1/projects/"+projectID+"/events/private-control", adminToken, "", 404)
	queued := request(t, s, "GET", "/api/v1/projects/"+projectID+"/events", adminToken, "", 200)
	if len(queued["events"].([]any)) != 1 {
		t.Fatal("queued event reader exposed private identity control")
	}
	s.ch = testClickHouse(t, func(http.ResponseWriter, *http.Request) {})
	ctx := mutationContext(actorID)
	_, err = s.editEvent(ctx, mutationEdit(projectID, "private-control", "edit-control", "0"))
	mutationErrorStatus(t, err, 409)
	_, err = s.deleteEvent(ctx, &EventDeleteInput{ID: projectID, UUID: "private-control", Body: EventDeleteBody{IdempotencyKey: "delete-control", ExpectedRevision: "0"}})
	mutationErrorStatus(t, err, 409)
	edit := mutationEdit(projectID, "ordinary", "rename-control", "0")
	edit.Body.Snapshot.Event = "$identify"
	_, err = s.editEvent(ctx, edit)
	mutationErrorStatus(t, err, 400)
	batch, err := s.deliveryBatch(context.Background())
	if err != nil || len(batch) != 2 || batch[0].IdentitySnapshot == "" || batch[1].IdentitySnapshot != "" {
		t.Fatal("durable delivery did not carry only the private control snapshot", err)
	}
	encoded, _ := json.Marshal(batch[0])
	if strings.Contains(string(encoded), "IdentitySnapshot") || strings.Contains(string(encoded), "identity_snapshot") {
		t.Fatal("private snapshot leaked from the Event JSON contract")
	}
}

func TestIdentityControlColdReceiptRetryCannotReapply(t *testing.T) {
	for _, limit := range []int64{0, 1} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			s, _ := finiteReceiptService(t, limit)
			projectID, token := project(t, s)
			identityCapture(t, s, token, []map[string]any{identifyEvent("control", "account", "anon", map[string]any{"plan": "first"}, map[string]any{})}, 200)
			_, _, version := storedIdentityProfile(t, s, projectID, "account")
			if err := s.flushDelivery(context.Background()); err != nil {
				t.Fatal(err)
			}
			deliverReceipt(t, s, token, "ordinary-later")
			var local int
			s.db.QueryRow(`SELECT count(*) FROM event_receipts WHERE uuid='control'`).Scan(&local)
			if limit == 1 && local != 0 || limit == 0 && local != 1 {
				t.Fatal("receipt retention fixture did not exercise the expected tier")
			}
			result := identityCapture(t, s, token, []map[string]any{identifyEvent("control", "account", "anon", map[string]any{"plan": "poison"}, map[string]any{})}, 200)
			traits, _, after := storedIdentityProfile(t, s, projectID, "account")
			if result["duplicates"] != float64(1) || after != version || traits["plan"] != "first" || eventCount(t, s) != 0 {
				t.Fatal("permanent or cold physical receipt replayed a hidden control")
			}
		})
	}
}

func TestIdentityQueueAccountingMigratesExistingDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	s, err := Open(Config{DBPath: path, BootstrapToken: adminToken})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { s.Close() }()
	_, token := project(t, s)
	request(t, s, "POST", "/capture/", "", captureBody(token, "legacy-ordinary"), 200)
	_, before := queueUsage(t, s)
	if _, err := s.db.Exec(`DROP TRIGGER queue_usage_insert; DROP TRIGGER queue_usage_update; DROP TRIGGER queue_usage_delete; ALTER TABLE events DROP COLUMN identity_snapshot; DELETE FROM metadata WHERE key='identity_queue_accounting';`); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(Config{DBPath: path})
	if err != nil {
		t.Fatal(err)
	}
	if count, size := queueUsage(t, s); count != 1 || size != before {
		t.Fatal("legacy queue accounting changed ordinary bytes")
	}
	identityCapture(t, s, token, []map[string]any{identifyEvent("new-control", "account", "anon", map[string]any{"trait": "new"}, map[string]any{})}, 200)
	var actual int64
	if err := s.db.QueryRow(`SELECT sum(` + queueByteExpression("") + `) FROM events`).Scan(&actual); err != nil {
		t.Fatal(err)
	}
	if count, size := queueUsage(t, s); count != 2 || size != actual || size <= before {
		t.Fatal("upgraded triggers did not charge private snapshot bytes")
	}
}

func TestIdentityConcurrentConflictsChooseOneCanonicalTarget(t *testing.T) {
	s := service(t, 100)
	projectID, token := project(t, s)
	handler := s.Handler()
	responses := make(chan string, 20)
	for i := range 20 {
		body, _ := json.Marshal(map[string]any{"api_key": token, "batch": []map[string]any{identifyEvent(fmt.Sprintf("concurrent-%d", i), fmt.Sprintf("account-%d", i), "same-anon", map[string]any{}, map[string]any{})}})
		go func() {
			r := httptest.NewRequest("POST", "/batch/", strings.NewReader(string(body)))
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			if w.Code != 200 {
				responses <- fmt.Sprintf("%d: %s", w.Code, w.Body.String())
				return
			}
			responses <- ""
		}()
	}
	for range 20 {
		if response := <-responses; response != "" {
			t.Fatal("concurrent valid control was rejected", response)
		}
	}
	var profiles, links int
	if err := s.db.QueryRow(`SELECT count(*) FROM identity_profiles WHERE project_id=?`, projectID).Scan(&profiles); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow(`SELECT count(*) FROM identity_links WHERE project_id=?`, projectID).Scan(&links); err != nil {
		t.Fatal(err)
	}
	if profiles != 1 || links != 1 || eventCount(t, s) != 20 {
		t.Fatal("concurrent conflicts created competing canonical identities or dropped records")
	}
}

func TestIdentityServerOnlyStillRequiresActualProjectOAuth(t *testing.T) {
	f := newOAuthIntegration(t)
	admin := f.token(t, "bootstrap", adminToken, f.s.oauth.apiResource)
	var created map[string]any
	f.call(t, "POST", "/api/v1/projects", admin.AccessToken, map[string]any{"name": "server identities", "ingestion_mode": "server_only"}, 201, &created)
	projectID := created["project"].(map[string]any)["id"].(string)
	writer := f.createClient(t, admin.AccessToken, []string{"events:write"}, []string{projectID})
	access := f.token(t, writer.ClientID, writer.ClientSecret, f.s.oauth.apiResource)
	var legacy AccessKeyCreated
	f.call(t, "POST", "/api/v1/access-keys", admin.AccessToken, map[string]any{"name": "selector", "permissions": []string{"events:write"}, "projects": []string{projectID}}, 201, &legacy)
	control := identifyEvent("server-control", "application-user", "", map[string]any{"plan": "server"}, map[string]any{})
	f.call(t, "POST", "/batch/", "", map[string]any{"api_key": legacy.Token, "batch": []any{control}}, 401, nil)
	f.call(t, "POST", "/batch/", access.AccessToken, map[string]any{"batch": []any{control}}, 200, nil)
	traits, identified, _ := storedIdentityProfile(t, f.s, projectID, "application-user")
	if !identified || traits["plan"] != "server" {
		t.Fatal("server-only OAuth identify did not apply canonical profile")
	}
}
