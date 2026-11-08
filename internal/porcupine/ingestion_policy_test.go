package porcupine

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func TestServerOnlyProjectRequiresActualOAuthOnEveryCaptureRoute(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprintf("allow_legacy_%t", legacy), func(t *testing.T) {
			f := newOAuthIntegration(t)
			f.s.oauth.config.AllowLegacyKeys = legacy
			admin := f.token(t, "bootstrap", adminToken, f.s.oauth.apiResource)
			var created map[string]any
			f.call(t, "POST", "/api/v1/projects", admin.AccessToken, map[string]string{"name": "Confidential server events", "ingestion_mode": "server_only"}, 201, &created)
			project := created["project"].(map[string]any)
			id := project["id"].(string)
			if project["ingestion_mode"] != ingestionServerOnly {
				t.Fatal("server ingestion mode absent from project response")
			}
			if _, present := created["ingest_token"]; present {
				t.Fatal("server-only project minted a public ingestion key")
			}
			var credentials int
			if err := f.s.db.QueryRow(`SELECT count(*) FROM access_keys,json_each(access_keys.projects) WHERE json_each.value=?`, id).Scan(&credentials); err != nil || credentials != 0 {
				t.Fatal("server-only creation stored an ingestion credential")
			}
			var listed ProjectList
			f.call(t, "GET", "/api/v1/projects", admin.AccessToken, nil, 200, &listed)
			if len(listed.Projects) != 1 || listed.Projects[0].IngestionMode != ingestionServerOnly {
				t.Fatal("project listing lost ingestion policy")
			}
			writer := f.createClient(t, admin.AccessToken, []string{"events:write"}, []string{id})
			writeToken := f.token(t, writer.ClientID, writer.ClientSecret, f.s.oauth.apiResource)
			mcpToken := f.token(t, writer.ClientID, writer.ClientSecret, f.s.oauth.mcpResource)
			reader := f.createClient(t, admin.AccessToken, []string{"events:read"}, []string{id})
			readToken := f.token(t, reader.ClientID, reader.ClientSecret, f.s.oauth.apiResource)
			var oldKey AccessKeyCreated
			f.call(t, "POST", "/api/v1/access-keys", admin.AccessToken, map[string]any{"name": "Legacy project selector", "permissions": []string{"events:write"}, "projects": []string{id}}, 201, &oldKey)
			event := func(uuid string) map[string]any {
				return map[string]any{"uuid": uuid, "event": "confidential server event", "distinct_id": "application-user", "properties": map[string]any{"unicode": "東京 🦔"}}
			}
			for index, route := range []string{"/capture/", "/batch/", "/i/v0/e/", "/e/"} {
				one := event(fmt.Sprintf("route-%d", index))
				var body any = one
				if route == "/batch/" {
					body = map[string]any{"batch": []any{one}}
				}
				if route == "/i/v0/e/" {
					body = []any{one}
				}
				publicBody := map[string]any{"api_key": oldKey.Token, "batch": []any{one}}
				f.call(t, "POST", route, "", publicBody, 401, nil)
				f.call(t, "POST", route, oldKey.Token, body, 401, nil)
				f.call(t, "POST", route, "pc_at_not-a-real-token", publicBody, 401, nil)
				f.call(t, "POST", route, writer.ClientSecret, publicBody, 401, nil)
				f.call(t, "POST", route, mcpToken.AccessToken, body, 401, nil)
				f.call(t, "POST", route, readToken.AccessToken, body, 403, nil)
				if count := eventCount(t, f.s); count != index {
					t.Fatal("rejected server-only request stored events")
				}
				f.call(t, "POST", route, writeToken.AccessToken, body, 200, nil)
			}
			before := eventCount(t, f.s)
			invalid := event("invalid-batch-row")
			invalid["distinct_id"] = ""
			f.call(t, "POST", "/batch/", writeToken.AccessToken, map[string]any{"batch": []any{event("valid-atomic-row"), invalid}}, 400, nil)
			if eventCount(t, f.s) != before {
				t.Fatal("invalid server-only batch partially committed")
			}
			other, otherPublic := f.project(t, admin.AccessToken)
			wrong := event("wrong-project-selector")
			wrong["api_key"] = otherPublic
			f.call(t, "POST", "/capture/", writeToken.AccessToken, wrong, 401, nil)
			conflict := event("conflicting-batch-token")
			conflict["api_key"] = otherPublic
			f.call(t, "POST", "/batch/", writeToken.AccessToken, map[string]any{"api_key": oldKey.Token, "batch": []any{event("first"), conflict}}, 400, nil)
			if eventCount(t, f.s) != before {
				t.Fatal("cross-project server-only request stored events")
			}
			multi := f.createClient(t, admin.AccessToken, []string{"events:write"}, []string{id, other})
			multiToken := f.token(t, multi.ClientID, multi.ClientSecret, f.s.oauth.apiResource)
			f.call(t, "POST", "/capture/", multiToken.AccessToken, event("ambiguous-server-project"), 403, nil)
			selected := event("oauth-selected-server-project")
			selected["api_key"] = oldKey.Token
			f.call(t, "POST", "/capture/", multiToken.AccessToken, selected, 200, nil)
			// A payload project ID never changes the authenticated project's scope.
			spoof := event("body-project-ignored")
			spoof["project_id"] = other
			f.call(t, "POST", "/capture/", writeToken.AccessToken, spoof, 200, nil)
			var savedProject string
			if err := f.s.db.QueryRow(`SELECT project_id FROM events WHERE uuid='body-project-ignored'`).Scan(&savedProject); err != nil || savedProject != id {
				t.Fatal("payload overrode authenticated project")
			}
			var serverEvents EventList
			f.call(t, "GET", "/api/v1/projects/"+id+"/events", readToken.AccessToken, nil, 200, &serverEvents)
			if len(serverEvents.Events) != 6 {
				t.Fatal("wrong number of authenticated server events")
			}
			for _, stored := range serverEvents.Events {
				if stored.DistinctID != "application-user" {
					t.Fatal("OAuth principal replaced visitor distinct ID")
				}
			}
			f.now.Add(int64(61 * time.Second))
			f.call(t, "POST", "/capture/", writeToken.AccessToken, selected, 401, nil)
		})
	}
}

func TestProjectIngestionModeDefaultsAndConfiguration(t *testing.T) {
	s := service(t, 20)
	for _, body := range []string{`{"name":"default"}`, `{"name":"explicit","ingestion_mode":"mixed"}`} {
		created := request(t, s, "POST", "/api/v1/projects", adminToken, body, 201)
		if created["project"].(map[string]any)["ingestion_mode"] != ingestionMixed || created["ingest_token"] == "" {
			t.Fatal("mixed project lost public ingestion compatibility")
		}
		request(t, s, "POST", "/capture/", "", captureBody(created["ingest_token"].(string), newID()), 200)
	}
	request(t, s, "POST", "/api/v1/projects", adminToken, `{"name":"invalid","ingestion_mode":"browser_only"}`, 422)
	request(t, s, "POST", "/api/v1/projects", adminToken, `{"name":"server","ingestion_mode":"server_only"}`, 503)
	var projects int
	if err := s.db.QueryRow(`SELECT count(*) FROM projects`).Scan(&projects); err != nil || projects != 2 {
		t.Fatal("invalid project mode created partial metadata")
	}
}

func TestProjectIngestionPolicyMigrationAndRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "old-schema.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`CREATE TABLE projects(id TEXT PRIMARY KEY,name TEXT NOT NULL,created_at TEXT NOT NULL);
CREATE TABLE access_keys(id TEXT PRIMARY KEY,name TEXT NOT NULL,token_hash TEXT UNIQUE NOT NULL,permissions TEXT NOT NULL,projects TEXT NOT NULL,revoked_at TEXT,created_at TEXT NOT NULL);
INSERT INTO projects VALUES('old-project','Existing project','2020-01-01T00:00:00Z')`)
	if err == nil {
		_, err = db.Exec(`INSERT INTO access_keys(id,name,token_hash,permissions,projects,created_at) VALUES('old-key','Old ingestion',?,'["events:write"]','["old-project"]','2020-01-01T00:00:00Z')`, hash("old-public-project-key"))
	}
	db.Close()
	if err != nil {
		t.Fatal(err)
	}
	s, err := Open(Config{DBPath: path, BootstrapToken: adminToken})
	if err != nil {
		t.Fatal(err)
	}
	var mode string
	if err := s.db.QueryRow(`SELECT ingestion_mode FROM projects WHERE id='old-project'`).Scan(&mode); err != nil || mode != ingestionMixed {
		s.Close()
		t.Fatal("old project did not migrate to mixed")
	}
	request(t, s, "POST", "/capture/", "", captureBody("old-public-project-key", "old-key-still-works"), 200)
	s.Close()
	// Create a persisted confidential project with OAuth, then restart without OAuth.
	s, err = Open(Config{DBPath: path, OAuth: &OAuthConfig{Issuer: "https://policy.example", AllowLegacyKeys: true}})
	if err != nil {
		t.Fatal(err)
	}
	created := request(t, s, "POST", "/api/v1/projects", adminToken, `{"name":"Persisted confidential project","ingestion_mode":"server_only"}`, 201)
	id := created["project"].(map[string]any)["id"].(string)
	_, selector := key(t, s, []string{"events:write"}, []string{id})
	s.Close()
	s, err = Open(Config{DBPath: path})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.db.QueryRow(`SELECT ingestion_mode FROM projects WHERE id=?`, id).Scan(&mode); err != nil || mode != ingestionServerOnly {
		t.Fatal("restart lost server-only policy")
	}
	request(t, s, "POST", "/capture/", "", captureBody(selector, "no-oauth-public-rejected"), 401)
	request(t, s, "POST", "/capture/", selector, captureBody(selector, "no-oauth-legacy-rejected"), 401)
	request(t, s, "POST", "/capture/", "", captureBody("old-public-project-key", "old-mixed-still-works"), 200)
	var forbidden int
	if err := s.db.QueryRow(`SELECT count(*) FROM events WHERE project_id=?`, id).Scan(&forbidden); err != nil || forbidden != 0 {
		t.Fatal("server-only accepted writes after OAuth was disabled")
	}
	listed := request(t, s, "GET", "/api/v1/projects", adminToken, "", 200)
	modes := map[string]string{}
	for _, item := range listed["projects"].([]any) {
		metadata := item.(map[string]any)
		modes[metadata["id"].(string)] = metadata["ingestion_mode"].(string)
	}
	if len(modes) != 2 || modes["old-project"] != ingestionMixed || modes[id] != ingestionServerOnly {
		t.Fatal("migrated project listing lost stored ingestion policies")
	}
}
