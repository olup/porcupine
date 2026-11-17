package porcupine

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/olup/porcupine/internal/output"
)

func newTestPipelineConfig(c *output.PipelineConfig) (*output.PipelineConfig, error) {
	_, err := output.NewSQLitePipelines(output.PipelineDependencies{}, c)
	return c, err
}
func attachTestPipelines(s *Service, c *output.PipelineConfig) { mustConfigureTestPipelines(s, c) }
func mustConfigureTestPipelines(s *Service, c *output.PipelineConfig) {
	if err := s.configurePipelines(c); err != nil {
		panic(err)
	}
	if c != nil {
		if err := s.pipelines.Initialize(context.Background()); err != nil {
			panic(err)
		}
	}
}
func testPipelineConfig(s *Service) *output.PipelineConfig {
	c := s.pipelines.Config()
	c.Key = bytes.Repeat([]byte{7}, 32)
	return &c
}
func updateTestPipelines(s *Service, update func(*output.PipelineConfig)) {
	c := testPipelineConfig(s)
	update(c)
	mustConfigureTestPipelines(s, c)
}
func enableTestPipelines(t *testing.T, s *Service) {
	t.Helper()
	runtime, err := newTestPipelineConfig(&output.PipelineConfig{Key: bytes.Repeat([]byte{7}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	attachTestPipelines(s, runtime)
	t.Cleanup(func() { attachTestPipelines(s, nil) })
}
func pipelineBody(endpoint string) string {
	data, _ := json.Marshal(map[string]any{"name": "Test hook", "kind": "webhook", "webhook": map[string]any{"endpoint": endpoint, "include_events": []string{"checkout"}}, "credentials": map[string]any{"bearer_token": "private-pipeline-secret"}})
	return string(data)
}
func TestPipelineHTTPPermissionIsolationRedactionAndRotation(t *testing.T) {
	t.Parallel()
	s := service(t, 20)
	enableTestPipelines(t, s)
	id, _ := project(t, s)
	other, _ := project(t, s)
	_, manager := key(t, s, []string{"pipelines:write", "pipelines:read", "events:read"}, []string{id})
	_, reader := key(t, s, []string{"events:read"}, []string{id})
	_, writeOnly := key(t, s, []string{"pipelines:write"}, []string{id})
	_, metadataOnly := key(t, s, []string{"pipelines:read"}, []string{id})
	base := "/api/v1/projects/" + id + "/pipelines"
	endpoint := "https://hooks.example.com/path?credential=private-url-secret"
	body := pipelineBody(endpoint)
	request(t, s, "POST", base, "", body, 401)
	request(t, s, "POST", base, reader, body, 403)
	request(t, s, "POST", base, writeOnly, body, 403)
	request(t, s, "GET", base, metadataOnly, "", 403)
	request(t, s, "POST", "/api/v1/projects/"+other+"/pipelines", manager, body, 403)
	created := request(t, s, "POST", base, manager, body, 201)
	pipelineID := created["id"].(string)
	if created["webhook"].(map[string]any)["delivery_mode"] != "event" {
		t.Fatal("missing public webhook rule")
	}
	request(t, s, "GET", "/api/v1/projects/"+other+"/pipelines/"+pipelineID, adminToken, "", 404)
	for _, result := range []map[string]any{created, request(t, s, "GET", base, manager, "", 200), request(t, s, "GET", base+"/"+pipelineID, manager, "", 200)} {
		encoded, _ := json.Marshal(result)
		for _, private := range []string{"private-pipeline-secret", "private-url-secret", "hooks.example.com", "headers", "credentials", "actor_id", "endpoint", "secret"} {
			if strings.Contains(string(encoded), private) {
				t.Fatalf("response leaked %q: %s", private, encoded)
			}
		}
	}
	stored, err := loadTestPipeline(s, context.Background(), id, pipelineID)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(stored.Secret, []byte("private-pipeline-secret")) || bytes.Contains(stored.Secret, []byte(endpoint)) || bytes.Contains(stored.Config, []byte(endpoint)) {
		t.Fatal("destination not encrypted")
	}
	plaintext, err := s.pipelines.OpenSecret(id, pipelineID, stored.Secret)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(plaintext, []byte(endpoint)) || !bytes.Contains(plaintext, []byte("private-pipeline-secret")) {
		t.Fatal("encrypted destination missing")
	}
	rotated := request(t, s, "PUT", base+"/"+pipelineID+"/credentials", manager, `{"credentials":{"bearer_token":"replacement-private-secret"}}`, 200)
	if rotated["id"] != pipelineID {
		t.Fatal("rotation replaced pipeline")
	}
	updated, err := loadTestPipeline(s, context.Background(), id, pipelineID)
	if err != nil {
		t.Fatal(err)
	}
	plaintext, err = s.pipelines.OpenSecret(id, pipelineID, updated.Secret)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(stored.Secret, updated.Secret) || !bytes.Contains(plaintext, []byte("replacement-private-secret")) || bytes.Contains(plaintext, []byte("private-pipeline-secret")) || !bytes.Contains(plaintext, []byte(endpoint)) {
		t.Fatal("rotation did not replace only credentials")
	}
	request(t, s, "PUT", base+"/"+pipelineID+"/credentials", manager, `{"credentials":{"headers":{"Host":"private-invalid-header"}}}`, 400)
	after, err := loadTestPipeline(s, context.Background(), id, pipelineID)
	if err != nil || !bytes.Equal(updated.Secret, after.Secret) {
		t.Fatal("invalid replacement changed credentials")
	}
	request(t, s, "POST", base+"/"+pipelineID+"/pause", manager, "", 200)
	paused := request(t, s, "GET", base+"/"+pipelineID, manager, "", 200)
	if paused["state"] != "paused" {
		t.Fatal("pause not persisted")
	}
	request(t, s, "POST", base+"/"+pipelineID+"/resume", manager, "", 200)
	request(t, s, "DELETE", base+"/"+pipelineID, manager, "", 204)
	request(t, s, "GET", base+"/"+pipelineID, manager, "", 404)
}

func TestPipelineValidationDoesNotEchoSecretsAndDisabledStatus(t *testing.T) {
	t.Parallel()
	s := service(t, 20)
	id, _ := project(t, s)
	base := "/api/v1/projects/" + id + "/pipelines"
	disabled := request(t, s, "GET", base, adminToken, "", 200)
	if disabled["enabled"] != false {
		t.Fatal("disabled pipeline status")
	}
	request(t, s, "POST", base, adminToken, pipelineBody("https://hooks.example.com"), 503)
	enableTestPipelines(t, s)
	for _, test := range []struct {
		body   string
		status int
	}{
		{`{"name":"test","kind":"invalid-private-marker","credentials":{"bearer_token":"private-marker"}}`, 422},
		{pipelineBody("https://private-marker@hooks.example.com"), 400},
		{`{"name":"test","kind":"webhook","webhook":{"endpoint":"https://hooks.example.com","delivery_mode":"batch","interval_seconds":1},"credentials":{"bearer_token":"private-marker"}}`, 400},
		{`{"name":"test","kind":"webhook","webhook":{"endpoint":"https://hooks.example.com"},"credentials":{"unknown":"private-marker"}}`, 422},
		{`{"name":"test","kind":"webhook","webhook":{"endpoint":"https://hooks.example.com"},"credentials":{"bearer_token":"private-marker`, 400},
	} {
		response := request(t, s, "POST", base, adminToken, test.body, test.status)
		encoded, _ := json.Marshal(response)
		if strings.Contains(string(encoded), "private-marker") {
			t.Fatalf("validation reflected secret: %s", encoded)
		}
	}
	request(t, s, "POST", base, adminToken, `{"name":"S3","kind":"s3","credentials":{"access_key_id":"key","secret_access_key":"secret"}}`, 503)
}

func TestPipelineTransactionalQuotaAndRunMetadata(t *testing.T) {
	t.Parallel()
	s := service(t, 20)
	enableTestPipelines(t, s)
	id, _ := project(t, s)
	base := "/api/v1/projects/" + id + "/pipelines"
	created := request(t, s, "POST", base, adminToken, pipelineBody("https://hooks.example.com"), 201)
	pipelineID := created["id"].(string)
	_, err := s.db.Exec(`WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<63) INSERT INTO pipelines(id,project_id,actor_id,kind,name,state,config,secret,created_at_ms) SELECT 'limit-'||n.x,project_id,actor_id,kind,name,state,config,secret,created_at_ms FROM pipelines,n WHERE id=?`, pipelineID)
	if err != nil {
		t.Fatal(err)
	}
	request(t, s, "POST", base, adminToken, pipelineBody("https://hooks.example.com"), 409)
	var count int
	if s.db.QueryRow(`SELECT count(*) FROM pipelines WHERE project_id=?`, id).Scan(&count) != nil || count != 64 {
		t.Fatal("pipeline quota not atomic")
	}
	_, err = s.db.Exec(`INSERT INTO webhook_deliveries(id,pipeline_id,project_id,event_uuid,payload,bytes,state,attempts,next_attempt_ms,error_code,created_at_ms) VALUES('run-one',?,?,'event-one','{}',2,'failed',2,0,'private-raw-error',1)`, pipelineID, id)
	if err != nil {
		t.Fatal(err)
	}
	runs := request(t, s, "GET", base+"/"+pipelineID+"/runs", adminToken, "", 200)
	items := runs["runs"].([]any)
	if len(items) != 1 || items[0].(map[string]any)["error"] != "delivery_failed" {
		t.Fatal("unsafe or missing run metadata", runs)
	}
	request(t, s, "GET", base+"/"+pipelineID+"/runs/run-one", adminToken, "", 200)
	request(t, s, "GET", base+"/"+pipelineID+"/runs/missing", adminToken, "", 404)
	request(t, s, "POST", base+"/"+pipelineID+"/runs/run-one/retry", adminToken, "", 202)
	request(t, s, "POST", base+"/"+pipelineID+"/runs/run-one/retry", adminToken, "", 409)
}

func TestPipelineMCPDiscoveryAndCallsUseCombinedPermissions(t *testing.T) {
	t.Parallel()
	s := service(t, 20)
	enableTestPipelines(t, s)
	id, _ := project(t, s)
	_, manager := key(t, s, []string{"pipelines:write", "pipelines:read", "events:read"}, []string{id})
	_, reader := key(t, s, []string{"events:read"}, []string{id})
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	ctx, session := mcpTestSession(t, host.URL, manager, "")
	catalog := mcpTestTool(t, ctx, session, "porcupine_docs", mcpDocsInput{Query: "pipeline", Limit: 20}, false)
	if len(catalog["operations"].([]any)) != 10 {
		t.Fatal("pipeline catalog incomplete", catalog)
	}
	var body map[string]any
	json.Unmarshal([]byte(pipelineBody("https://hooks.example.com")), &body)
	created := mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "create-pipeline", Path: map[string]string{"id": id}, Body: body}, 201)
	mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "get-pipeline", Path: map[string]string{"id": id, "pipeline_id": created["id"].(string)}}, 200)
	readerContext, readerSession := mcpTestSession(t, host.URL, reader, "")
	hidden := mcpTestTool(t, readerContext, readerSession, "porcupine_docs", mcpDocsInput{Query: "pipeline", Limit: 20}, false)
	if len(hidden["operations"].([]any)) != 0 {
		t.Fatal("event reader learned pipeline operation authority", hidden)
	}
}

func TestPipelineS3InspectableRuleAndWriteOnlyRotation(t *testing.T) {
	t.Parallel()
	s := duckLakeJourneyService(t)
	enableTestPipelines(t, s)
	id, _ := project(t, s)
	base := "/api/v1/projects/" + id + "/pipelines"
	body := `{"name":"Hourly results","kind":"s3","s3":{"sql":"SELECT uuid,event FROM events","format":"jsonl","interval_seconds":3600,"start_at":"2026-01-01T00:00:00Z","destination":{"bucket":"private-events","region":"us-east-1","prefix":"hourly"}},"credentials":{"access_key_id":"private-access-id","secret_access_key":"private-s3-secret"}}`
	created := request(t, s, "POST", base, adminToken, body, 201)
	pipelineID := created["id"].(string)
	listing := request(t, s, "GET", base, adminToken, "", 200)
	if listing["pipelines"].([]any)[0].(map[string]any)["s3"] != nil {
		t.Fatal("list returned unbounded full S3 rules")
	}
	rule := created["s3"].(map[string]any)
	if !strings.Contains(rule["sql"].(string), "events") || rule["interval_seconds"] != float64(3600) || rule["destination"].(map[string]any)["bucket"] != "private-events" {
		t.Fatal("S3 rule not inspectable", created)
	}
	for _, result := range []map[string]any{created, request(t, s, "GET", base, adminToken, "", 200), request(t, s, "GET", base+"/"+pipelineID, adminToken, "", 200)} {
		raw, _ := json.Marshal(result)
		for _, forbidden := range []string{"private-access-id", "private-s3-secret", "secret_access_key", "access_key_id", "credentials", "endpoint"} {
			if strings.Contains(string(raw), forbidden) {
				t.Fatalf("S3 metadata leaked %s: %s", forbidden, raw)
			}
		}
	}
	request(t, s, "PUT", base+"/"+pipelineID+"/credentials", adminToken, `{"credentials":{"access_key_id":"new-private-id","secret_access_key":"new-private-secret"}}`, 200)
	stored, err := loadTestPipeline(s, context.Background(), id, pipelineID)
	if err != nil {
		t.Fatal(err)
	}
	plaintext, err := s.pipelines.OpenSecret(id, pipelineID, stored.Secret)
	if err != nil || !bytes.Contains(plaintext, []byte("new-private-secret")) || bytes.Contains(plaintext, []byte("private-s3-secret")) {
		t.Fatal("S3 secret not replaced")
	}
}

func TestPipelineOpenAPIOAuthScopesAndSecretSchemas(t *testing.T) {
	t.Parallel()
	s, _ := oauthProtocolService(t)
	doc := request(t, s, "GET", "/openapi.json", "", "", 200)
	components := doc["components"].(map[string]any)
	scopes := components["securitySchemes"].(map[string]any)["oauth2"].(map[string]any)["flows"].(map[string]any)["clientCredentials"].(map[string]any)["scopes"].(map[string]any)
	for _, name := range []string{"pipelines:read", "pipelines:write"} {
		if scopes[name] == nil {
			t.Fatalf("missing OAuth permission %s", name)
		}
	}
	operation := doc["paths"].(map[string]any)["/api/v1/projects/{id}/pipelines"].(map[string]any)["post"].(map[string]any)
	required := operation["security"].([]any)[0].(map[string]any)["oauth2"].([]any)
	if len(required) != 2 || required[0] != "pipelines:write" || required[1] != "events:read" {
		t.Fatal("pipeline egress omitted a required OAuth scope", required)
	}
	schemas := components["schemas"].(map[string]any)
	for _, name := range []string{"Pipeline", "WebhookPipelineRule", "S3PipelineRule", "S3PipelinePublicDestination"} {
		properties := schemas[name].(map[string]any)["properties"].(map[string]any)
		for _, forbidden := range []string{"endpoint", "credentials", "headers", "secret", "secret_access_key", "access_key_id"} {
			if properties[forbidden] != nil {
				t.Fatalf("response schema %s contains private field %s", name, forbidden)
			}
		}
	}
	properties := schemas["PipelineCredentials"].(map[string]any)["properties"].(map[string]any)
	for _, name := range []string{"headers", "bearer_token", "access_key_id", "secret_access_key", "session_token"} {
		if properties[name].(map[string]any)["writeOnly"] != true {
			t.Fatalf("credential %s lacks write-only schema", name)
		}
	}
}

type testStoredPipeline struct {
	ID, ProjectID, ActorID, Kind, State string
	Config                              json.RawMessage
	Secret                              []byte
}

func loadTestPipeline(s *Service, ctx context.Context, project, id string) (testStoredPipeline, error) {
	var p testStoredPipeline
	var config string
	err := s.db.QueryRowContext(ctx, `SELECT id,project_id,actor_id,kind,state,config,secret FROM pipelines WHERE project_id=? AND id=?`, project, id).Scan(&p.ID, &p.ProjectID, &p.ActorID, &p.Kind, &p.State, &config, &p.Secret)
	p.Config = json.RawMessage(config)
	return p, err
}
func testS3RunPage(s *Service, ctx context.Context, project, pipeline string, limit, offset int) ([]output.PipelineRun, error) {
	r, err := s.pipelines.Runs(ctx, project, pipeline, limit, offset)
	if err != nil {
		return nil, err
	}
	return r.Runs, nil
}
func testS3Run(s *Service, ctx context.Context, project, pipeline, id string) (output.PipelineRun, error) {
	r, err := s.pipelines.Run(ctx, project, pipeline, id)
	if err != nil {
		return output.PipelineRun{}, err
	}
	return *r, nil
}

type testPipelineManifest struct {
	Version       int
	ProjectID     string `json:"project_id"`
	PipelineID    string `json:"pipeline_id"`
	RunID         string `json:"run_id"`
	WindowStartMS int64  `json:"window_start_ms"`
	WindowEndMS   int64  `json:"window_end_ms"`
	Format        string
	Rows          int64
	Columns       []struct{ Name string }
	Files         []struct {
		Key                       string
		Rows, Bytes, DecodedBytes int64
		SHA256                    string
	}
}

func retryTestPipelineRun(s *Service, ctx context.Context, project, pipeline, id string) (bool, error) {
	r, err := s.pipelines.Retry(ctx, project, pipeline, id)
	if err != nil {
		return false, err
	}
	return r, nil
}
