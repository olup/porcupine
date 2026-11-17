package porcupine

import (
	"fmt"
	"net/http"
	"net/url"
	"path/filepath"
	"testing"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

func TestSharedOperatorAPIProvisioner(t *testing.T) {
	t.Parallel()
	skipInShort(t)

	dsn := sharedPostgresDSN(t)
	configA := sharedJourneyProcessConfig{
		DatabaseURL: dsn,
		DBPath:      filepath.Join(t.TempDir(), "journal.db"),
		WorkerID:    "operator-a",
		OAuthIssuer: "http://localhost:8080",
	}
	configB := configA
	configB.DBPath = filepath.Join(t.TempDir(), "journal.db")
	configB.WorkerID = "operator-b"
	a, b := startSharedJourneyProcess(t, configA), startSharedJourneyProcess(t, configB)

	// Bootstrap once. The private provisioner uses its revocable API key for
	// every subsequent control operation and can use either worker.
	bootstrap := clientcredentials.Config{
		ClientID: "bootstrap", ClientSecret: adminToken, TokenURL: a.origin + "/oauth/token",
		AuthStyle: oauth2.AuthStyleInHeader, Scopes: []string{"instance:admin"},
		EndpointParams: url.Values{"resource": {configA.OAuthIssuer + "/api/v1"}},
	}
	bootstrapToken, err := bootstrap.Token(t.Context())
	if err != nil {
		t.Fatal("exchange the bootstrap credential", err)
	}
	client := sharedJourneyHTTP(t, a.origin, http.MethodPost, "/api/v1/oauth-clients", bootstrapToken.AccessToken,
		[]byte(`{"name":"Private provisioner","permissions":["instance:admin"],"projects":["*"]}`), nil, 201)
	key := sharedJourneyHTTP(t, b.origin, http.MethodPost, "/api/v1/oauth-clients/"+client["client_id"].(string)+"/api-keys", bootstrapToken.AccessToken,
		[]byte(`{"name":"Provisioner API key","audience":"api","permissions":["instance:admin"],"projects":["*"]}`), nil, 201)
	operator := key["token"].(string)
	call := func(origin, method, path, body string, want int) map[string]any {
		t.Helper()
		return sharedJourneyHTTP(t, origin, method, "/api/v1"+path, operator, []byte(body), nil, want)
	}
	tenantA := call(b.origin, http.MethodPost, "/tenants", `{"name":"Customer A"}`, 201)["tenant"].(map[string]any)["id"].(string)
	tenantB := call(a.origin, http.MethodPost, "/tenants", `{"name":"Customer B"}`, 201)["tenant"].(map[string]any)["id"].(string)
	call(b.origin, http.MethodPut, "/tenants/"+tenantA+"/budgets",
		`{"ingestion_monthly_events":1,"hot_query_max_concurrent":1,"hot_query_timeout_ms":1000,"hot_query_max_rows":100,"hot_query_max_result_bytes":65536}`, 200)

	createProject := func(origin, tenant string) (id, token string) {
		t.Helper()
		project := call(origin, http.MethodPost, "/projects", fmt.Sprintf(`{"name":"Website","tenant_id":%q}`, tenant), 201)
		return project["project"].(map[string]any)["id"].(string), project["ingest_token"].(string)
	}
	projectA, captureA := createProject(a.origin, tenantA)
	projectB, captureB := createProject(b.origin, tenantB)
	eventsA := "/api/v1/projects/" + projectA + "/events"
	eventsB := "/api/v1/projects/" + projectB + "/events"
	accountPath := "/tenants/" + tenantA + "/service-accounts"
	grants := fmt.Sprintf(`"permissions":["events:read"],"projects":[%q]`, projectA)
	customer := call(b.origin, http.MethodPost, accountPath, `{"name":"Customer dashboard",`+grants+`}`, 201)
	customerKeyPath := accountPath + "/" + customer["client_id"].(string) + "/api-keys"
	customerKey := call(a.origin, http.MethodPost, customerKeyPath, `{"name":"Dashboard API key","audience":"api",`+grants+`}`, 201)
	reader := customerKey["token"].(string)
	sharedJourneyHTTP(t, b.origin, http.MethodGet, eventsA, reader, nil, nil, 200)
	sharedJourneyHTTP(t, a.origin, http.MethodGet, eventsB, reader, nil, nil, 403)
	sharedJourneyHTTP(t, b.origin, http.MethodGet, "/api/v1/usage", reader, nil, nil, 403)

	const event = `{"type":"track","message_id":"before-crash","name":"page_viewed","actor_id":"visitor"}`
	first := sharedJourneyNative(t, b.origin, captureA, event, 200)
	if first["accepted"] != float64(1) || first["durability"] != "durable" {
		t.Fatalf("expected one durably accepted event: %v", first)
	}
	over := sharedJourneyNative(t, b.origin, captureA, `{"type":"track","message_id":"over-budget","name":"page_viewed","actor_id":"visitor"}`, 429)
	if over["code"] != "ingestion_budget_exceeded" || over["receipts"] != nil {
		t.Fatalf("budget refusal returned an unexpected response: %v", over)
	}

	call(a.origin, http.MethodDelete, customerKeyPath+"/"+customerKey["id"].(string), "", 204)
	for _, worker := range []*sharedJourneyProcess{a, b} {
		sharedJourneyHTTP(t, worker.origin, http.MethodGet, eventsA, reader, nil, nil, 401)
	}

	a.stopProcess(t, true)
	sharedJourneyNative(t, b.origin, captureA, event, 503)
	other := sharedJourneyNative(t, b.origin, captureB, event, 200)
	if other["accepted"] != float64(1) {
		t.Fatalf("owner outage or another tenant's budget blocked the healthy tenant: %v", other)
	}

	a = startSharedJourneyProcess(t, configA)
	replay := sharedJourneyNative(t, b.origin, captureA, event, 200)
	if replay["duplicates"] != float64(1) || sharedJourneyReceipt(t, replay) != sharedJourneyReceipt(t, first) {
		t.Fatalf("owner restart changed the acknowledged receipt: first=%v retry=%v", first, replay)
	}
	for _, path := range []string{eventsA, eventsB} {
		rows := sharedJourneyHTTP(t, b.origin, http.MethodGet, path, operator, nil, nil, 200)["events"].([]any)
		if len(rows) != 1 {
			t.Fatalf("expected one retained event at %s, got %d", path, len(rows))
		}
	}
	sharedJourneyHTTP(t, a.origin, http.MethodGet, eventsA, reader, nil, nil, 401)
	usage := call(b.origin, http.MethodGet, "/tenants/"+tenantA+"/usage", "", 200)
	if usage["accepted_events"] != "1" || usage["remaining_events"] != "0" {
		t.Fatalf("restart and replay changed the budget counters: %v", usage)
	}

	// Revoking the provisioner's API key is itself a standard operator call.
	sharedJourneyHTTP(t, b.origin, http.MethodDelete, "/api/v1/access-keys/"+key["id"].(string), bootstrapToken.AccessToken, nil, nil, 204)
	for _, worker := range []*sharedJourneyProcess{a, b} {
		sharedJourneyHTTP(t, worker.origin, http.MethodGet, "/api/v1/tenants", operator, nil, nil, 401)
	}
}
