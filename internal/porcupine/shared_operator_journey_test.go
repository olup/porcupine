package porcupine

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	usageapi "github.com/olup/porcupine/internal/usage"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

func TestSharedOperatorAPIProvisioner(t *testing.T) {
	t.Parallel()
	skipInShort(t)

	dsn := sharedPostgresDSN(t)
	fixture := nativeS3Fixture(t)
	issuer := "http://localhost:8080"
	config := hostedJourneyConfig(t, dsn, fixture)
	config.OAuth = &OAuthConfig{Issuer: issuer}
	serviceA, a, _ := hostedCleanupWorker(t, config)
	config.DuckLake.Directory = t.TempDir()
	serviceB, b, _ := hostedCleanupWorker(t, config)

	// Bootstrap once. The private provisioner uses its revocable API key for
	// every subsequent control operation and can use either worker.
	bootstrap := clientcredentials.Config{
		ClientID: "bootstrap", ClientSecret: adminToken, TokenURL: a.URL + "/oauth/token",
		AuthStyle: oauth2.AuthStyleInHeader, Scopes: []string{"instance:admin"},
		EndpointParams: url.Values{"resource": {issuer + "/api/v1"}},
	}
	bootstrapToken, err := bootstrap.Token(t.Context())
	if err != nil {
		t.Fatal("exchange the bootstrap credential", err)
	}
	client := sharedJourneyHTTP(t, a.URL, http.MethodPost, "/api/v1/oauth-clients", bootstrapToken.AccessToken,
		[]byte(`{"name":"Private provisioner","permissions":["instance:admin"],"projects":["*"]}`), nil, 201)
	key := sharedJourneyHTTP(t, b.URL, http.MethodPost, "/api/v1/oauth-clients/"+client["client_id"].(string)+"/api-keys", bootstrapToken.AccessToken,
		[]byte(`{"name":"Provisioner API key","audience":"api","permissions":["instance:admin"],"projects":["*"]}`), nil, 201)
	operator := key["token"].(string)
	call := func(origin, method, path, body string, want int) map[string]any {
		t.Helper()
		return sharedJourneyHTTP(t, origin, method, "/api/v1"+path, operator, []byte(body), nil, want)
	}
	tenantA := call(b.URL, http.MethodPost, "/tenants", `{"name":"Customer A"}`, 201)["tenant"].(map[string]any)["id"].(string)
	tenantB := call(a.URL, http.MethodPost, "/tenants", `{"name":"Customer B"}`, 201)["tenant"].(map[string]any)["id"].(string)
	call(b.URL, http.MethodPut, "/tenants/"+tenantA+"/budgets",
		`{"ingestion_monthly_events":1,"hot_query_max_concurrent":1,"hot_query_timeout_ms":1000,"hot_query_max_rows":100,"hot_query_max_result_bytes":65536}`, 200)

	createProject := func(origin, tenant string) string {
		t.Helper()
		project := call(origin, http.MethodPost, "/projects", fmt.Sprintf(`{"name":"Website","tenant_id":%q}`, tenant), 201)
		return project["project"].(map[string]any)["id"].(string)
	}
	projectA := createProject(a.URL, tenantA)
	projectB := createProject(b.URL, tenantB)
	eventsA := "/api/v1/projects/" + projectA + "/events"
	eventsB := "/api/v1/projects/" + projectB + "/events"
	accountPath := "/tenants/" + tenantA + "/service-accounts"
	grants := fmt.Sprintf(`"permissions":["events:read"],"projects":[%q]`, projectA)
	customer := call(b.URL, http.MethodPost, accountPath, `{"name":"Customer dashboard",`+grants+`}`, 201)
	customerKeyPath := accountPath + "/" + customer["client_id"].(string) + "/api-keys"
	customerKey := call(a.URL, http.MethodPost, customerKeyPath, `{"name":"Dashboard API key","audience":"api",`+grants+`}`, 201)
	reader := customerKey["token"].(string)
	hostedCleanupWrite(t, serviceA, projectA, "customer A event")
	hostedCleanupWrite(t, serviceB, projectB, "customer B event")
	sharedJourneyHTTP(t, b.URL, http.MethodGet, eventsA, reader, nil, nil, 200)
	sharedJourneyHTTP(t, a.URL, http.MethodGet, eventsB, reader, nil, nil, 403)
	sharedJourneyHTTP(t, b.URL, http.MethodGet, "/api/v1/usage", reader, nil, nil, 403)

	call(a.URL, http.MethodDelete, customerKeyPath+"/"+customerKey["id"].(string), "", 204)
	for _, worker := range []*httptest.Server{a, b} {
		sharedJourneyHTTP(t, worker.URL, http.MethodGet, eventsA, reader, nil, nil, 401)
	}
	for _, path := range []string{eventsA, eventsB} {
		rows := sharedJourneyHTTP(t, b.URL, http.MethodGet, path, operator, nil, nil, 200)["events"].([]any)
		if len(rows) != 1 {
			t.Fatalf("expected one retained event at %s, got %d", path, len(rows))
		}
	}

	usage := call(b.URL, http.MethodGet, "/tenants/"+tenantA+"/usage", "", 200)
	if usage["accepted_events"] != "1" || usage["remaining_events"] != "0" {
		t.Fatalf("hosted publication did not update the tenant budget: %v", usage)
	}
	// Advance the rollup retention clock without changing the budget month.
	meter := usageapi.NewStore(serviceA.control, true, usageapi.Config{RetentionDays: 1}, func() time.Time {
		return time.Now().AddDate(0, 0, 2)
	})
	if err := meter.Prune(t.Context()); err != nil {
		t.Fatal("prune hosted daily usage", err)
	}
	var daily int64
	if err := serviceA.control.QueryRow(`SELECT coalesce(sum(live_events),0) FROM usage_rollups WHERE tenant_id=$1`, tenantA).Scan(&daily); err != nil || daily != 0 {
		t.Fatalf("retained daily events = %d after pruning: %v", daily, err)
	}
	usage = call(a.URL, http.MethodGet, "/tenants/"+tenantA+"/usage", "", 200)
	if usage["accepted_events"] != "1" || usage["remaining_events"] != "0" {
		t.Fatalf("daily rollup expiry changed the tenant budget: %v", usage)
	}

	// Revoking the provisioner's API key is itself a standard operator call.
	sharedJourneyHTTP(t, b.URL, http.MethodDelete, "/api/v1/access-keys/"+key["id"].(string), bootstrapToken.AccessToken, nil, nil, 204)
	for _, worker := range []*httptest.Server{a, b} {
		sharedJourneyHTTP(t, worker.URL, http.MethodGet, "/api/v1/tenants", operator, nil, nil, 401)
	}
}
