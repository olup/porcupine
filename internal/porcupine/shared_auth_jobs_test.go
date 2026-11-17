package porcupine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/auth"
	eventapi "github.com/olup/porcupine/internal/event"
)

func TestSharedAuthExpiredConsentDoesNotStarveWebhookQueue(t *testing.T) {
	t.Parallel()
	s, project, ingest, expiredPipeline := webhookTestService(t, "event")
	now := s.clock()
	s.clock = func() time.Time { return now }
	oauth, err := normalizeOAuthConfig(OAuthConfig{Issuer: "http://127.0.0.1:8081", Email: &EmailConfig{SendCode: func(context.Context, string, string) error { return nil }}})
	if err != nil {
		t.Fatal(err)
	}
	s.oauth = oauth
	defer func() { s.oauth = nil }()
	var operatorID string
	if err := s.db.QueryRow(`SELECT actor_id FROM pipelines WHERE id=$1`, expiredPipeline).Scan(&operatorID); err != nil {
		t.Fatal(err)
	}
	healthy := eventapi.NewID()
	secret, _ := json.Marshal(WebhookPipelineSecret{Endpoint: "https://healthy.example.test/events"})
	encrypted, err := s.sealPipelineSecret(project, healthy, secret)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO pipelines(id,project_id,actor_id,kind,name,state,config,secret,created_at_ms) SELECT $1,project_id,actor_id,kind,'healthy',state,config,$2,created_at_ms FROM pipelines WHERE id=$3`, healthy, encrypted, expiredPipeline); err != nil {
		t.Fatal(err)
	}
	scopes := []string{"events:read", "pipelines:write"}
	grants, _ := json.Marshal(scopes)
	projects, _ := json.Marshal([]string{project})
	human := EmailHuman{ID: eventapi.NewID(), Email: "pipeline-owner@example.test", CreatedAt: now.Format(time.RFC3339Nano), Permissions: scopes, Projects: []string{project}, Grants: []HumanProjectGrant{{ProjectID: project, Permissions: scopes}}}
	client, consent := eventapi.NewID(), eventapi.NewID()
	tx, err := s.control.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := insertEmailHuman(t.Context(), tx, human); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{client, consent} {
		if _, err := tx.Exec(`INSERT INTO access_keys(id,name,token_hash,permissions,projects,created_at) VALUES($1,'Pipeline consent test',$2,$3,$4,$5)`, id, hash(id), string(grants), string(projects), now.Format(time.RFC3339Nano)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tx.Exec(`INSERT INTO oauth_clients(client_id,secret_hash,grant_types,redirect_uris,token_endpoint_auth_method) VALUES($1,'not-a-login-secret','["authorization_code","refresh_token"]','["https://client.example.test/callback"]','none')`, client); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO oauth_interactive_grants(id,principal_id,human_id,client_id,resource,created_at_ms,expires_at_ms) VALUES($1,$1,$2,$3,$4,$5,$6)`, consent, human.ID, client, s.oauth.mcpResource, now.UnixMilli(), now.Add(time.Minute).UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := s.ensureProjectLocal(t.Context(), project, auth.Principal{ID: consent}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE pipelines SET actor_id=$1 WHERE id=$2`, consent, expiredPipeline); err != nil {
		t.Fatal(err)
	}
	if allowed, err := s.pipelineCreatorAuthorized(t.Context(), consent, project); err != nil || !allowed {
		t.Fatal("consent was not initially valid", allowed, err)
	}
	request(t, s, "POST", "/capture/", "", captureBody(ingest, "consent-expiry-event"), 200)
	if _, err := s.db.Exec(`UPDATE webhook_deliveries SET next_attempt_ms=next_attempt_ms-1 WHERE pipeline_id=$1`, expiredPipeline); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute)
	var revoked sql.NullString
	if err := s.control.QueryRow(`SELECT revoked_at FROM access_keys WHERE id=$1`, consent).Scan(&revoked); err != nil || revoked.Valid {
		t.Fatal("fixture must exercise expired consent before pruning revokes its principal", err)
	}
	if _, err := s.loadCredentialActor(t.Context(), consent); !errors.Is(err, errInteractiveGrant) {
		t.Fatal("expired consent did not produce the domain authorization error", err)
	}
	var destinations []string
	s.pipelines.webhookClient = &http.Client{Transport: webhookRoundTrip(func(r *http.Request) (*http.Response, error) {
		destinations = append(destinations, r.URL.Host)
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(""))}, nil
	})}
	if progress, err := s.stepWebhook(t.Context()); !progress || err != nil {
		t.Fatal("expired creator stayed a transient queue error", progress, err)
	}
	var state, code string
	if err := s.db.QueryRow(`SELECT state,error_code FROM webhook_deliveries WHERE pipeline_id=$1`, expiredPipeline).Scan(&state, &code); err != nil || state != "failed" || code != "creator_unauthorized" {
		t.Fatal("expired consent delivery was not marked unauthorized", state, code, err)
	}
	if len(destinations) != 0 {
		t.Fatal("expired consent sent a webhook", destinations)
	}
	if progress, err := s.stepWebhook(t.Context()); !progress || err != nil {
		t.Fatal("expired creator blocked healthy destination", progress, err)
	}
	if len(destinations) != 1 || destinations[0] != "healthy.example.test" {
		t.Fatal("healthy destination did not advance after expired creator", destinations)
	}
	if err := s.db.QueryRow(`SELECT state FROM webhook_deliveries WHERE pipeline_id=$1`, healthy).Scan(&state); err != nil || state != "delivered" {
		t.Fatal("healthy delivery did not complete", state, err)
	}
}
