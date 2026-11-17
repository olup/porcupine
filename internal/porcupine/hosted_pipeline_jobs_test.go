package porcupine

import (
	"bytes"
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/auth"
	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/output"
)

func TestSharedHostedPipelineAuthorityExpiresAndRevalidates(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db, _ := sharedControlFixture(t)
	queries, _ := normalizeInteractiveQueries(nil)
	if err := initializeSharedControl(ctx, db, adminToken, queries, false); err != nil {
		t.Fatal(err)
	}
	oauth, err := normalizeOAuthConfig(OAuthConfig{Issuer: "http://127.0.0.1:8081", Email: &EmailConfig{SendCode: func(context.Context, string, string) error { return nil }}})
	if err != nil {
		t.Fatal(err)
	}
	s := &Service{control: db, eventRules: &eventRules{clock: time.Now}, oauth: oauth, resources: newResourceRuntime(oauth.issuer)}
	scopes := []string{"pipelines:write", "events:read"}
	now := time.Now()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `INSERT INTO projects(id,tenant_id,name,created_at) VALUES('project','default','Fixture','now')`); err != nil {
		t.Fatal(err)
	}
	human := EmailHuman{ID: "human", Email: "pipeline-owner@example.test", CreatedAt: now.Format(time.RFC3339Nano), Permissions: scopes, Projects: []string{"project"}, Grants: []HumanProjectGrant{{ProjectID: "project", Permissions: scopes}}}
	if err = insertEmailHuman(ctx, tx, human); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO access_keys(id,name,token_hash,permissions,projects,created_at)
VALUES ('client','client','hash-client','["pipelines:write","events:read"]','["project"]','now'),
('consent','consent','hash-consent','["pipelines:write","events:read"]','["project"]','now')`); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO oauth_clients(client_id,secret_hash,grant_types,redirect_uris,token_endpoint_auth_method)
VALUES ('client','not-a-secret','["authorization_code","refresh_token"]','["https://client.example.test/callback"]','none')`); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO oauth_interactive_grants(id,principal_id,human_id,client_id,resource,created_at_ms,expires_at_ms)
VALUES ('grant','consent','human','client',$1,$2,$3)`, s.resources.mcpResource, now.UnixMilli(), now.Add(time.Minute).UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}

	resolve := func(tenant, creator string, expected bool) {
		t.Helper()
		tx, err := db.BeginTx(ctx, &sql.TxOptions{})
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		actual, err := (hostedPipelineAuthority{service: s}).ResolveCreatorTx(ctx, tx, tenant, "project", creator)
		if err != nil || actual != expected {
			t.Fatalf("resolve %s for %s: got %t want %t: %v", creator, tenant, actual, expected, err)
		}
	}
	resolve("default", "consent", true)
	coordinator, err := lakeapi.NewPostgresProjectCoordinator(db, lakeapi.DefaultProjectCoordinatorConfig())
	if err != nil {
		t.Fatal(err)
	}
	key := lakeapi.ProjectWriteKey{SourceID: "source", ProjectID: "project"}
	flusher := &hostedLiveFlusher{
		coordinator: coordinator,
		native:      &lakeapi.DuckLakeStore{Config: lakeapi.DuckLakeConfig{SharedSourceID: key.SourceID}},
		publisher:   &hostedEffectPublisher{control: db},
	}
	config := &output.PipelineConfig{Key: bytes.Repeat([]byte{1}, 32)}
	execution := hostedPipelineExecution{
		deleteGate:     func(context.Context, *sql.Tx, lakeapi.HostedProjectScope, string) (bool, error) { return true, nil },
		beginAttemptTx: func(context.Context, *sql.Tx, string, string) error { t.Error("unexpected send"); return nil },
		endAttempt:     func(context.Context, string, string) error { t.Error("unexpected send"); return nil },
	}
	if err = s.configureHostedPipelines(config, flusher, execution); err != nil {
		t.Fatal(err)
	}
	if err = s.pipelines.Initialize(ctx); err != nil {
		t.Fatal(err)
	}
	pipeline, err := s.pipelines.Define(ctx, "project", output.PipelineRequest{Kind: "webhook", Name: "hosted", Webhook: &output.WebhookPipelineSpec{Endpoint: "https://hooks.example.test/events", IncludeEvents: []string{"checkout"}}}, auth.Principal{ID: "consent"})
	if err != nil {
		t.Fatal(err)
	}
	session, err := coordinator.Acquire(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	scope := lakeapi.HostedProjectScope{Key: key, CatalogID: "catalog", StreamID: "stream"}
	_, routing, err := flusher.publisher.plan(ctx, "default", scope)
	if releaseErr := session.Release(); releaseErr != nil {
		t.Fatal(releaseErr)
	}
	if err != nil {
		t.Fatal(err)
	}
	if len(routing.Targets) != 1 || routing.Targets[0].PipelineID != pipeline.ID || !routing.Targets[0].Rule.Matches("checkout") || routing.Targets[0].Rule.Matches("login") {
		t.Fatal("live publisher selected a different hosted definition")
	}

	resolve("other", "consent", false)
	resolve("default", "human", false)
	if _, err = db.ExecContext(ctx, `UPDATE oauth_interactive_grants SET expires_at_ms=$1 WHERE id='grant'`, now.Add(-time.Second).UnixMilli()); err != nil {
		t.Fatal(err)
	}
	resolve("default", "consent", false)
	if _, err = db.ExecContext(ctx, `UPDATE oauth_interactive_grants SET expires_at_ms=$1 WHERE id='grant'`, now.Add(time.Minute).UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if _, err = db.ExecContext(ctx, `UPDATE access_keys SET permissions='["events:read"]' WHERE id='client'`); err != nil {
		t.Fatal(err)
	}
	resolve("default", "consent", false)
}
