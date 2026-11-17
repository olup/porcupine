package porcupine

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

func sharedControlFixture(t *testing.T) (*sql.DB, string) {
	t.Helper()
	dsn := sharedPostgresDSN(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db, err := openSharedControl(ctx, SharedConfig{DatabaseURL: dsn})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db, dsn
}

func TestSharedControlBootstrapIsAtomicAndConcurrent(t *testing.T) {
	t.Parallel()
	db, dsn := sharedControlFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	config, _ := normalizeInteractiveQueries(nil)
	if err := initializeSharedControl(ctx, db, "short", config, false); err == nil {
		t.Fatal("first shared initialization accepted an unusable bootstrap")
	}
	var tables int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM information_schema.tables WHERE table_schema=current_schema()`).Scan(&tables); err != nil || tables != 0 {
		t.Fatalf("failed bootstrap left partial schema: tables=%d error=%v", tables, err)
	}
	second, err := openSharedControl(ctx, SharedConfig{DatabaseURL: dsn})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	var group sync.WaitGroup
	failures := make(chan error, 6)
	for i := range 6 {
		group.Go(func() {
			connection := db
			if i%2 == 1 {
				connection = second
			}
			failures <- initializeSharedControl(ctx, connection, adminToken, config, false)
		})
	}
	group.Wait()
	close(failures)
	for err := range failures {
		if err != nil {
			t.Fatal(err)
		}
	}
	var admins int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM access_keys WHERE name='bootstrap admin'`).Scan(&admins); err != nil || admins != 1 {
		t.Fatalf("shared bootstrap was not unique: %d %v", admins, err)
	}
	var source, principal string
	if err := second.QueryRowContext(ctx, `SELECT value FROM metadata WHERE key='shared_source_id'`).Scan(&source); err != nil || source == "" {
		t.Fatal("missing shared analytical namespace", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT value FROM metadata WHERE key='oauth_bootstrap_id'`).Scan(&principal); err != nil || principal == "" {
		t.Fatal("missing stable bootstrap principal", err)
	}
	if err := initializeSharedControl(ctx, second, "", config, false); err != nil {
		t.Fatal("restart incorrectly required bootstrap secret", err)
	}
	var retained string
	if err := db.QueryRowContext(ctx, `SELECT value FROM metadata WHERE key='shared_source_id'`).Scan(&retained); err != nil || retained != source {
		t.Fatal("restart changed shared source identity", err)
	}
	var tokenHash string
	if err := db.QueryRowContext(ctx, `SELECT token_hash FROM access_keys WHERE id=$1`, principal).Scan(&tokenHash); err != nil || tokenHash != hash(adminToken) {
		t.Fatal("shared bootstrap secret was not hashed", err)
	}
}

func TestSharedControlSchemaMatchesMetadataAndExcludesPayloads(t *testing.T) {
	t.Parallel()
	db, _ := sharedControlFixture(t)
	ctx := context.Background()
	config, _ := normalizeInteractiveQueries(nil)
	if err := initializeSharedControl(ctx, db, adminToken, config, false); err != nil {
		t.Fatal(err)
	}
	local := service(t, 20)
	for _, table := range []string{"tenants", "tenant_usage", "projects", "project_deletions", "access_keys", "metadata", "cookieless_keys", "oauth_clients", "service_api_keys", "oauth_tokens", "oauth_authorization_requests", "oauth_interactive_grants", "oauth_authorization_codes", "oauth_refresh_tokens", "oauth_interactive_access", "oidc_identities", "email_humans"} {
		t.Run(table, func(t *testing.T) {
			rows, err := local.control.QueryContext(ctx, `SELECT name FROM pragma_table_info($1) ORDER BY name`, table)
			if err != nil {
				t.Fatal(err)
			}
			var columns []string
			for rows.Next() {
				var name string
				if err := rows.Scan(&name); err != nil {
					t.Fatal(err)
				}
				columns = append(columns, name)
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			rows.Close()
			rows, err = db.QueryContext(ctx, `SELECT column_name FROM information_schema.columns WHERE table_schema=current_schema() AND table_name=$1 ORDER BY column_name`, table)
			if err != nil {
				t.Fatal(err)
			}
			var sharedColumns []string
			for rows.Next() {
				var name string
				if err := rows.Scan(&name); err != nil {
					t.Fatal(err)
				}
				sharedColumns = append(sharedColumns, name)
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			rows.Close()
			if len(columns) == 0 || !slices.Equal(columns, sharedColumns) {
				t.Fatalf("control schema drift: SQLite=%v PostgreSQL=%v", columns, sharedColumns)
			}
		})
	}
	for _, table := range []string{"events", "delivery_ready", "identity_profiles", "identity_links", "event_heads", "mutation_outbox", "pipelines", "webhook_deliveries", "pipeline_s3_runs"} {
		var exists bool
		if err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema=current_schema() AND table_name=$1)`, table).Scan(&exists); err != nil || exists {
			t.Fatalf("payload table leaked into shared control: %s %v", table, err)
		}
	}
	for _, table := range []string{"export_jobs", "export_files", "hosted_pipeline_configs", "pipeline_effect_jobs", "pipeline_effect_assemblies"} {
		var columns, payloads int
		err := db.QueryRowContext(ctx, `SELECT count(*),count(*) FILTER (WHERE column_name IN ('payload','body','event_data'))
FROM information_schema.columns WHERE table_schema=current_schema() AND table_name=$1`, table).Scan(&columns, &payloads)
		if err != nil || columns == 0 || payloads != 0 {
			t.Fatalf("invalid shared job metadata schema: %s columns=%d payloads=%d error=%v", table, columns, payloads, err)
		}
	}
	if err := checkSharedImportMetadata(ctx, db); err != nil {
		t.Fatal(err)
	}
	t.Run("rejects payload column", func(t *testing.T) {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		if _, err := tx.ExecContext(ctx, `ALTER TABLE import_upload_files ADD COLUMN hash_state bytea`); err != nil {
			t.Fatal(err)
		}
		if err := checkSharedImportMetadata(ctx, tx); err == nil || !strings.Contains(err.Error(), "import_upload_files.hash_state bytea") {
			t.Fatalf("serialized hash column escaped metadata validation: %v", err)
		}
	})
	for _, value := range []bool{false, true} {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE tenants SET ingestion_allowed=$1 WHERE id='default'`, value); err != nil {
			t.Fatal("integer flag binding failed inside transaction", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		var flag bool
		if err := db.QueryRowContext(ctx, `SELECT ingestion_allowed FROM tenants WHERE id='default'`).Scan(&flag); err != nil || flag != value {
			t.Fatalf("integer flag roundtrip failed: %t %v", flag, err)
		}
	}
}

// Keep the allowlist independent of the migration. Both names and types must
// match so an added payload field fails even if it uses an ordinary text type.
func checkSharedImportMetadata(ctx context.Context, db oauthQueryer) error {
	allowed := map[string]map[string]string{}
	for table, typedColumns := range map[string]map[string]string{
		"import_uploads": {
			"text":   "id project_id actor_id source_id format idempotency_hash request_hash state created_at updated_at completed_at error",
			"bigint": "delivery_cursor",
		},
		"import_upload_files": {
			"text":    "id job_id path kind sha256 state parser_key parser_version parser_sha256 last_sha256 error",
			"integer": "ordinal",
			"bigint":  "size_bytes offset_bytes line_number generation parser_bytes last_offset last_size rows accepted duplicates ignored unresolved invalid",
		},
		"import_upload_samples": {
			"text":   "job_id file_id reason",
			"bigint": "row_number",
		},
	} {
		allowed[table] = map[string]string{}
		for typ, names := range typedColumns {
			for _, name := range strings.Fields(names) {
				allowed[table][name] = typ
			}
		}
	}
	columns, err := db.QueryContext(ctx, `SELECT table_name,column_name,data_type FROM information_schema.columns
WHERE table_schema=current_schema() AND table_name IN ('import_uploads','import_upload_files','import_upload_samples')
ORDER BY table_name,column_name`)
	if err != nil {
		return err
	}
	defer columns.Close()
	for columns.Next() {
		var table, name, typ string
		if err := columns.Scan(&table, &name, &typ); err != nil {
			return err
		}
		if allowed[table][name] != typ {
			return fmt.Errorf("unexpected hosted import column %s.%s %s", table, name, typ)
		}
		delete(allowed[table], name)
	}
	if err := columns.Err(); err != nil {
		return err
	}
	for table, remaining := range allowed {
		if len(remaining) != 0 {
			return fmt.Errorf("missing hosted import metadata columns in %s: %v", table, remaining)
		}
	}
	return nil
}

func TestSharedControlRevokesDependentCredentials(t *testing.T) {
	t.Parallel()
	db, _ := sharedControlFixture(t)
	ctx := context.Background()
	config, _ := normalizeInteractiveQueries(nil)
	if err := initializeSharedControl(ctx, db, adminToken, config, false); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"machine", "service-key", "human", "grant-one", "grant-two"} {
		if _, err := db.ExecContext(ctx, `INSERT INTO access_keys(id,name,token_hash,permissions,projects,created_at) VALUES($1,$1,$1,'["events:read"]','["project"]','now')`, id); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO oauth_clients(client_id,secret_hash) VALUES('machine','hashed'); INSERT INTO service_api_keys(principal_id,client_id,audience) VALUES('service-key','machine','api'); INSERT INTO email_humans(principal_id,email,project_grants) VALUES('human','human@example.test','[{"project_id":"project","permissions":["events:read"]}]')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO oauth_interactive_grants(id,principal_id,human_id,client_id,resource,created_at_ms,expires_at_ms) VALUES('grant-one','grant-one','human','machine','api',0,1),('grant-two','grant-two','human','machine','api',0,1)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE email_humans SET project_grants='[]' WHERE principal_id='human'`); err != nil {
		t.Fatal(err)
	}
	var revoked int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM access_keys WHERE id IN('grant-one','grant-two') AND revoked_at IS NOT NULL`).Scan(&revoked); err != nil || revoked != 2 {
		t.Fatalf("email grant change retained consent credentials: %d %v", revoked, err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE access_keys SET source_id='changed-producer' WHERE id='machine'`); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM access_keys WHERE id='service-key' AND revoked_at IS NOT NULL`).Scan(&revoked); err != nil || revoked != 1 {
		t.Fatalf("machine grant change retained service key: %d %v", revoked, err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE access_keys SET revoked_at='revoked-again' WHERE id='machine'`); err != nil {
		t.Fatal("revocation triggers are not idempotent", err)
	}
}

func TestHostedControlRejectsLegacyOwnerRoutedDatabaseWithoutMigrating(t *testing.T) {
	t.Parallel()
	db, _ := sharedControlFixture(t)
	ctx := t.Context()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, sharedControlSteps[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO metadata(key,value) VALUES('schema_version','1'),('initialized','1')`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	queries, _ := normalizeInteractiveQueries(nil)
	if err := initializeSharedControl(ctx, db, adminToken, queries, false); err == nil || !strings.Contains(err.Error(), "requires a fresh PostgreSQL control database") {
		t.Fatal("previous deployment was not rejected", err)
	}
	var version string
	var tablesRetained bool
	if err := db.QueryRowContext(ctx, `SELECT value,to_regclass('shared_workers') IS NOT NULL FROM metadata WHERE key='schema_version'`).Scan(&version, &tablesRetained); err != nil || version != "1" || !tablesRetained {
		t.Fatal("rejected database was modified", version, tablesRetained, err)
	}
}
