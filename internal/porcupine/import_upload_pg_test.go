package porcupine

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/importer"
	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/output"
)

func TestDuckLakeS3SharedPostgresHostedImportUploads(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	fixture := nativeS3Fixture(t)
	dsn := sharedPostgresDSN(t)
	start := func() *sharedJourneyNode {
		t.Helper()
		lake := lakeapi.DuckLakeConfig{
			Directory: t.TempDir(), PostgresURL: dsn, S3: &fixture.storage, MemoryLimitMB: 128,
			Threads: 1, MaxOpenProjects: 2, MaintenanceInterval: 24 * time.Hour,
			CleanupGrace: 10 * 24 * time.Hour, RestoreWindow: 24 * time.Hour,
			RestoreMaxReaderTime: time.Hour, RestoreSafetyMargin: time.Hour,
		}
		s, err := Open(Config{
			Profile: ProfileHosted, AcceptanceMode: AcceptanceModeBuffered, BootstrapToken: adminToken,
			Shared: &SharedConfig{DatabaseURL: dsn}, DuckLake: &lake, allowUnreleasedHostedFixture: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		host := httptest.NewServer(s.Handler())
		t.Cleanup(func() {
			host.Close()
			if err := s.Close(); err != nil {
				t.Error("hosted import worker close:", err)
			}
		})
		return &sharedJourneyNode{service: s, host: host}
	}
	a, b := start(), start()
	db := a.service.control
	project, _ := sharedJourneyProject(t, a.host.URL, defaultTenantID, "historical upload")
	other, _ := sharedJourneyProject(t, b.host.URL, defaultTenantID, "other project")
	native := a.service.hosted.native
	key := lakeapi.ProjectWriteKey{SourceID: native.Config.SharedSourceID, ProjectID: project}
	scope, err := native.LookupHostedProjectExisting(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	f := a.service.imports.hosted
	limits := output.PipelineDemand{References: 10, QueuedBytes: 1 << 20, MetadataRows: 100, ArtifactBytes: 1 << 20, ProofSlots: 2}
	if err := f.publisher.transaction(t.Context(), func(tx *sql.Tx) error {
		for _, v := range [][2]string{{"instance", ""}, {"tenant", defaultTenantID}, {"project", project}, {"project", other}} {
			if err := f.publisher.effects.ConfigureCapacityTx(t.Context(), tx, v[0], v[1], limits); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	_, token := sharedJourneyKey(t, a.host.URL, project, []string{"imports:write", "events:write", "events:read", "events:delete"})
	_, foreign := sharedJourneyKey(t, b.host.URL, other, []string{"imports:write", "events:write", "events:read"})

	binary := filepath.Join(t.TempDir(), "porcupine")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", binary, "./cmd/porcupine")
	build.Dir = "../../cli"
	build.Env = append(os.Environ(), "CGO_ENABLED=0")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build CLI: %v %s", err, out)
	}

	var sequence, total int64
	var completedJob string
	for _, format := range []string{"native", "posthog", "segment"} {
		if !t.Run(format, func(t *testing.T) {
			lines := bytes.Split(historicalJourney(t, format), []byte("\n"))
			data := bytes.Join(lines[:2], []byte("\n"))
			directory := t.TempDir()
			if err := os.WriteFile(filepath.Join(directory, "events.jsonl"), data, 0600); err != nil {
				t.Fatal(err)
			}
			files := []importer.ImportUploadFileSpec{{Path: "events.jsonl", Kind: "events", SizeBytes: int64(len(data)), SHA256: importTestSHA(data)}}
			var person []byte
			if format == "posthog" {
				person = []byte(`{"person_id":"hosted-person","distinct_id":"posthog-archive-browser","person_version":1,"person_distinct_id_version":1,"properties":{"campaign":"archived"},"created_at":"2020-01-01T00:00:00Z"}`)
				path := filepath.Join(directory, "persons")
				if err := os.Mkdir(path, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(path, "part.jsonl"), person, 0600); err != nil {
					t.Fatal(err)
				}
				files = append(files, importer.ImportUploadFileSpec{Path: "persons/part.jsonl", Kind: "persons", SizeBytes: int64(len(person)), SHA256: importTestSHA(person)})
			}
			body, _ := json.Marshal(map[string]any{"format": format, "idempotency_key": format, "files": files})
			base := "/api/v1/projects/" + project + "/imports"
			request := func(origin, method, path, credential string, body []byte, headers map[string]string, status int) map[string]any {
				t.Helper()
				return sharedJourneyHTTP(t, origin, method, path, credential, body, headers, status)
			}
			job := request(a.host.URL, "POST", base, token, body, nil, 201)
			id := job["id"].(string)
			completedJob = id
			path := base + "/" + id
			request(b.host.URL, "GET", path, foreign, nil, nil, 403)
			request(b.host.URL, "GET", importTestBase(other, id), foreign, nil, nil, 404)

			upload := func(origin, file string, offset int, data []byte, want int) map[string]any {
				t.Helper()
				headers := map[string]string{"Content-Type": "application/octet-stream", "Upload-Offset": strconv.Itoa(offset), "X-Chunk-SHA256": importTestSHA(data)}
				return request(origin, "PUT", path+"/files/"+file, token, data, headers, want)
			}
			eventIndex := 0
			if format == "posthog" {
				eventIndex = 1
				personFile := job["files"].([]any)[0].(map[string]any)
				if personFile["kind"] != "persons" {
					t.Fatal("persons were not sorted first")
				}
				eventFile := job["files"].([]any)[1].(map[string]any)["id"].(string)
				upload(b.host.URL, eventFile, 0, data[:12], 409)
				upload(a.host.URL, personFile["id"].(string), 0, person, 200)
				request(b.host.URL, "POST", path+"/files/"+personFile["id"].(string)+"/complete", token, nil, nil, 200)
				sequence += 2
				total++
			}
			file := job["files"].([]any)[eventIndex].(map[string]any)["id"].(string)
			split := bytes.IndexByte(data, '\n') + 10
			chunk := data[:split]
			if format == "native" {
				if _, err := db.Exec(`CREATE FUNCTION reject_cursor() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'candidate interrupted before native commit'; END $$;
CREATE TRIGGER reject_cursor BEFORE INSERT ON pipeline_effect_streams FOR EACH ROW EXECUTE FUNCTION reject_cursor()`); err != nil {
					t.Fatal(err)
				}
				upload(a.host.URL, file, 0, chunk, 503)
				before, err := native.PendingNativeEffects(t.Context(), scope, 0, 1)
				if err != nil || before.NativeHighWater != 0 {
					t.Fatalf("precommit failure wrote native evidence: %+v %v", before, err)
				}
				if _, err := db.Exec(`DROP TRIGGER reject_cursor ON pipeline_effect_streams; DROP FUNCTION reject_cursor()`); err != nil {
					t.Fatal(err)
				}
			}
			// Force the real PG import checkpoint update to roll back after the
			// native outcome and S3 parser state are durable.
			if _, err := db.Exec(`CREATE FUNCTION reject_upload_checkpoint() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'checkpoint interrupted'; END $$;
CREATE TRIGGER reject_upload_checkpoint BEFORE UPDATE ON import_upload_files FOR EACH ROW EXECUTE FUNCTION reject_upload_checkpoint()`); err != nil {
				t.Fatal(err)
			}
			upload(a.host.URL, file, 0, chunk, 503)
			original, err := native.PendingNativeEffects(t.Context(), scope, sequence, 1)
			if err != nil || len(original.Evidence) != 1 || original.Evidence[0].Import.Delta.Accepted != 1 || original.AcknowledgedThrough != sequence {
				t.Fatalf("missing original native outcome: %+v %v", original, err)
			}
			var offset int64
			if err := db.QueryRow(`SELECT offset_bytes FROM import_upload_files WHERE id=$1`, file).Scan(&offset); err != nil || offset != 0 {
				t.Fatalf("failed checkpoint advanced PG: %d %v", offset, err)
			}
			if _, err := db.Exec(`DROP TRIGGER reject_upload_checkpoint ON import_upload_files; DROP FUNCTION reject_upload_checkpoint()`); err != nil {
				t.Fatal(err)
			}
			if err := b.service.hosted.flusher.RecoverProject(t.Context(), key); err != nil {
				t.Fatal("cold reconciliation could not publish the upload checkpoint", err)
			}
			recovered := request(b.host.URL, "GET", path, token, nil, nil, 200)
			replayed := upload(b.host.URL, file, 0, chunk, 200)
			changed := bytes.Clone(chunk)
			changed[0] ^= 1
			upload(b.host.URL, file, 0, changed, 409)
			if recovered["counts"].(map[string]any)["accepted"] != float64(1+eventIndex) || replayed["counts"].(map[string]any)["duplicates"] != float64(0) {
				t.Fatalf("retry reclassified the original outcome: %v %v", recovered, replayed)
			}
			if err := db.QueryRow(`SELECT offset_bytes FROM import_upload_files WHERE id=$1`, file).Scan(&offset); err != nil || offset != int64(split) {
				t.Fatalf("PG progress after recovery: %d %v", offset, err)
			}

			if format == "native" {
				upload(a.host.URL, file, split, data[split:], 200)
				if _, err := db.Exec(`CREATE FUNCTION reject_eof() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'EOF checkpoint interrupted'; END $$;
CREATE TRIGGER reject_eof BEFORE UPDATE ON import_upload_files FOR EACH ROW WHEN (NEW.state='completed') EXECUTE FUNCTION reject_eof()`); err != nil {
					t.Fatal(err)
				}
				request(a.host.URL, "POST", path+"/files/"+file+"/complete", token, nil, nil, 503)
				pending, err := native.PendingNativeEffects(t.Context(), scope, sequence+2, 1)
				if err != nil || len(pending.Evidence) != 1 || pending.Evidence[0].Import.Key.Phase != "finalize" || pending.Evidence[0].AcceptedImport != 1 {
					t.Fatalf("missing EOF outcome: %+v %v", pending, err)
				}
				if _, err := db.Exec(`DROP TRIGGER reject_eof ON import_upload_files; DROP FUNCTION reject_eof()`); err != nil {
					t.Fatal(err)
				}
				request(b.host.URL, "POST", path+"/files/"+file+"/complete", token, nil, nil, 200)
				request(b.host.URL, "POST", path+"/files/"+file+"/complete", token, nil, nil, 200)
			}

			cmd := exec.CommandContext(t.Context(), binary, "--url", b.host.URL, "import", "--project", project, "--format", format, "--resume", id, directory)
			cmd.Env = append(os.Environ(), "PORCUPINE_API_KEY="+token)
			var stdout, stderr bytes.Buffer
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			if err := cmd.Run(); err != nil {
				t.Fatalf("resume CLI: %v %s", err, stderr.String())
			}
			var completed importer.ImportUploadJob
			if err := json.Unmarshal(stdout.Bytes(), &completed); err != nil || completed.State != "completed" || completed.Counts.Accepted != int64(2+eventIndex) || completed.Counts.Duplicates != 0 || completed.Delivery == nil || !completed.Delivery.Ready {
				t.Fatalf("CLI completion: %s %v", stdout.String(), err)
			}

			next, err := native.PendingNativeEffects(t.Context(), scope, sequence, 3)
			if err != nil || len(next.Evidence) != 3 || next.Evidence[0].ProofFingerprint != original.Evidence[0].ProofFingerprint || next.Evidence[2].Import.Key.Phase != "finalize" || next.Evidence[2].Import.Delta.Accepted != 1 {
				t.Fatalf("original proof or EOF outcome changed: %+v %v", next, err)
			}
			sequence += 3
			total += 2
		}) {
			return
		}
	}
	var imported, activeParsers int64
	if err := db.QueryRow(`SELECT coalesce(sum(import_events),0) FROM usage_rollups WHERE tenant_id=$1`, defaultTenantID).Scan(&imported); err != nil || imported != total {
		t.Fatalf("usage = %d, want %d: %v", imported, total, err)
	}
	if err := checkSharedImportMetadata(t.Context(), db); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM import_upload_files WHERE parser_key<>''`).Scan(&activeParsers); err != nil || activeParsers != 0 {
		t.Fatalf("completed jobs kept active parsers: %d %v", activeParsers, err)
	}
	rowCount := func(origin, p, credential string) any {
		t.Helper()
		result := sharedJourneyQuery(t, origin, p, credential, "SELECT count(*) AS n FROM events WHERE is_imported=1", 200)
		return result["result"].(map[string]any)["data"].([]any)[0].(map[string]any)["n"]
	}
	if got := rowCount(b.host.URL, project, token); got != "3" {
		t.Fatalf("queryable history count = %v", got)
	}
	if got := rowCount(a.host.URL, other, foreign); got != "0" {
		t.Fatalf("other project count = %v", got)
	}
	var jobs int
	if err := db.QueryRow(`SELECT count(*) FROM pipeline_effect_jobs`).Scan(&jobs); err != nil || jobs != 0 {
		t.Fatalf("historical import created output work: %d %v", jobs, err)
	}

	// The upload path on another worker also settles a native mutation proof
	// with the same publisher, including the webhook cancellation callback.
	observed := sharedJourneyQuery(t, b.host.URL, project, token, "SELECT uuid FROM events WHERE is_imported=1 ORDER BY id LIMIT 1", 200)
	uuid := observed["result"].(map[string]any)["data"].([]any)[0].(map[string]any)["uuid"].(string)
	mutation := sharedJourneyHTTP(t, a.host.URL, "DELETE", "/api/v1/projects/"+project+"/events/"+uuid, token,
		[]byte(`{"expected_revision":"0","idempotency_key":"recover-mutation-through-upload"}`), nil, 202)
	if _, err := db.Exec(`CREATE FUNCTION reject_mutation_settlement() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'mutation settlement interrupted'; END $$;
CREATE TRIGGER reject_mutation_settlement BEFORE UPDATE ON hosted_mutation_intents FOR EACH ROW WHEN (NEW.state='applied') EXECUTE FUNCTION reject_mutation_settlement()`); err != nil {
		t.Fatal(err)
	}
	if err := a.service.hosted.flusher.RecoverProject(t.Context(), key); err == nil {
		t.Fatal("mutation settlement unexpectedly returned success")
	}
	proof, err := native.PendingNativeEffects(t.Context(), scope, sequence, 1)
	if err != nil || len(proof.Evidence) != 1 || proof.Evidence[0].Kind != lakeapi.NativeCommitMutation {
		t.Fatalf("missing unpublished mutation proof: %+v %v", proof, err)
	}
	if _, err := db.Exec(`DROP TRIGGER reject_mutation_settlement ON hosted_mutation_intents; DROP FUNCTION reject_mutation_settlement()`); err != nil {
		t.Fatal(err)
	}
	sharedJourneyHTTP(t, b.host.URL, "GET", "/api/v1/projects/"+project+"/imports/"+completedJob, token, nil, nil, 200)
	status := sharedJourneyHTTP(t, b.host.URL, "GET", "/api/v1/projects/"+project+"/event-mutations/"+mutation["mutation_id"].(string), token, nil, nil, 200)
	if status["state"] != "applied" {
		t.Fatalf("the upload worker did not settle the mutation: %v", status)
	}
	sequence++

	// Leave the real upload cursor unpublished, fence the project, then let a
	// cold worker settle both the native proof and the same upload checkpoint.
	data := historicalJourney(t, "native")
	spec, err := json.Marshal(map[string]any{
		"format": "native", "idempotency_key": "recover-on-deletion",
		"files": []importer.ImportUploadFileSpec{
			{Path: "events.jsonl", Kind: "events", SizeBytes: int64(len(data)), SHA256: importTestSHA(data)},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	base := "/api/v1/projects/" + project + "/imports"
	job := sharedJourneyHTTP(t, a.host.URL, "POST", base, token, spec, nil, 201)
	file := job["files"].([]any)[0].(map[string]any)["id"].(string)
	if _, err := db.Exec(`CREATE FUNCTION reject_deletion_checkpoint() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'checkpoint interrupted'; END $$;
CREATE TRIGGER reject_deletion_checkpoint BEFORE UPDATE ON import_upload_files FOR EACH ROW EXECUTE FUNCTION reject_deletion_checkpoint()`); err != nil {
		t.Fatal(err)
	}
	sharedJourneyHTTP(t, a.host.URL, "PUT", base+"/"+job["id"].(string)+"/files/"+file, token, data,
		map[string]string{"Content-Type": "application/octet-stream", "Upload-Offset": "0", "X-Chunk-SHA256": importTestSHA(data)}, 503)
	if _, err := db.Exec(`DROP TRIGGER reject_deletion_checkpoint ON import_upload_files; DROP FUNCTION reject_deletion_checkpoint()`); err != nil {
		t.Fatal(err)
	}
	pending, err := native.PendingNativeEffects(t.Context(), scope, sequence, 1)
	if err != nil || len(pending.Evidence) != 1 || pending.Evidence[0].Kind != lakeapi.NativeCommitImport {
		t.Fatalf("expected an unpublished import proof: %+v, %v", pending, err)
	}
	if err := a.service.beginControlProjectDeletion(t.Context(), project); err != nil {
		t.Fatal(err)
	}
	if err := b.service.imports.hosted.sealDeletedProject(t.Context(), key); err != nil {
		t.Fatal("deletion seal failed to recover the upload checkpoint", err)
	}
	var offset, final int64
	err = db.QueryRow(`SELECT f.offset_bytes,s.final_sequence FROM import_upload_files f
JOIN hosted_project_seals s ON s.project_id=$1 WHERE f.id=$2`, project, file).Scan(&offset, &final)
	if err != nil || offset != int64(len(data)) || final != pending.Evidence[0].Sequence {
		t.Fatalf("seal and import checkpoint differ from committed proof: %d, %d, %v", offset, final, err)
	}

}
