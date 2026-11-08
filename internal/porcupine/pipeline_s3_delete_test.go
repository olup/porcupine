package porcupine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestPipelineS3DeletionPreservesCommittedManifestFiles(t *testing.T) {
	for _, scenario := range []string{"matching_commit", "absent_commit", "unavailable_commit", "conflicting_commit", "running"} {
		t.Run(scenario, func(t *testing.T) {
			var deletes, gets atomic.Int32
			checkResume := make(chan func(), 1)
			checkpoint := `{"version":1,"files":[{"key":"pipeline/part.jsonl.gz"}]}`
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "GET" {
					gets.Add(1)
					switch scenario {
					case "matching_commit":
						(<-checkResume)()
						fmt.Fprint(w, checkpoint)
					case "conflicting_commit":
						fmt.Fprint(w, `{"files":["other"]}`)
					case "unavailable_commit":
						w.WriteHeader(403)
						fmt.Fprint(w, `<Error><Code>AccessDenied</Code><Message>secret-value</Message></Error>`)
					default:
						w.WriteHeader(404)
						fmt.Fprint(w, `<Error><Code>NoSuchKey</Code></Error>`)
					}
					return
				}
				if r.Method == "DELETE" {
					deletes.Add(1)
					w.WriteHeader(204)
					return
				}
				t.Error("unexpected S3 call")
				w.WriteHeader(400)
			}))
			defer server.Close()
			s, err := Open(Config{DBPath: filepath.Join(t.TempDir(), "pipeline.db"), BootstrapToken: adminToken, Pipelines: &PipelineConfig{Key: bytes.Repeat([]byte{7}, 32), AllowedS3Endpoints: []string{server.URL}}})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			s.stopPipelines()
			ch, err := newClickHouse(ClickHouseConfig{URL: "http://127.0.0.1:1", Database: "test_db"})
			if err != nil {
				t.Fatal(err)
			}
			configureAnalyticsTest(s, ch)
			a, _ := project(t, s)
			b, _ := project(t, s)
			_, other := key(t, s, []string{"pipelines:write", "events:read"}, []string{b})
			spec := testS3Spec()
			spec.Destination.Endpoint = server.URL
			body, _ := json.Marshal(PipelineCreateBody{Name: "delete recovery", Kind: "s3", S3: &spec, Credentials: PipelineCredentials{AccessKeyID: "test-id", SecretAccessKey: "test-secret"}})
			base := "/api/v1/projects/" + a + "/pipelines"
			created := request(t, s, "POST", base, adminToken, string(body), 201)
			id := created["id"].(string)
			start, _ := time.Parse(time.RFC3339, spec.StartAt)
			run := newID()
			state := "queued"
			if scenario == "running" {
				state = "running"
			}
			_, err = s.db.Exec(`INSERT INTO pipeline_s3_runs(id,pipeline_id,window_start_ms,window_end_ms,state,created_at_ms,manifest) VALUES(?,?,?,?,?,?,?)`, run, id, start.UnixMilli(), start.Add(time.Minute).UnixMilli(), state, start.UnixMilli(), checkpoint)
			if err != nil {
				t.Fatal(err)
			}
			_, err = s.db.Exec(`INSERT INTO pipeline_s3_files(run_id,object_key,upload_id) VALUES(?,?,?)`, run, "pipeline/part.jsonl.gz", "known-upload")
			if err != nil {
				t.Fatal(err)
			}
			request(t, s, "DELETE", base+"/"+id, other, "", 403)
			if deletes.Load() != 0 || gets.Load() != 0 {
				t.Fatal("foreign project touched destination")
			}
			status := 204
			if scenario == "unavailable_commit" || scenario == "conflicting_commit" || scenario == "running" {
				status = 503
			}
			if scenario == "matching_commit" {
				checkResume <- func() {
					request(t, s, "POST", base+"/"+id+"/resume", adminToken, "", 409)
				}
			}
			request(t, s, "DELETE", base+"/"+id, adminToken, "", status)
			var count int
			if s.db.QueryRow(`SELECT count(*) FROM pipelines WHERE id=?`, id).Scan(&count) != nil {
				t.Fatal("query retained pipeline")
			}
			if status == 204 && count != 0 {
				t.Fatal("deletion did not finish")
			}
			if status == 503 && count != 1 {
				t.Fatal("ambiguous cleanup lost durable checkpoints")
			}
			want := int32(0)
			if scenario == "absent_commit" {
				want = 2
			}
			if deletes.Load() != want {
				t.Fatalf("object delete/abort calls %d want %d", deletes.Load(), want)
			}
			if scenario == "matching_commit" {
				if err = s.deleteS3Pipeline(context.Background(), a, id); err == nil {
					t.Fatal("repeated delete should report missing pipeline")
				}
			}
		})
	}
}
