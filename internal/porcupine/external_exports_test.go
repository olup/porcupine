package porcupine

import (
	"context"
	"fmt"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/exportstore"
	"github.com/olup/porcupine/internal/output"
)

func TestExternalExportAuthorityExpiresBeforePublication(t *testing.T) {
	t.Parallel()
	for _, expired := range []bool{false, true} {
		t.Run(fmt.Sprint(expired), func(t *testing.T) {
			f := newExternalAuthFixture(t)
			s := f.s
			enableTestExports(t, s, 10)
			fake := &exportTestS3{}
			server := httptest.NewServer(fake)
			defer server.Close()
			store, err := exportstore.New(t.Context(), exportstore.Config{Region: "us-east-1", Bucket: "external-results", Prefix: "isolated", Endpoint: server.URL, AccessKeyID: "key", SecretAccessKey: "secret", UsePathStyle: true})
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			config := s.exports.Config()
			config.S3 = exportstore.Config{Region: "us-east-1", Bucket: "external-results", Prefix: "isolated", Endpoint: server.URL, AccessKeyID: "key", SecretAccessKey: "secret", UsePathStyle: true}
			if err := s.configureExports(&config); err != nil {
				t.Fatal(err)
			}
			response := request(t, s, "POST", "/api/v1/projects", f.operator(t), `{"name":"test"}`, 201)
			id := response["project"].(map[string]any)["id"].(string)
			request(t, s, "POST", "/capture/", "", fmt.Sprintf(`{"api_key":%q,"event":"purchase","uuid":"one","distinct_id":"person"}`, response["ingest_token"]), 200)
			duckLakeJourneyDrain(t, s)
			reader := f.token(t, "reader", defaultTenantID, "events:read", s.resources.apiResource, id)
			base := "/api/v1/projects/" + id + "/exports"
			body := `{"idempotency_key":"bounded","sql":"SELECT uuid,event FROM events"}`
			created := request(t, s, "POST", base, reader, body, 202)
			job, err := s.loadExport(t.Context(), id, created["id"].(string))
			if err != nil {
				t.Fatal(err)
			}
			if job.ExternalExpiresAtMS != f.clock.Add(5*time.Minute).UnixMilli() || job.ExternalTenantID != defaultTenantID {
				t.Fatalf("incorrect authority snapshot: %+v", job)
			}
			if _, err := s.loadCredentialActor(t.Context(), job.ActorID); err == nil {
				t.Fatal("external audit anchor became native authority")
			}
			if expired {
				fake.onComplete = func() { f.now.Store(f.clock.Add(6 * time.Minute).UnixMilli()) }
			}
			s.exports.RunOne(context.Background())
			current, err := s.loadExport(t.Context(), id, job.ID)
			if err != nil {
				t.Fatal(err)
			}
			if expired {
				if current.State != output.ExportExpired || current.Error != "authorization_expired" || current.CleanupPending {
					t.Fatalf("expired execution published or skipped cleanup: %+v", current)
				}
				fake.mu.Lock()
				defer fake.mu.Unlock()
				if len(fake.objects) != 0 {
					t.Fatal("expired execution retained output")
				}
				return
			}
			if current.State != output.ExportSucceeded || current.ExpiresAtMS <= job.ExternalExpiresAtMS {
				t.Fatalf("completed result lost normal retention: %+v", current)
			}
			f.now.Store(f.clock.Add(6 * time.Minute).UnixMilli())
			s.exports.RunOne(t.Context())
			fresh := f.token(t, "reader", defaultTenantID, "events:read", s.resources.apiResource, id)
			request(t, s, "GET", base+"/"+job.ID+"/files", reader, "", 401)
			files := request(t, s, "GET", base+"/"+job.ID+"/files", fresh, "", 200)
			link := files["files"].([]any)[0].(map[string]any)["url"].(string)
			parsed, err := url.Parse(link)
			if err != nil || parsed.Query().Get("X-Amz-Expires") != "300" {
				t.Fatal("fresh caller link not JWT-bound", link, err)
			}
			request(t, s, "POST", base, fresh, body, 202)
			retry, err := s.loadExport(t.Context(), id, job.ID)
			if err != nil || retry.ExternalExpiresAtMS != job.ExternalExpiresAtMS {
				t.Fatal("retry renewed finite execution authority", err)
			}
		})
	}
}

func TestExternalExportControlOutageDoesNotRevokeExecution(t *testing.T) {
	t.Parallel()
	f := newExternalAuthFixture(t)
	s := f.s
	enableTestExports(t, s, 10)
	p := request(t, s, "POST", "/api/v1/projects", f.operator(t), `{"name":"bounded job"}`, 201)["project"].(map[string]any)["id"].(string)
	reader := f.token(t, "reader", defaultTenantID, "events:read", s.resources.apiResource, p)
	created := request(t, s, "POST", "/api/v1/projects/"+p+"/exports", reader, `{"idempotency_key":"control-outage","sql":"SELECT event FROM events"}`, 202)
	job, err := s.loadExport(t.Context(), p, created["id"].(string))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.control.Close(); err != nil {
		t.Fatal(err)
	}
	s.exports.RunOne(t.Context())
	current, err := s.loadExport(t.Context(), p, job.ID)
	if err != nil || current.State != output.ExportQueued || current.SQL == "" {
		t.Fatal("retryable authority failure destroyed execution intent", current.State, current.Error, err)
	}
	f.now.Add(int64(6 * time.Minute / time.Millisecond))
}

func TestExternalExportDownloadExpiresDuringMetadataOrSigning(t *testing.T) {
	t.Parallel()
	f := newExternalAuthFixture(t)
	s := f.s
	enableTestExports(t, s, 10)
	store, err := exportstore.New(t.Context(), exportstore.Config{Region: "us-east-1", Bucket: "external-results", Prefix: "isolated", Endpoint: "http://127.0.0.1:9000", AccessKeyID: "key", SecretAccessKey: "secret", UsePathStyle: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	config := s.exports.Config()
	config.S3 = exportstore.Config{Region: "us-east-1", Bucket: "external-results", Prefix: "isolated", Endpoint: "http://127.0.0.1:9000", AccessKeyID: "key", SecretAccessKey: "secret", UsePathStyle: true}
	if err := s.configureExports(&config); err != nil {
		t.Fatal(err)
	}
	p := request(t, s, "POST", "/api/v1/projects", f.operator(t), `{"name":"two output files"}`, 201)["project"].(map[string]any)["id"].(string)
	reader := f.token(t, "reader", defaultTenantID, "events:read", s.resources.apiResource, p)
	created := request(t, s, "POST", "/api/v1/projects/"+p+"/exports", reader, `{"idempotency_key":"download-race","sql":"SELECT event FROM events"}`, 202)
	id := created["id"].(string)
	if _, err := s.db.Exec(`UPDATE export_jobs SET state='succeeded',attempt_id='complete',expires_at_ms=$1 WHERE id=$2`, f.clock.Add(time.Hour).UnixMilli(), id); err != nil {
		t.Fatal(err)
	}
	for i := range 2 {
		if _, err := s.db.Exec(`INSERT INTO export_files(job_id,attempt_id,file_index,object_key,complete) VALUES($1,'complete',$2,$3,1)`, id, i, fmt.Sprintf("jobs/%s/%d.jsonl.gz", id, i)); err != nil {
			t.Fatal(err)
		}
	}
	actor, err := s.authenticateResource(t.Context(), reader, s.resources.apiResource)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.WithValue(t.Context(), actorContextKey{}, actor)
	for _, advanceAt := range []int{2, 3} {
		t.Run(fmt.Sprint(advanceAt), func(t *testing.T) {
			originalClock := s.clock
			defer func() { s.clock = originalClock }()
			calls := 0
			s.clock = func() time.Time {
				calls++
				if calls >= advanceAt {
					return f.clock.Add(6 * time.Minute)
				}
				return f.clock
			}
			files, err := s.eventExportFiles(ctx, &ExportFilesInput{ID: p, ExportID: id, Limit: 2})
			mutationErrorStatus(t, err, 410)
			if files != nil || calls < advanceAt {
				t.Fatal("download returned URLs after expiry during metadata or between signatures")
			}
		})
	}
}
