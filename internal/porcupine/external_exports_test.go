package porcupine

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/exportstore"
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
			s.exports.store = store
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
			if _, err := s.exportActor(t.Context(), job.ActorID); err == nil {
				t.Fatal("external audit anchor became native authority")
			}
			if _, err := s.exportJobActor(t.Context(), job); err != nil {
				t.Fatal(err)
			}
			job, err = s.claimExport(t.Context(), job)
			if err != nil {
				t.Fatal(err)
			}
			if expired {
				fake.onComplete = func() { f.now.Store(f.clock.Add(6 * time.Minute).UnixMilli()) }
			}
			s.runExport(context.Background(), job)
			current, err := s.loadExport(t.Context(), id, job.ID)
			if err != nil {
				t.Fatal(err)
			}
			if expired {
				if current.State != ExportExpired || current.Error != "authorization_expired" || current.CleanupPending {
					t.Fatalf("expired execution published or skipped cleanup: %+v", current)
				}
				fake.mu.Lock()
				defer fake.mu.Unlock()
				if len(fake.objects) != 0 {
					t.Fatal("expired execution retained output")
				}
				if _, err := s.exportJobActor(t.Context(), job); !errors.Is(err, errExportAuthorizationExpired) {
					t.Fatal("expired authority survived", err)
				}
				return
			}
			if current.State != ExportSucceeded || current.ExpiresAtMS <= job.ExternalExpiresAtMS {
				t.Fatalf("completed result lost normal retention: %+v", current)
			}
			f.now.Store(f.clock.Add(6 * time.Minute).UnixMilli())
			s.expireExports(t.Context())
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
	job, err = s.claimExport(t.Context(), job)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.control.Close(); err != nil {
		t.Fatal(err)
	}
	_, authorityErr := s.exportJobActor(t.Context(), job)
	if !authenticationUnavailable(authorityErr) {
		t.Fatal("control outage became revoked execution", authorityErr)
	}
	s.failExport(t.Context(), job, authorityErr)
	current, err := s.loadExport(t.Context(), p, job.ID)
	if err != nil || current.State != ExportQueued || current.Error != "authorization_unavailable" || current.SQL == "" {
		t.Fatal("retryable authority failure destroyed execution intent", current.State, current.Error, err)
	}
	f.now.Add(int64(6 * time.Minute / time.Millisecond))
	if _, err := s.exportJobActor(t.Context(), job); !errors.Is(err, errExportAuthorizationExpired) {
		t.Fatal("dependency outage extended finite authority", err)
	}
}
