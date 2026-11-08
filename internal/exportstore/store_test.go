package exportstore

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type fakeS3 struct {
	contentType, contentEncoding string
	mu                           sync.Mutex
	parts                        [][]byte
	paths                        []string
	complete, abort, delete      int
	completeError                bool
	partError                    bool
	completedParts               []int
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.paths = append(f.paths, r.URL.Path)
	w.Header().Set("Content-Type", "application/xml")
	switch {
	case r.Method == "POST" && r.URL.Query().Has("uploads"):
		f.contentType, f.contentEncoding = r.Header.Get("Content-Type"), r.Header.Get("Content-Encoding")
		fmt.Fprint(w, `<InitiateMultipartUploadResult><Bucket>test-bucket</Bucket><Key>private/job/events.ndjson</Key><UploadId>test-upload</UploadId></InitiateMultipartUploadResult>`)
	case r.Method == "PUT":
		if f.partError {
			w.WriteHeader(403)
			fmt.Fprint(w, `<Error><Code>AccessDenied</Code><Message>secret-token at https://secret.example</Message></Error>`)
			return
		}
		body, _ := io.ReadAll(r.Body)
		f.parts = append(f.parts, body)
		w.Header().Set("ETag", fmt.Sprintf(`"part-%d"`, len(f.parts)))
	case r.Method == "POST":
		f.complete++
		if f.completeError {
			fmt.Fprint(w, `<Error><Code>InvalidPart</Code><Message>secret-token at https://secret.example</Message></Error>`)
			return
		}
		var payload struct {
			Parts []struct {
				Number int `xml:"PartNumber"`
			} `xml:"Part"`
		}
		if err := xml.NewDecoder(r.Body).Decode(&payload); err != nil {
			w.WriteHeader(400)
			return
		}
		for _, part := range payload.Parts {
			f.completedParts = append(f.completedParts, part.Number)
		}
		fmt.Fprint(w, `<CompleteMultipartUploadResult><Location>private</Location><Bucket>test-bucket</Bucket><Key>private/job/events.ndjson</Key><ETag>"complete"</ETag></CompleteMultipartUploadResult>`)
	case r.Method == "DELETE" && r.URL.Query().Has("uploadId"):
		f.abort++
		w.WriteHeader(204)
	case r.Method == "DELETE":
		f.delete++
		w.WriteHeader(204)
	default:
		w.WriteHeader(400)
	}
}
func testStore(t *testing.T, handler http.Handler) *Store {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	store, err := New(context.Background(), Config{Region: "us-east-1", Bucket: "test-bucket", Prefix: "private/", Endpoint: server.URL, AccessKeyID: "test-access", SecretAccessKey: "test-secret", UsePathStyle: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	return store
}
func begin(t *testing.T, store *Store, ctx context.Context) *Upload {
	t.Helper()
	upload, err := store.Begin(ctx, "job/events.ndjson")
	if err != nil {
		t.Fatal(err)
	}
	return upload
}
func TestMultipartBufferAndExplicitCompletion(t *testing.T) {
	fake := &fakeS3{}
	store := testStore(t, fake)
	upload := begin(t, store, context.Background())
	if upload.ID() != "test-upload" {
		t.Fatal("missing upload ID")
	}
	body := bytes.Repeat([]byte("x"), PartSize+17)
	if n, err := upload.Write(body); err != nil || n != len(body) {
		t.Fatalf("write: %d %v", n, err)
	}
	fake.mu.Lock()
	if fake.contentType != "application/gzip" || fake.contentEncoding != "" {
		t.Fatal("incorrect gzip export metadata")
	}
	if fake.complete != 0 || len(fake.parts) != 1 || len(fake.parts[0]) != PartSize {
		t.Fatal("multipart completed before explicit validation")
	}
	fake.mu.Unlock()
	if err := upload.Complete(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := upload.Complete(context.Background()); err != nil {
		t.Fatal("repeat complete", err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.complete != 1 || len(fake.parts) != 2 || len(fake.parts[1]) != 17 || fmt.Sprint(fake.completedParts) != "[1 2]" {
		t.Fatal("incorrect multipart boundaries or order")
	}
	for _, path := range fake.paths {
		if path != "/test-bucket/private/job/events.ndjson" {
			t.Fatal("prefix escaped")
		}
	}
	if _, err := upload.Write([]byte("late")); !errors.Is(err, ErrClosed) {
		t.Fatal("write after completion")
	}
}
func TestEmptyAndExactPartUploads(t *testing.T) {
	for _, size := range []int{0, PartSize} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			fake := &fakeS3{}
			upload := begin(t, testStore(t, fake), context.Background())
			if _, err := upload.Write(make([]byte, size)); err != nil {
				t.Fatal(err)
			}
			if err := upload.Complete(context.Background()); err != nil {
				t.Fatal(err)
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			if len(fake.parts) != 1 || len(fake.parts[0]) != size {
				t.Fatal("extra or missing final part")
			}
		})
	}
}
func TestEmbeddedCompleteErrorAndAbort(t *testing.T) {
	fake := &fakeS3{completeError: true}
	upload := begin(t, testStore(t, fake), context.Background())
	_, _ = upload.Write([]byte("validated query"))
	err := upload.Complete(context.Background())
	if !errors.Is(err, ErrStorage) || strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "https") {
		t.Fatal("complete error not recognized or sanitized")
	}
	if _, err := upload.Write([]byte("more")); !errors.Is(err, ErrStorage) {
		t.Fatal("failed upload accepted data")
	}
	if err := upload.Abort(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := upload.Abort(context.Background()); err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.abort != 1 {
		t.Fatal("abort not idempotent")
	}
}
func TestFailedPartIsStickyAndAbortUsesFreshContext(t *testing.T) {
	fake := &fakeS3{partError: true}
	store := testStore(t, fake)
	ctx, cancel := context.WithCancel(context.Background())
	upload := begin(t, store, ctx)
	if _, err := upload.Write(make([]byte, PartSize)); !errors.Is(err, ErrStorage) || strings.Contains(err.Error(), "secret") {
		t.Fatal("failed part not sanitized")
	}
	cancel()
	if err := upload.Complete(context.Background()); !errors.Is(err, ErrFailed) {
		t.Fatal("failed upload completed")
	}
	if err := upload.Abort(context.Background()); err != nil {
		t.Fatal("abort reused canceled write context", err)
	}
}
func TestPartLimitNeverSilentlyTruncates(t *testing.T) {
	fake := &fakeS3{}
	upload := begin(t, testStore(t, fake), context.Background())
	// Reach the protocol boundary without allocating or sending 80 GiB.
	upload.parts = make([]types.CompletedPart, MaxParts)
	if n, err := upload.Write([]byte("overflow")); n != 0 || !errors.Is(err, ErrPartLimit) {
		t.Fatal("part limit did not fail explicitly")
	}
	if err := upload.Complete(context.Background()); !errors.Is(err, ErrFailed) {
		t.Fatal("truncated upload was completed")
	}
	if err := upload.Abort(context.Background()); err != nil {
		t.Fatal(err)
	}
}
func TestPresignAndDelete(t *testing.T) {
	fake := &fakeS3{}
	store := testStore(t, fake)
	signed, err := store.Presign(context.Background(), "job/events.ndjson", 3*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(signed)
	if err != nil {
		t.Fatal(err)
	}
	if u.Path != "/test-bucket/private/job/events.ndjson" || u.Query().Get("X-Amz-Expires") != "180" || u.Query().Get("X-Amz-Signature") == "" {
		t.Fatal("invalid signed download")
	}
	if strings.Contains(signed, "test-secret") {
		t.Fatal("secret exposed in signed URL")
	}
	if err := store.Delete(context.Background(), "job/events.ndjson"); err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.delete != 1 {
		t.Fatal("delete missing")
	}
}
func TestNoRedirects(t *testing.T) {
	var mu sync.Mutex
	requests := 0
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { mu.Lock(); requests++; mu.Unlock(); w.WriteHeader(204) }))
	defer target.Close()
	store := testStore(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	_, err := store.Begin(context.Background(), "job/events.ndjson")
	if !errors.Is(err, ErrStorage) {
		t.Fatal("redirect was accepted")
	}
	mu.Lock()
	defer mu.Unlock()
	if requests != 0 {
		t.Fatal("signed request followed redirect")
	}
}
func TestConfigurationAndKeyValidation(t *testing.T) {
	valid := Config{Region: "us-east-1", Bucket: "test-bucket", Prefix: "private/", AccessKeyID: "key", SecretAccessKey: "secret"}
	cases := []Config{{}, valid, valid, valid, valid, valid, valid, valid, valid, valid}
	cases[1].Prefix = ""
	cases[2].Prefix = "../public"
	cases[3].Bucket = "arn:bucket"
	cases[4].Region = ""
	cases[5].SecretAccessKey = ""
	cases[6].Endpoint = "http://example.com"
	cases[7].Endpoint = "https://user:secret@example.com"
	cases[8].Endpoint = "https://example.com/path"
	cases[9].Prefix = "private//nested"
	for _, c := range cases {
		if _, err := New(context.Background(), c); !errors.Is(err, ErrConfig) {
			t.Fatal("invalid configuration accepted")
		}
	}
	store := testStore(t, &fakeS3{})
	for _, key := range []string{"", "/absolute", "../escape", "a/../escape", "a//b", `a\b`, "a/%2e%2e/b", "a?query", strings.Repeat("a", 1024)} {
		if _, err := store.Begin(context.Background(), key); !errors.Is(err, ErrKey) {
			t.Fatal("unsafe key accepted")
		}
	}
	for _, ttl := range []time.Duration{0, -time.Second, 8 * 24 * time.Hour} {
		if _, err := store.Presign(context.Background(), "safe", ttl); !errors.Is(err, ErrConfig) {
			t.Fatal("unsafe expiry accepted")
		}
	}
	for _, endpoint := range []string{"http://minio:9000", "http://127.0.0.1:9000", "http://[::1]:9000", "https://s3.example.com"} {
		if !validEndpoint(endpoint) {
			t.Fatal("valid operator endpoint rejected")
		}
	}
}
func TestMissingUploadAbortIsIdempotent(t *testing.T) {
	store := testStore(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(404)
		fmt.Fprint(w, `<Error><Code>NoSuchUpload</Code><Message>missing</Message></Error>`)
	}))
	if err := store.Abort(context.Background(), "job/events.ndjson", "already-gone"); err != nil {
		t.Fatal(err)
	}
}

func TestDefaultCredentialsAndExplicitEndpoint(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "chain-access")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "chain-secret")
	t.Setenv("AWS_SESSION_TOKEN", "chain-session")
	t.Setenv("AWS_ENDPOINT_URL", "http://ambient.invalid")
	t.Setenv("AWS_ENDPOINT_URL_S3", "http://ambient-s3.invalid")
	server := httptest.NewServer(&fakeS3{})
	defer server.Close()
	store, err := New(context.Background(), Config{Region: "us-east-1", Bucket: "test-bucket", Prefix: "private", Endpoint: server.URL, UsePathStyle: true})
	if err != nil {
		t.Fatal(err)
	}
	signed, err := store.Presign(context.Background(), "job/events.ndjson", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(signed)
	if err != nil {
		t.Fatal("parse signed download")
	}
	if !strings.HasPrefix(u.Query().Get("X-Amz-Credential"), "chain-access/") || u.Query().Get("X-Amz-Security-Token") != "chain-session" {
		t.Fatal("default credential chain was not used")
	}
	if u.Scheme+"://"+u.Host != server.URL {
		t.Fatal("ambient endpoint overrode explicit validated configuration")
	}
	store, err = New(context.Background(), Config{Region: "us-east-1", Bucket: "test-bucket", Prefix: "private"})
	if err != nil {
		t.Fatal(err)
	}
	signed, err = store.Presign(context.Background(), "job/events.ndjson", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	u, _ = url.Parse(signed)
	if strings.Contains(u.Host, "ambient") {
		t.Fatal("ambient endpoint bypassed validated configuration")
	}
}
