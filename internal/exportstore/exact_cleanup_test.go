package exportstore

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/olup/porcupine/internal/s3client"
)

func TestExactArtifactCleanupUsesVersionAndUploadIDs(t *testing.T) {
	t.Parallel()
	const key = "project/job/file.gz"
	const fullKey = "isolated/" + key
	var mu sync.Mutex
	var deletions []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		w.Header().Set("Content-Type", "application/xml")
		switch {
		case r.Method == http.MethodGet && q.Has("versions"):
			if q.Get("prefix") != fullKey {
				t.Error("version listing escaped exact prefix")
			}
			switch q.Get("key-marker") {
			case "":
				fmt.Fprintf(w, `<ListVersionsResult><IsTruncated>true</IsTruncated>
					<NextKeyMarker>%s</NextKeyMarker><NextVersionIdMarker>first</NextVersionIdMarker>
					<Version><Key>%s</Key><VersionId>first</VersionId></Version>
					<Version><Key>%s.foreign</Key><VersionId>untouched</VersionId></Version></ListVersionsResult>`, fullKey, fullKey, fullKey)
			case fullKey:
				if q.Get("version-id-marker") != "first" {
					t.Error("version listing lost pagination identity")
				}
				fmt.Fprintf(w, `<ListVersionsResult><IsTruncated>false</IsTruncated>
					<DeleteMarker><Key>%s</Key><VersionId>marker</VersionId></DeleteMarker></ListVersionsResult>`, fullKey)
			default:
				t.Error("unexpected version listing cursor")
			}
		case r.Method == http.MethodGet && q.Has("uploads"):
			if q.Get("prefix") != fullKey {
				t.Error("upload listing escaped exact prefix")
			}
			fmt.Fprintf(w, `<ListMultipartUploadsResult><IsTruncated>false</IsTruncated>
				<Upload><Key>%s</Key><UploadId>unfinished</UploadId></Upload>
				<Upload><Key>%s.foreign</Key><UploadId>untouched</UploadId></Upload></ListMultipartUploadsResult>`, fullKey, fullKey)
		case r.Method == http.MethodGet && q.Has("uploadId"):
			if !strings.HasSuffix(r.URL.Path, "/"+fullKey) {
				t.Error("upload lookup escaped owned key")
			}
			if q.Get("uploadId") == "unlisted" {
				fmt.Fprint(w, `<ListPartsResult><IsTruncated>false</IsTruncated></ListPartsResult>`)
				return
			}
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `<Error><Code>NoSuchUpload</Code></Error>`)
		case r.Method == http.MethodDelete:
			if !strings.HasSuffix(r.URL.Path, "/"+fullKey) {
				t.Error("delete escaped owned key")
			}
			mu.Lock()
			deletions = append(deletions, q.Encode())
			mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected storage operation %s", r.Method)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	t.Cleanup(server.Close)
	store, err := New(t.Context(), Config{
		Region: "us-east-1", Bucket: "disposable-exports", Prefix: "isolated", Endpoint: server.URL,
		AccessKeyID: "key", SecretAccessKey: "secret", UsePathStyle: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	artifacts, err := store.ListExactArtifacts(t.Context(), key)
	want := []s3client.Artifact{
		{Key: fullKey, VersionID: "first"}, {Key: fullKey, VersionID: "marker"}, {Key: fullKey, UploadID: "unfinished"},
	}
	if err != nil || !slices.Equal(artifacts, want) {
		t.Fatalf("list artifacts = %+v, %v", artifacts, err)
	}
	found, err := store.ExactUploadExists(t.Context(), key, "unlisted")
	if err != nil || !found {
		t.Fatalf("checkpointed unlisted upload: %v, %v", found, err)
	}
	found, err = store.ExactUploadExists(t.Context(), key, "completed")
	if err != nil || found {
		t.Fatalf("completed upload lookup: %v, %v", found, err)
	}
	for _, artifact := range artifacts {
		if err = store.DeleteExactArtifact(t.Context(), key, artifact.VersionID, artifact.UploadID); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.DeleteExactArtifact(t.Context(), key, "", ""); err == nil {
		t.Fatal("delete accepted a missing exact identity")
	}
	mu.Lock()
	defer mu.Unlock()
	for _, query := range deletions {
		// The AWS SDK may add x-id to the query. Keep only identity fields.
		if !strings.Contains(query, "versionId=") && !strings.Contains(query, "uploadId=") {
			t.Fatalf("non-exact delete: %q", query)
		}
	}
	if len(deletions) != 3 {
		t.Fatalf("delete requests = %v", deletions)
	}
}
