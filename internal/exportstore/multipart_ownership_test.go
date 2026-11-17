package exportstore

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestAbortKeyUploadsReconcilesOnlyExactOwnedKey(t *testing.T) {
	t.Parallel()
	var aborted []string
	pages := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		if r.Method == http.MethodDelete {
			aborted = append(aborted, r.URL.Query().Get("uploadId"))
			w.WriteHeader(204)
			return
		}
		if r.Method != http.MethodGet || !r.URL.Query().Has("uploads") || r.URL.Query().Get("prefix") != "private/owned/file.gz" {
			t.Errorf("unexpected reconciliation request: %s %s", r.Method, r.URL)
			w.WriteHeader(400)
			return
		}
		pages++
		if pages == 1 {
			fmt.Fprint(w, `<ListMultipartUploadsResult><IsTruncated>true</IsTruncated><NextKeyMarker>private/owned/file.gz</NextKeyMarker><NextUploadIdMarker>first</NextUploadIdMarker><Upload><Key>private/owned/file.gz</Key><UploadId>first</UploadId></Upload><Upload><Key>private/owned/file.gz-neighbor</Key><UploadId>neighbor</UploadId></Upload></ListMultipartUploadsResult>`)
		} else {
			if r.URL.Query().Get("key-marker") != "private/owned/file.gz" || r.URL.Query().Get("upload-id-marker") != "first" {
				t.Error("multipart cursor lost")
			}
			fmt.Fprint(w, `<ListMultipartUploadsResult><IsTruncated>false</IsTruncated><Upload><Key>private/owned/file.gz</Key><UploadId>second</UploadId></Upload></ListMultipartUploadsResult>`)
		}
	}))
	defer server.Close()
	store, err := New(context.Background(), Config{Region: "us-east-1", Bucket: "private-bucket", Prefix: "private", Endpoint: server.URL, UsePathStyle: true, AccessKeyID: "test-access", SecretAccessKey: "test-secret"})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err = store.AbortKeyUploads(t.Context(), "owned/file.gz"); err != nil {
		t.Fatal(err)
	}
	if pages != 2 || !reflect.DeepEqual(aborted, []string{"first", "second"}) {
		t.Fatal("reconciliation crossed exact key ownership", pages, aborted)
	}
}
