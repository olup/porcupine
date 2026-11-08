package exportstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
)

func TestImmutableManifest(t *testing.T) {
	var mu sync.Mutex
	var data []byte
	store := testStore(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.URL.Path != "/test-bucket/private/job/manifest.json" {
			t.Error("unexpected object scope")
		}
		switch r.Method {
		case "GET":
			if data == nil {
				w.WriteHeader(404)
				fmt.Fprint(w, "<Error><Code>NoSuchKey</Code></Error>")
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.Write(data)
		case "PUT":
			if r.Header.Get("If-None-Match") != "*" || r.Header.Get("Content-Type") != "application/json" {
				t.Error("manifest must be immutable JSON")
			}
			if data != nil {
				w.WriteHeader(412)
				fmt.Fprint(w, "<Error><Code>PreconditionFailed</Code><Message>private-credential</Message></Error>")
				return
			}
			data, _ = io.ReadAll(r.Body)
		default:
			t.Error("unexpected method")
			w.WriteHeader(400)
		}
	}))
	ctx := context.Background()
	if _, err := store.ReadManifest(ctx, "job/manifest.json"); !errors.Is(err, ErrNotFound) {
		t.Fatal("missing marker must be explicit")
	}
	expected := `{"rows":0,"files":[]}`
	if err := store.CommitManifest(ctx, "job/manifest.json", []byte(expected)); err != nil {
		t.Fatal(err)
	}
	if err := store.CommitManifest(ctx, "job/manifest.json", []byte(`{}`)); !errors.Is(err, ErrExists) || strings.Contains(err.Error(), "private") {
		t.Fatal("commit overwrite must be denied safely")
	}
	got, err := store.ReadManifest(ctx, "job/manifest.json")
	if err != nil || string(got) != expected {
		t.Fatal("manifest changed")
	}
	if err = store.CommitManifest(ctx, "../manifest.json", []byte(expected)); !errors.Is(err, ErrKey) {
		t.Fatal("unsafe manifest path")
	}
	if err = store.CommitManifest(ctx, "job/manifest.json", make([]byte, MaxManifestBytes+1)); !errors.Is(err, ErrStorage) {
		t.Fatal("unbounded marker")
	}
}

func TestManifestReadBound(t *testing.T) {
	for _, size := range []int{2 << 20, MaxManifestBytes + 1} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			store := testStore(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				io.WriteString(w, strings.Repeat("x", size))
			}))
			data, err := store.ReadManifest(context.Background(), "job/manifest.json")
			if size > MaxManifestBytes {
				if !errors.Is(err, ErrStorage) {
					t.Fatal("oversized remote marker accepted")
				}
			} else if err != nil || len(data) != size {
				t.Fatal("valid bounded marker truncated")
			}
		})
	}
}
