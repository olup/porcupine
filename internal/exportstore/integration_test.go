package exportstore

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"os"
	"testing"
	"time"
)

// Requires a pre-created private bucket. The endpoint opt-in keeps normal tests local.
func TestMinIOIntegration(t *testing.T) {
	endpoint := os.Getenv("PORCUPINE_TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("set PORCUPINE_TEST_S3_ENDPOINT for an existing MinIO bucket")
	}
	region := os.Getenv("PORCUPINE_TEST_S3_REGION")
	if region == "" {
		region = "us-east-1"
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		t.Fatal("generate test prefix")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	store, err := New(ctx, Config{Region: region, Bucket: os.Getenv("PORCUPINE_TEST_S3_BUCKET"), Prefix: "porcupine-test/" + hex.EncodeToString(random[:]), Endpoint: endpoint, AccessKeyID: os.Getenv("PORCUPINE_TEST_S3_ACCESS_KEY_ID"), SecretAccessKey: os.Getenv("PORCUPINE_TEST_S3_SECRET_ACCESS_KEY"), SessionToken: os.Getenv("PORCUPINE_TEST_S3_SESSION_TOKEN"), UsePathStyle: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	const key = "job/events.ndjson"
	upload, err := store.Begin(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if upload.completed {
			if err := store.Delete(cleanup, key); err != nil {
				t.Error("delete test object failed")
			}
		} else if err := upload.Abort(cleanup); err != nil {
			t.Error("abort test multipart failed")
		}
	})
	body := bytes.Repeat([]byte("export row\n"), PartSize/11+100)
	if n, err := upload.Write(body); err != nil || n != len(body) {
		t.Fatal("multipart write failed")
	}
	if err := upload.Complete(ctx); err != nil {
		t.Fatal(err)
	}
	signed, err := store.Presign(ctx, key, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return ErrStorage }}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, signed, nil)
	if err != nil {
		t.Fatal("create download request")
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal("signed download failed")
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatal("signed download returned unsuccessful status")
	}
	digest := sha256.New()
	n, err := io.Copy(digest, io.LimitReader(response.Body, int64(len(body))+1))
	expected := sha256.Sum256(body)
	if err != nil || n != int64(len(body)) || !bytes.Equal(digest.Sum(nil), expected[:]) {
		t.Fatal("downloaded export differs from multipart input")
	}
}
