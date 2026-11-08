package porcupine

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestClickHouseIntegrationS3Pipelines(t *testing.T) {
	endpoint := os.Getenv("PORCUPINE_TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("run make integration-s3 for isolated ClickHouse and Silo")
	}
	ch := integrationClickHouseConfig(t)
	var clock atomic.Int64
	start := time.Now().UTC().Truncate(time.Minute)
	clock.Store(start.Add(5 * time.Second).UnixMilli())
	dbPath := filepath.Join(t.TempDir(), "pipelines.db")
	config := Config{DBPath: dbPath, BootstrapToken: adminToken, QueueMaxEvents: 2000, Clock: func() time.Time { return time.UnixMilli(clock.Load()).UTC() }, ClickHouse: &ch, Pipelines: &PipelineConfig{Key: bytes.Repeat([]byte{0x59}, 32), AllowedS3Endpoints: []string{endpoint}, ExportLimits: ExportConfig{FileBytes: 32 << 10, MaxExecutionSeconds: 30, BandwidthBytesPerSecond: 1 << 20}}}
	s, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if s != nil {
			s.Close()
		}
	}()
	s.stopPipelines()
	// No global on-demand bucket exists in this fixture.
	if s.exports != nil {
		t.Fatal("pipeline unexpectedly requires global exports")
	}
	a, ingest := project(t, s)
	b, otherIngest := project(t, s)
	_, owner := key(t, s, []string{"events:read", "pipelines:write", "pipelines:read"}, []string{a})
	_, other := key(t, s, []string{"events:read", "pipelines:write", "pipelines:read"}, []string{b})
	const count = 1103
	for offset := 0; offset < count; offset += 100 {
		batch := []map[string]any{}
		for i := offset; i < min(offset+100, count); i++ {
			batch = append(batch, map[string]any{"uuid": fmt.Sprintf("pipeline-%06d", i), "event": "café東京", "distinct_id": "customer", "timestamp": "2026-01-01T00:00:00.123Z"})
		}
		body, _ := json.Marshal(map[string]any{"api_key": ingest, "batch": batch})
		request(t, s, "POST", "/batch/", "", string(body), 200)
	}
	request(t, s, "POST", "/capture/", "", fmt.Sprintf(`{"api_key":%q,"uuid":"other-private","event":"café東京","distinct_id":"private"}`, otherIngest), 200)
	_, releaseAdmission := s.beginCaptureReceipt(a)
	defer releaseAdmission()
	clock.Store(start.Add(65 * time.Second).UnixMilli())
	request(t, s, "POST", "/capture/", "", fmt.Sprintf(`{"api_key":%q,"uuid":"second-window","event":"café東京","distinct_id":"customer"}`, ingest), 200)
	deadline := time.Now().Add(30 * time.Second)
	for {
		var pending int
		if s.db.QueryRow(`SELECT count(*) FROM events`).Scan(&pending) != nil {
			t.Fatal("seed queue status")
		}
		if pending == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("seed delivery did not drain")
		}
		if err = s.flushDelivery(context.Background()); err != nil {
			t.Fatal("seed delivery failed")
		}
	}
	spec := testS3Spec()
	spec.StartAt = start.Format(time.RFC3339)
	spec.SQL = "SELECT uuid, event, received_at_ms FROM events WHERE event={name:String} ORDER BY uuid"
	spec.Parameters = map[string]string{"name": "café東京"}
	spec.Destination = S3PipelineDestination{Bucket: os.Getenv("PORCUPINE_TEST_S3_BUCKET"), Region: os.Getenv("PORCUPINE_TEST_S3_REGION"), Prefix: "pipeline-" + newID(), Endpoint: endpoint}
	body, _ := json.Marshal(PipelineCreateBody{Name: "customer windows", Kind: "s3", S3: &spec, Credentials: PipelineCredentials{AccessKeyID: os.Getenv("PORCUPINE_TEST_S3_ACCESS_KEY_ID"), SecretAccessKey: os.Getenv("PORCUPINE_TEST_S3_SECRET_ACCESS_KEY"), SessionToken: os.Getenv("PORCUPINE_TEST_S3_SESSION_TOKEN")}})
	base := "/api/v1/projects/" + a + "/pipelines"
	created := request(t, s, "POST", base, owner, string(body), 201)
	id := created["id"].(string)
	request(t, s, "GET", base+"/"+id, other, "", 403)
	if progress, stepErr := s.stepPipelineS3(context.Background()); stepErr != nil || progress {
		t.Fatal("older in-flight capture must block closing its receipt window")
	}
	var unclosed int
	if s.db.QueryRow(`SELECT count(*) FROM pipeline_s3_runs WHERE pipeline_id=?`, id).Scan(&unclosed) != nil || unclosed != 0 {
		t.Fatal("in-flight arrival established an incomplete watermark")
	}
	releaseAdmission()
	if progress, stepErr := s.stepPipelineS3(context.Background()); stepErr != nil || !progress {
		t.Fatalf("first window step: progress=%v err=%v", progress, stepErr)
	}
	runs, err := s.s3PipelineRunPage(context.Background(), a, id, 10, 0)
	if err != nil || len(runs) != 1 || runs[0].State != "succeeded" {
		t.Fatalf("first window did not succeed: %#v err=%v", runs, err)
	}
	p, err := s.loadPipeline(context.Background(), a, id)
	if err != nil {
		t.Fatal(err)
	}
	var normalized S3PipelineSpec
	json.Unmarshal(p.Config, &normalized)
	store, err := s.pipelineS3Store(context.Background(), p, normalized)
	if err != nil {
		t.Fatal("open fixture S3 destination")
	}
	defer store.Close()
	readManifest := func(window int64) pipelineS3Manifest {
		t.Helper()
		data, e := store.ReadManifest(context.Background(), fmt.Sprintf("%s/%d/manifest.json", id, window))
		if e != nil {
			t.Fatal("read committed manifest")
		}
		var manifest pipelineS3Manifest
		if json.Unmarshal(data, &manifest) != nil {
			t.Fatal("invalid committed manifest")
		}
		return manifest
	}
	manifest := readManifest(start.UnixMilli())
	if manifest.Rows != count || len(manifest.Files) < 2 || manifest.WindowEndMS != start.Add(time.Minute).UnixMilli() {
		t.Fatalf("manifest scope/count/files: rows=%d files=%d", manifest.Rows, len(manifest.Files))
	}
	seen := map[string]bool{}
	for _, file := range manifest.Files {
		url, e := store.Presign(context.Background(), strings.TrimPrefix(file.Key, normalized.Destination.Prefix+"/"), time.Minute)
		if e != nil {
			t.Fatal("presign fixture object")
		}
		response, e := http.Get(url)
		if e != nil {
			t.Fatal("download fixture object")
		}
		data, e := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		response.Body.Close()
		if e != nil || response.StatusCode != 200 {
			t.Fatal("download status")
		}
		sum := sha256.Sum256(data)
		if hex.EncodeToString(sum[:]) != file.SHA256 || int64(len(data)) != file.Bytes {
			t.Fatal("artifact hash/bytes mismatch")
		}
		gz, e := gzip.NewReader(bytes.NewReader(data))
		if e != nil {
			t.Fatal("artifact gzip")
		}
		decoded, e := io.ReadAll(gz)
		gz.Close()
		if e != nil {
			t.Fatal("artifact truncated")
		}
		for _, line := range bytes.Split(bytes.TrimSpace(decoded), []byte("\n")) {
			var row struct {
				UUID     string      `json:"uuid"`
				Event    string      `json:"event"`
				Received json.Number `json:"received_at_ms"`
			}
			decodeErr := json.Unmarshal(line, &row)
			received, receivedErr := row.Received.Int64()
			if decodeErr != nil || receivedErr != nil || !strings.HasPrefix(row.UUID, "pipeline-") || seen[row.UUID] || row.Event != "café東京" || received < manifest.WindowStartMS || received >= manifest.WindowEndMS {
				t.Fatal("cross-project/window, duplicate or malformed artifact row")
			}
			seen[row.UUID] = true
		}
	}
	if len(seen) != count {
		t.Fatal("missing streamed rows")
	}
	// Restart must retain the cursor; paused time catches up sequentially with an
	// empty-window commit, without repeating the completed first window.
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(config)
	if err != nil {
		t.Fatal(err)
	}
	s.stopPipelines()
	clock.Store(start.Add(181 * time.Second).UnixMilli())
	for range 2 {
		if ok, e := s.stepPipelineS3(context.Background()); e != nil || !ok {
			t.Fatal("catch-up step failed")
		}
	}
	second := readManifest(start.Add(time.Minute).UnixMilli())
	empty := readManifest(start.Add(2 * time.Minute).UnixMilli())
	if second.Rows != 1 || empty.Rows != 0 || len(empty.Files) != 0 {
		t.Fatal("restart/catch-up skipped or duplicated a completed window")
	}
	var cursor int64
	if err = s.db.QueryRow(`SELECT cursor_ms FROM pipeline_s3_state WHERE pipeline_id=?`, id).Scan(&cursor); err != nil || cursor != start.Add(3*time.Minute).UnixMilli() {
		t.Fatal("cursor did not advance through exact completed windows")
	}
	var retainedManifestBytes int
	if s.db.QueryRow(`SELECT COALESCE(sum(length(manifest)),0) FROM pipeline_s3_runs WHERE pipeline_id=?`, id).Scan(&retainedManifestBytes) != nil || retainedManifestBytes != 0 {
		t.Fatal("successful runs retain unbounded manifests")
	}
	// A real HTTP transport remains available for API metadata after execution.
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	req, _ := http.NewRequest("GET", host.URL+base+"/"+id+"/runs", nil)
	req.Header.Set("Authorization", "Bearer "+owner)
	res, e := http.DefaultClient.Do(req)
	if e != nil {
		t.Fatal("run metadata HTTP")
	}
	res.Body.Close()
	if res.StatusCode != 200 {
		t.Fatal("run metadata unavailable")
	}
}
