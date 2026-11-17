package porcupine

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/exportstore"
)

func nativeExportWorkerFixture(t *testing.T, fake *exportTestS3) (*Service, storedExport) {
	t.Helper()
	s := duckLakeJourneyService(t)
	s.lakeMemoryMB = defaultDuckLakeMemoryMB
	config, err := normalizeExportConfig(ExportConfig{FileBytes: 1024, BandwidthBytesPerSecond: 100 << 20, MaxExecutionSeconds: 10})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(fake)
	t.Cleanup(server.Close)
	store, err := exportstore.New(context.Background(), exportstore.Config{Region: "us-east-1", Bucket: "native-exports", Prefix: "isolated", Endpoint: server.URL, AccessKeyID: "key", SecretAccessKey: "secret", UsePathStyle: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	s.exports = &exportRuntime{config: config, store: store, wake: make(chan struct{}, 1)}
	p, ingest := project(t, s)
	_, reader := key(t, s, []string{"events:read"}, []string{p})
	request(t, s, "POST", "/capture/", "", fmt.Sprintf(`{"api_key":%q,"uuid":"native-export","event":"purchase","distinct_id":"user","properties":{"amount":42}}`, ingest), 200)
	duckLakeJourneyDrain(t, s)
	input := &ExportCreateInput{ID: p, Body: ExportCreateBody{IdempotencyKey: "native-export", SQL: "SELECT uuid,event,json_extract_string(properties,'$.amount') AS amount FROM events WHERE event=$name", Parameters: map[string]string{"name": "purchase"}}}
	created, err := s.createEventExport(exportContext(t, s, reader), input)
	if err != nil {
		t.Fatal(err)
	}
	queued, err := s.loadExport(context.Background(), p, created.Body.ID)
	if err != nil {
		t.Fatal(err)
	}
	job, err := s.claimExport(context.Background(), queued)
	if err != nil {
		t.Fatal(err)
	}
	return s, job
}

func TestDuckLakeExportWorkerPublicationAndRevocation(t *testing.T) {
	t.Parallel()
	for _, revoke := range []bool{false, true} {
		t.Run(fmt.Sprint(revoke), func(t *testing.T) {
			fake := &exportTestS3{}
			s, job := nativeExportWorkerFixture(t, fake)
			if revoke {
				fake.onComplete = func() {
					if _, err := s.control.Exec("UPDATE access_keys SET revoked_at=? WHERE id=?", now(), job.ActorID); err != nil {
						t.Error(err)
					}
				}
			}
			s.runExport(context.Background(), job)
			current, err := s.loadExport(context.Background(), job.ProjectID, job.ID)
			if err != nil {
				t.Fatal(err)
			}
			if revoke {
				if current.State != ExportCanceled || current.CleanupPending {
					t.Fatalf("revoked export published: %+v", current)
				}
				fake.mu.Lock()
				defer fake.mu.Unlock()
				if len(fake.objects) != 0 {
					t.Fatal("revoked export left objects")
				}
				return
			}
			if current.State != ExportSucceeded || current.Rows != 1 || current.FileCount != 1 {
				t.Fatalf("native export failed: %+v", current)
			}
			fake.mu.Lock()
			defer fake.mu.Unlock()
			for _, data := range fake.objects {
				gz, err := gzip.NewReader(bytes.NewReader(data))
				if err != nil {
					t.Fatal(err)
				}
				body, err := io.ReadAll(gz)
				gz.Close()
				if err != nil {
					t.Fatal(err)
				}
				var row map[string]any
				if json.Unmarshal(bytes.TrimSpace(body), &row) != nil || row["uuid"] != "native-export" || row["amount"] != "42" {
					t.Fatalf("native object: %s", body)
				}
			}
		})
	}
}

func TestDuckLakeConfiguredExportsOpen(t *testing.T) {
	t.Parallel()
	fake := &exportTestS3{}
	server := httptest.NewServer(fake)
	defer server.Close()
	s, err := Open(Config{DBPath: filepath.Join(t.TempDir(), "acceptance.sqlite"), BootstrapToken: adminToken, DuckLake: &DuckLakeConfig{Directory: filepath.Join(t.TempDir(), "lake")}, Exports: &ExportConfig{S3: exportstore.Config{Region: "us-east-1", Bucket: "native-exports", Prefix: "isolated", Endpoint: server.URL, AccessKeyID: "key", SecretAccessKey: "secret", UsePathStyle: true}}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s.lake == nil || s.exports == nil {
		t.Fatal("native exports not configured")
	}
}

func TestDuckLakeS3PipelineWindowsPublicationAndRevocation(t *testing.T) {
	t.Parallel()
	for _, revoke := range []bool{false, true} {
		t.Run(fmt.Sprint(revoke), func(t *testing.T) {
			s := duckLakeJourneyService(t)
			enableTestPipelines(t, s)
			fake := &pipelineRecoveryS3{markers: map[string][]byte{}}
			server := httptest.NewServer(fake)
			t.Cleanup(server.Close)
			s.pipelines.s3Origins[server.URL] = true
			limits, err := normalizeExportConfig(ExportConfig{FileBytes: 1024, BandwidthBytesPerSecond: 100 << 20, MaxExecutionSeconds: 10})
			if err != nil {
				t.Fatal(err)
			}
			s.pipelines.config.ExportLimits = limits
			start := time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC)
			current := start.Add(5 * time.Second)
			s.clock = func() time.Time { return current }
			a, ingest := project(t, s)
			b, otherIngest := project(t, s)
			ownerID, owner := key(t, s, []string{"events:read", "pipelines:read", "pipelines:write"}, []string{a})
			_, other := key(t, s, []string{"events:read", "pipelines:read", "pipelines:write"}, []string{b})
			for i := range 3 {
				body, _ := json.Marshal(map[string]any{"api_key": ingest, "uuid": fmt.Sprintf("window-%d", i), "event": "purchase", "distinct_id": "user", "properties": map[string]any{"note": strings.Repeat("x", 900)}})
				request(t, s, "POST", "/capture/", "", string(body), 200)
			}
			request(t, s, "POST", "/capture/", "", fmt.Sprintf(`{"api_key":%q,"uuid":"private","event":"purchase","distinct_id":"other"}`, otherIngest), 200)
			_, releaseAdmission := s.beginCaptureReceipt(a)
			defer releaseAdmission()
			current = start.Add(65 * time.Second)
			request(t, s, "POST", "/capture/", "", fmt.Sprintf(`{"api_key":%q,"uuid":"late","event":"purchase","distinct_id":"user"}`, ingest), 200)
			duckLakeJourneyDrain(t, s)
			imported := lakeStorageProjection(t, a, "historical", 9999)
			imported.Event.Event = "purchase"
			imported.Imported = true
			if err = s.lake.Apply(context.Background(), []eventProjection{imported}); err != nil {
				t.Fatal(err)
			}
			spec := testS3Spec()
			spec.StartAt = start.Format(time.RFC3339)
			spec.SQL = "WITH selected AS (SELECT uuid,properties FROM events WHERE event=$name) SELECT uuid,properties FROM selected WHERE uuid IN (SELECT uuid FROM events) ORDER BY uuid"
			spec.Parameters = map[string]string{"name": "purchase"}
			spec.Destination.Endpoint = server.URL
			body, _ := json.Marshal(PipelineCreateBody{Name: "native window", Kind: "s3", S3: &spec, Credentials: PipelineCredentials{AccessKeyID: "key", SecretAccessKey: "secret"}})
			base := "/api/v1/projects/" + a + "/pipelines"
			created := request(t, s, "POST", base, owner, string(body), 201)
			id := created["id"].(string)
			request(t, s, "GET", base+"/"+id, other, "", 403)
			if progress, err := s.stepPipelineS3(context.Background()); err != nil || progress {
				t.Fatalf("in-flight capture not respected: %v %v", progress, err)
			}
			releaseAdmission()
			if revoke {
				fake.data.onComplete = func() {
					if _, err := s.control.Exec("UPDATE access_keys SET revoked_at=? WHERE id=?", now(), ownerID); err != nil {
						t.Error(err)
					}
				}
			}
			if progress, err := s.stepPipelineS3(context.Background()); err != nil || !progress {
				t.Fatalf("native pipeline did not run: %v %v", progress, err)
			}
			runs, err := s.s3PipelineRunPage(context.Background(), a, id, 10, 0)
			if err != nil || len(runs) != 1 {
				t.Fatalf("native runs: %+v %v", runs, err)
			}
			if revoke {
				if runs[0].State == "succeeded" {
					t.Fatal("revoked pipeline published")
				}
				fake.mu.Lock()
				markers := len(fake.markers)
				fake.mu.Unlock()
				fake.data.mu.Lock()
				objects := len(fake.data.objects)
				fake.data.mu.Unlock()
				if markers != 0 || objects != 0 {
					t.Fatalf("revoked pipeline retained output: %d %d", markers, objects)
				}
				return
			}
			if runs[0].State != "succeeded" || runs[0].Rows != 3 {
				t.Fatalf("native pipeline state: %+v", runs[0])
			}
			fake.mu.Lock()
			if len(fake.markers) != 1 {
				t.Fatal("missing commit marker")
			}
			for _, raw := range fake.markers {
				var manifest pipelineS3Manifest
				if json.Unmarshal(raw, &manifest) != nil || manifest.Rows != 3 || len(manifest.Files) != 3 || manifest.ProjectID != a || manifest.WindowStartMS != start.UnixMilli() {
					t.Fatalf("native manifest: %s", raw)
				}
			}
			fake.mu.Unlock()
			fake.data.mu.Lock()
			seen := map[string]bool{}
			for _, data := range fake.data.objects {
				gz, err := gzip.NewReader(bytes.NewReader(data))
				if err != nil {
					t.Fatal(err)
				}
				raw, err := io.ReadAll(gz)
				gz.Close()
				if err != nil {
					t.Fatal(err)
				}
				var row map[string]any
				if json.Unmarshal(bytes.TrimSpace(raw), &row) != nil {
					t.Fatalf("bad native pipeline row: %s", raw)
				}
				uuid, _ := row["uuid"].(string)
				if !strings.HasPrefix(uuid, "window-") || seen[uuid] {
					t.Fatalf("window or project leak: %s", uuid)
				}
				seen[uuid] = true
			}
			fake.data.mu.Unlock()
			if len(seen) != 3 {
				t.Fatal("missing exported rows")
			}
			if progress, err := s.stepPipelineS3(context.Background()); err != nil || progress {
				t.Fatalf("completed window retried: %v %v", progress, err)
			}
			for _, sql := range []string{"SELECT 1", "SELECT * FROM lake.current_events", "SELECT * FROM events WHERE uuid=$missing"} {
				invalid := spec
				invalid.SQL = sql
				invalid.Parameters = nil
				if _, err := s.validateS3PipelineSpec(context.Background(), a, invalid); err == nil {
					t.Fatalf("invalid native pipeline accepted: %s", sql)
				}
			}
		})
	}
}

func TestDuckLakeExportProducesPrefixBeforeLargeResultCompletes(t *testing.T) {
	t.Parallel()
	store := openLakeStorageTest(t, t.TempDir(), 1)
	batch := make([]eventProjection, 100)
	for i := range batch {
		batch[i] = lakeStorageProjection(t, "large", fmt.Sprintf("row-%03d", i), int64(i+1))
		batch[i].Properties = json.RawMessage(`{"padding":"` + strings.Repeat("x", 1024) + `"}`)
	}
	if err := store.Apply(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	seen := 0
	_, err := store.Export(ctx, "large", "SELECT a.uuid,a.properties FROM events a CROSS JOIN events b CROSS JOIN events c", nil, nil, 4096, exportStreamCallbacks{Columns: func([]AnalyticsResultColumn) error { return nil }, Row: func([]json.RawMessage) error {
		seen++
		if seen == 20 {
			cancel()
		}
		return nil
	}})
	if !errors.Is(err, context.Canceled) || seen != 20 {
		t.Fatalf("large output failed before prefix or ignored cancellation: %d %v", seen, err)
	}
	entries, readErr := filepath.Glob(filepath.Join(store.ProjectDirectory("large"), "temp", "export-*"))
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("stream directory retained after cancellation: %v", entries)
	}
	_, err = store.Export(context.Background(), "large", "SELECT a.properties FROM events a CROSS JOIN events b CROSS JOIN events c", nil, nil, 64, exportStreamCallbacks{Columns: func([]AnalyticsResultColumn) error { return nil }, Row: func([]json.RawMessage) error { return nil }})
	if !errors.Is(err, errExportValueTooLarge) {
		t.Fatalf("row-limit cause masked by COPY cancellation: %v", err)
	}
	_, err = store.Export(context.Background(), "large", "SELECT a.properties FROM events a CROSS JOIN events b CROSS JOIN events c", nil, nil, 4096, exportStreamCallbacks{Columns: func([]AnalyticsResultColumn) error { return nil }, Row: func([]json.RawMessage) error { return errExportPressure }})
	if !errors.Is(err, errExportPressure) {
		t.Fatalf("consumer cause masked by COPY cancellation: %v", err)
	}

	if count := lakeStorageCount(t, store, "large", "events"); count != 100 {
		t.Fatal("cancelled export broke reads")
	}
}
