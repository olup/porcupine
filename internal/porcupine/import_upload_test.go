package porcupine

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/importer"
)

func importTestSHA(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }
func importTestRow(uuid string) []byte {
	raw, _ := json.Marshal(map[string]any{"uuid": uuid, "event": "imported purchase", "distinct_id": "customer", "timestamp": "2020-01-02T03:04:05Z", "properties": map[string]any{"amount": 42}})
	return append(raw, '\n')
}
func importTestRequest(t *testing.T, s *Service, method, path, token string, data []byte, headers map[string]string, status int) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, path, bytes.NewReader(data))
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	if method == "PUT" {
		r.Header.Set("Content-Type", "application/octet-stream")
	} else if len(data) > 0 {
		r.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != status {
		t.Fatalf("%s %s got%d want%d: %s", method, path, w.Code, status, w.Body.String())
	}
	return w
}
func importTestJob(t *testing.T, w *httptest.ResponseRecorder) importer.ImportUploadJob {
	t.Helper()
	var job importer.ImportUploadJob
	if err := json.Unmarshal(w.Body.Bytes(), &job); err != nil {
		t.Fatal(err)
	}
	return job
}
func importTestCreate(t *testing.T, s *Service, project, token, key string, files []importer.ImportUploadFileSpec) importer.ImportUploadJob {
	t.Helper()
	data, _ := json.Marshal(map[string]any{"idempotency_key": key, "files": files})
	return importTestJob(t, importTestRequest(t, s, "POST", "/api/v1/projects/"+project+"/imports", token, data, nil, 201))
}
func importTestBase(project, job string) string {
	return "/api/v1/projects/" + project + "/imports/" + job
}
func importTestPut(t *testing.T, s *Service, project string, job importer.ImportUploadJob, file importer.ImportUploadFile, token string, offset int64, data []byte, status int) *httptest.ResponseRecorder {
	t.Helper()
	return importTestRequest(t, s, "PUT", importTestBase(project, job.ID)+"/files/"+file.ID, token, data, map[string]string{"Upload-Offset": fmt.Sprint(offset), "X-Chunk-SHA256": importTestSHA(data)}, status)
}

func TestImportUploadRestartChunkIdempotencyCompletionAndMarker(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "imports.db")
	s, err := Open(Config{DBPath: path, BootstrapToken: adminToken})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { s.Close() }()
	project, _ := project(t, s)
	_, token := key(t, s, []string{"imports:write", "events:write"}, []string{project})
	data := append(importTestRow("first"), bytes.TrimSuffix(importTestRow("second"), []byte{'\n'})...)
	files := []importer.ImportUploadFileSpec{{Path: "events/part.jsonl", Kind: "events", SizeBytes: int64(len(data)), SHA256: importTestSHA(data)}}
	job := importTestCreate(t, s, project, token, "stable-import", files)
	again := importTestCreate(t, s, project, token, "stable-import", files)
	if again.ID != job.ID || again.Files[0].ID != job.Files[0].ID {
		t.Fatal("create retry lost manifest identity")
	}
	split := len(importTestRow("first")) + 17
	first := importTestJob(t, importTestPut(t, s, project, job, job.Files[0], token, 0, data[:split], 200))
	if first.Files[0].OffsetBytes != int64(split) || first.Counts.Accepted != 1 {
		t.Fatal("first durable checkpoint wrong", first)
	}
	repeated := importTestJob(t, importTestPut(t, s, project, job, job.Files[0], token, 0, data[:split], 200))
	if repeated.Counts != first.Counts || eventCount(t, s) != 1 {
		t.Fatal("lost ACK replay reapplied accepted rows")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(Config{DBPath: path})
	if err != nil {
		t.Fatal(err)
	}
	status := importTestJob(t, importTestRequest(t, s, "GET", importTestBase(project, job.ID), token, nil, nil, 200))
	if status.Files[0].OffsetBytes != int64(split) || status.Counts.Accepted != 1 {
		t.Fatal("restart lost file progress")
	}
	uploaded := importTestJob(t, importTestPut(t, s, project, job, job.Files[0], token, int64(split), data[split:], 200))
	if uploaded.Files[0].OffsetBytes != int64(len(data)) || uploaded.Counts.Accepted != 1 {
		t.Fatal("final partial row was accepted prematurely")
	}
	completedFile := importTestJob(t, importTestRequest(t, s, "POST", importTestBase(project, job.ID)+"/files/"+job.Files[0].ID+"/complete", token, nil, nil, 200))
	if completedFile.Files[0].State != "completed" || completedFile.Counts.Accepted != 2 {
		t.Fatal("EOF final row was not accepted", completedFile)
	}
	importTestRequest(t, s, "POST", importTestBase(project, job.ID)+"/files/"+job.Files[0].ID+"/complete", token, nil, nil, 200)
	completed := importTestJob(t, importTestRequest(t, s, "POST", importTestBase(project, job.ID)+"/complete", token, nil, nil, 200))
	if completed.State != "completed" {
		t.Fatal("import did not complete")
	}
	var retained int
	s.db.QueryRow(`SELECT sum(length(tail)+length(hash_state)) FROM import_upload_files`).Scan(&retained)
	if retained != 0 {
		t.Fatal("completed import retained raw resumability data")
	}
	batch, err := s.deliveryBatch(t.Context())
	if err != nil || len(batch) != 2 {
		t.Fatal(err)
	}
	for _, event := range batch {
		if !event.Imported || event.Timestamp != "2020-01-02T03:04:05Z" {
			t.Fatal("archive timestamp or import isolation marker lost")
		}
	}
}

func TestImportUploadConflictDigestAndQueueAtomicity(t *testing.T) {
	t.Parallel()
	s := service(t, 1)
	project, _ := project(t, s)
	_, token := key(t, s, []string{"imports:write", "events:write"}, []string{project})
	data := append(importTestRow("one"), importTestRow("two")...)
	file := importer.ImportUploadFileSpec{Path: "events.jsonl", Kind: "events", SizeBytes: int64(len(data)), SHA256: importTestSHA(data)}
	job := importTestCreate(t, s, project, token, "queue", []importer.ImportUploadFileSpec{file})
	importTestPut(t, s, project, job, job.Files[0], token, 0, data, 503)
	status, _ := s.imports.loadImportJob(t.Context(), project, job.ID)
	if status.Files[0].OffsetBytes != 0 || status.Counts.Rows != 0 || eventCount(t, s) != 0 {
		t.Fatal("queue pressure partially committed rows/cursor")
	}
	s.queueMax = 10
	importTestRequest(t, s, "PUT", importTestBase(project, job.ID)+"/files/"+job.Files[0].ID, token, data, map[string]string{"Upload-Offset": "0", "X-Chunk-SHA256": strings.Repeat("0", 64)}, 400)
	importTestPut(t, s, project, job, job.Files[0], token, 1, data[:10], 409)
	importTestPut(t, s, project, job, job.Files[0], token, 0, data, 200)
	changed := append([]byte(nil), data...)
	changed[1] = 'x'
	importTestPut(t, s, project, job, job.Files[0], token, 0, changed, 409)
	wrong := file
	wrong.SHA256 = strings.Repeat("a", 64)
	wrong.Path = "wrong.jsonl"
	bad := importTestCreate(t, s, project, token, "bad-digest", []importer.ImportUploadFileSpec{wrong})
	importTestPut(t, s, project, bad, bad.Files[0], token, 0, data, 200)
	importTestRequest(t, s, "POST", importTestBase(project, bad.ID)+"/files/"+bad.Files[0].ID+"/complete", token, nil, nil, 409)
	failed, _ := s.imports.loadImportJob(t.Context(), project, bad.ID)
	if failed.State != "failed" || failed.Error != "sha256_mismatch_partial_data_retained" || failed.Counts.Duplicates != 2 || eventCount(t, s) != 2 {
		t.Fatal("final hash failure concealed durable partial ingestion", failed)
	}
	var retained int
	s.db.QueryRow(`SELECT length(tail)+length(hash_state) FROM import_upload_files WHERE id=?`, bad.Files[0].ID).Scan(&retained)
	if retained != 0 {
		t.Fatal("failed import retained partial raw data")
	}
}

func TestImportUploadOrderCancellationAndBoundedSamples(t *testing.T) {
	t.Parallel()
	s := service(t, 2000)
	project, _ := project(t, s)
	_, token := key(t, s, []string{"imports:write", "events:write"}, []string{project})
	data := append([]byte(strings.Repeat("{\"private\":\"must-not-be-in-status\"}\n", 50)), importTestRow("valid")...)
	job := importTestCreate(t, s, project, token, "order", []importer.ImportUploadFileSpec{{Path: "z-events.jsonl", Kind: "events", SizeBytes: int64(len(data)), SHA256: importTestSHA(data)}, {Path: "a-persons.jsonl", Kind: "persons", SizeBytes: 0, SHA256: importTestSHA(nil)}})
	if job.Files[0].Kind != "persons" {
		t.Fatal("persons are not first in manifest upload order")
	}
	importTestPut(t, s, project, job, job.Files[1], token, 0, data, 409)
	importTestRequest(t, s, "POST", importTestBase(project, job.ID)+"/files/"+job.Files[0].ID+"/complete", token, nil, nil, 200)
	imported := importTestJob(t, importTestPut(t, s, project, job, job.Files[1], token, 0, data, 200))
	if imported.Counts.Invalid != 50 || imported.Counts.Accepted != 1 || len(imported.Samples) != importer.MaxSamples {
		t.Fatal("invalid row counts or sample budget wrong", imported.Counts)
	}
	encoded, _ := json.Marshal(imported)
	if bytes.Contains(encoded, []byte("must-not-be-in-status")) {
		t.Fatal("status leaked invalid row data")
	}
	partialData := []byte(`{"uuid":"partial-secret`)
	partial := importTestCreate(t, s, project, token, "cancel", []importer.ImportUploadFileSpec{{Path: "partial.jsonl", Kind: "events", SizeBytes: int64(len(partialData) + 10), SHA256: strings.Repeat("a", 64)}})
	importTestPut(t, s, project, partial, partial.Files[0], token, 0, partialData, 200)
	cancelled := importTestJob(t, importTestRequest(t, s, "DELETE", importTestBase(project, partial.ID), token, nil, nil, 200))
	if cancelled.State != "cancelled" {
		t.Fatal("cancel did not persist")
	}
	var retained int
	s.db.QueryRow(`SELECT length(tail)+length(hash_state) FROM import_upload_files WHERE id=?`, partial.Files[0].ID).Scan(&retained)
	if retained != 0 {
		t.Fatal("cancellation retained private partial row")
	}
	importTestPut(t, s, project, partial, partial.Files[0], token, 0, partialData, 409)
	if eventCount(t, s) != 1 {
		t.Fatal("cancellation erased already accepted data")
	}
}

func TestImportUploadAuthorizationAndManifestBounds(t *testing.T) {
	t.Parallel()
	s := service(t, 100)
	other, _ := project(t, s)
	project, public := project(t, s)
	_, eventWriter := key(t, s, []string{"events:write"}, []string{project})
	_, writer := key(t, s, []string{"imports:write", "events:write"}, []string{project})
	_, onlyImport := key(t, s, []string{"imports:write"}, []string{project})
	peerID, peer := key(t, s, []string{"imports:write", "events:write"}, []string{project})
	data := importTestRow("row")
	manifest := map[string]any{"idempotency_key": "auth", "files": []importer.ImportUploadFileSpec{{Path: "file.jsonl", Kind: "events", SizeBytes: int64(len(data)), SHA256: importTestSHA(data)}}}
	raw, _ := json.Marshal(manifest)
	base := "/api/v1/projects/" + project + "/imports"
	importTestRequest(t, s, "POST", base, "", raw, nil, 401)
	importTestRequest(t, s, "POST", base, public, raw, nil, 401)
	importTestRequest(t, s, "POST", base, eventWriter, raw, nil, 403)
	importTestRequest(t, s, "POST", base, onlyImport, raw, nil, 403)
	job := importTestCreate(t, s, project, writer, "auth", manifest["files"].([]importer.ImportUploadFileSpec))
	importTestRequest(t, s, "GET", importTestBase(other, job.ID), writer, nil, nil, 403)
	importTestRequest(t, s, "GET", importTestBase(other, job.ID), adminToken, nil, nil, 404)
	importTestRequest(t, s, "GET", importTestBase(project, job.ID), peer, nil, nil, 200)
	importTestPut(t, s, project, job, job.Files[0], peer, 0, data, 403)
	request(t, s, "DELETE", "/api/v1/access-keys/"+peerID, adminToken, "", 204)
	importTestRequest(t, s, "GET", importTestBase(project, job.ID), peer, nil, nil, 401)
	for i, label := range []string{"../escape.jsonl", "/absolute.jsonl", "x/../escape.jsonl", "x\\file.jsonl"} {
		manifest["idempotency_key"] = fmt.Sprint("invalid-", i)
		manifest["files"] = []importer.ImportUploadFileSpec{{Path: label, Kind: "events", SHA256: importTestSHA(nil)}}
		raw, _ = json.Marshal(manifest)
		importTestRequest(t, s, "POST", base, writer, raw, nil, 400)
	}
	changed := map[string]any{"idempotency_key": "auth", "files": []importer.ImportUploadFileSpec{{Path: "other.jsonl", Kind: "events", SHA256: importTestSHA(nil)}}}
	raw, _ = json.Marshal(changed)
	importTestRequest(t, s, "POST", base, writer, raw, nil, 409)
	for i := 0; i < 3; i++ {
		importTestCreate(t, s, project, writer, fmt.Sprint("cap-", i), []importer.ImportUploadFileSpec{{Path: "empty.jsonl", Kind: "events", SHA256: importTestSHA(nil)}})
	}
	raw, _ = json.Marshal(map[string]any{"idempotency_key": "fifth", "files": []importer.ImportUploadFileSpec{{Path: "empty.jsonl", Kind: "events", SHA256: importTestSHA(nil)}}})
	importTestRequest(t, s, "POST", base, writer, raw, nil, 503)
}

func TestImportUploadOAuthRequiresAPIAudience(t *testing.T) {
	t.Parallel()
	s, _ := oauthProtocolService(t)
	project, _ := oauthProject(t, s, accountTestToken(t, s))
	clientID, secret := oauthProtocolClient(t, s, []string{"imports:write", "events:write"}, []string{project})
	form := oauthGrantForm()
	form.Set("resource", s.resources.apiResource)
	access := oauthProtocolRequest(t, s, clientID, secret, form, 200)["access_token"].(string)
	form.Set("resource", s.resources.mcpResource)
	mcp := oauthProtocolRequest(t, s, clientID, secret, form, 200)["access_token"].(string)
	files := []importer.ImportUploadFileSpec{{Path: "empty.jsonl", Kind: "events", SHA256: importTestSHA(nil)}}
	body, _ := json.Marshal(map[string]any{"idempotency_key": "oauth", "files": files})
	base := "/api/v1/projects/" + project + "/imports"
	importTestRequest(t, s, "POST", base, mcp, body, nil, 401)
	job := importTestJob(t, importTestRequest(t, s, "POST", base, access, body, nil, 201))
	if job.ID == "" {
		t.Fatal("project API OAuth import failed")
	}
	legacy := token()
	if _, err := s.control.Exec(`INSERT INTO access_keys(id,name,token_hash,permissions,projects,created_at) VALUES('legacy-importer','Legacy importer',?,'["imports:write","events:write"]',?,?)`, hash(legacy), tenantTestBody(t, []string{project}), now()); err != nil {
		t.Fatal(err)
	}
	importTestRequest(t, s, "GET", base, legacy, nil, nil, 401)
}

func TestImportUploadOversizedRowsRemainBoundedAndResume(t *testing.T) {
	t.Parallel()
	s := service(t, 100)
	project, _ := project(t, s)
	_, token := key(t, s, []string{"imports:write", "events:write"}, []string{project})
	data := append([]byte(strings.Repeat("x", importer.MaxRowBytes+100)+"\n"), importTestRow("after-oversize")...)
	job := importTestCreate(t, s, project, token, "oversize", []importer.ImportUploadFileSpec{{Path: "oversize.jsonl", Kind: "events", SizeBytes: int64(len(data)), SHA256: importTestSHA(data)}})
	for offset := 0; offset < len(data); {
		end := min(offset+importer.MaxChunkBytes, len(data))
		importTestPut(t, s, project, job, job.Files[0], token, int64(offset), data[offset:end], 200)
		var retained int
		s.db.QueryRow(`SELECT length(tail) FROM import_upload_files WHERE id=?`, job.Files[0].ID).Scan(&retained)
		if retained > importer.MaxRowBytes {
			t.Fatal("oversized row exceeded durable staging cap")
		}
		offset = end
	}
	complete := importTestJob(t, importTestRequest(t, s, "POST", importTestBase(project, job.ID)+"/files/"+job.Files[0].ID+"/complete", token, nil, nil, 200))
	if complete.Counts.Invalid != 1 || complete.Counts.Accepted != 1 || complete.Counts.Rows != 2 || complete.Samples[0].Reason != "row_too_large" {
		t.Fatal("oversized row was not safely skipped", complete.Counts)
	}
}

type importBlockingBody struct {
	reader           io.Reader
	entered, release chan struct{}
	once             sync.Once
}

func (b *importBlockingBody) Read(p []byte) (int, error) {
	b.once.Do(func() { close(b.entered); <-b.release })
	return b.reader.Read(p)
}
func (*importBlockingBody) Close() error { return nil }

func TestImportUploadRevocationAndCancellationDuringBodyRead(t *testing.T) {
	t.Parallel()
	for _, action := range []string{"revoke", "cancel"} {
		t.Run(action, func(t *testing.T) {
			s := service(t, 20)
			project, _ := project(t, s)
			actorID, token := key(t, s, []string{"imports:write", "events:write"}, []string{project})
			data := importTestRow("new-row")
			job := importTestCreate(t, s, project, token, "blocked", []importer.ImportUploadFileSpec{{Path: "events.jsonl", Kind: "events", SizeBytes: int64(len(data)), SHA256: importTestSHA(data)}})
			body := &importBlockingBody{reader: bytes.NewReader(data), entered: make(chan struct{}), release: make(chan struct{})}
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(body.release) }) }
			defer unblock()
			done := make(chan *httptest.ResponseRecorder, 1)
			handler := s.Handler()
			go func() {
				r := httptest.NewRequest("PUT", importTestBase(project, job.ID)+"/files/"+job.Files[0].ID, bytes.NewReader(data))
				r.Body = body
				r.Header.Set("Authorization", "Bearer "+token)
				r.Header.Set("Content-Type", "application/octet-stream")
				r.Header.Set("Upload-Offset", "0")
				r.Header.Set("X-Chunk-SHA256", importTestSHA(data))
				w := httptest.NewRecorder()
				handler.ServeHTTP(w, r)
				done <- w
			}()
			select {
			case <-body.entered:
			case <-time.After(3 * time.Second):
				t.Fatal("upload did not reach body decoding after authorization")
			}
			want := 409
			if action == "revoke" {
				request(t, s, "DELETE", "/api/v1/access-keys/"+actorID, adminToken, "", 204)
				want = 403
			} else {
				importTestRequest(t, s, "DELETE", importTestBase(project, job.ID), token, nil, nil, 200)
			}
			unblock()
			select {
			case response := <-done:
				if response.Code != want {
					t.Fatalf("race returned%d want%d: %s", response.Code, want, response.Body.String())
				}
			case <-time.After(3 * time.Second):
				t.Fatal("upload did not stop after authority/state changed")
			}
			status, err := s.imports.loadImportJob(t.Context(), project, job.ID)
			if err != nil || status.Files[0].OffsetBytes != 0 || status.Counts.Accepted != 0 || eventCount(t, s) != 0 {
				t.Fatal("race committed unauthorized rows or progress", err)
			}
		})
	}
}

func TestImportUploadConcurrentChunksCommitExactlyOnce(t *testing.T) {
	t.Parallel()
	s := service(t, 100)
	project, _ := project(t, s)
	_, token := key(t, s, []string{"imports:write", "events:write"}, []string{project})
	data := importTestRow("concurrent")
	job := importTestCreate(t, s, project, token, "concurrent", []importer.ImportUploadFileSpec{{Path: "events.jsonl", Kind: "events", SizeBytes: int64(len(data)), SHA256: importTestSHA(data)}})
	handler := s.Handler()
	responses := make(chan int, 12)
	for range 12 {
		go func() {
			r := httptest.NewRequest("PUT", importTestBase(project, job.ID)+"/files/"+job.Files[0].ID, bytes.NewReader(data))
			r.Header.Set("Authorization", "Bearer "+token)
			r.Header.Set("Content-Type", "application/octet-stream")
			r.Header.Set("Upload-Offset", "0")
			r.Header.Set("X-Chunk-SHA256", importTestSHA(data))
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			responses <- w.Code
		}()
	}
	success := 0
	for range 12 {
		status := <-responses
		if status == 200 {
			success++
		} else if status != 409 && status != 429 {
			t.Fatal("unexpected concurrent upload status", status)
		}
	}
	current, err := s.imports.loadImportJob(t.Context(), project, job.ID)
	if err != nil || success == 0 || current.Counts.Accepted != 1 || current.Counts.Rows != 1 || current.Files[0].OffsetBytes != int64(len(data)) || eventCount(t, s) != 1 {
		t.Fatal("concurrent upload did not commit exactly once", err, current.Counts)
	}
}

func TestImportUploadFreezesBoundSourceAcrossAdminResumeAndLiveReplay(t *testing.T) {
	t.Parallel()
	s := service(t, 100)
	project, _ := project(t, s)
	createWriter := func(source string) string {
		payload, _ := json.Marshal(map[string]any{"name": "bound importer", "permissions": []string{"imports:write", "events:write"}, "projects": []string{project}, "source_id": source})
		created := request(t, s, "POST", "/api/v1/access-keys", adminToken, string(payload), 201)
		return created["token"].(string)
	}
	warehouse := createWriter("warehouse")
	other := createWriter("other-source")
	request(t, s, "POST", "/capture/", "", captureBody(warehouse, "same-live"), 200)
	data := append(importTestRow("same-live"), importTestRow("import-first")...)
	spec := []importer.ImportUploadFileSpec{{Path: "events.jsonl", Kind: "events", SizeBytes: int64(len(data)), SHA256: importTestSHA(data)}}
	job := importTestCreate(t, s, project, warehouse, "bound", spec)
	if job.SourceID != "warehouse" {
		t.Fatal("job did not freeze authenticated creator source")
	}
	importTestPut(t, s, project, job, job.Files[0], other, 0, data, 403)
	imported := importTestJob(t, importTestPut(t, s, project, job, job.Files[0], adminToken, 0, data, 200))
	if imported.SourceID != "warehouse" || imported.Counts.Accepted != 1 || imported.Counts.Duplicates != 1 {
		t.Fatal("admin resume changed source or failed live/import dedup", imported)
	}
	importTestRequest(t, s, "POST", importTestBase(project, job.ID)+"/files/"+job.Files[0].ID+"/complete", adminToken, nil, nil, 200)
	importTestRequest(t, s, "POST", importTestBase(project, job.ID)+"/complete", adminToken, nil, nil, 200)
	native := []byte(`{"type":"track","message_id":"import-first","name":"purchase","actor_id":"customer"}`)
	response := importTestRequest(t, s, "POST", "/ingest/v1/events", warehouse, native, nil, 200)
	var replay map[string]any
	json.Unmarshal(response.Body.Bytes(), &replay)
	if replay["duplicates"] != float64(1) || eventCount(t, s) != 2 {
		t.Fatal("live native replay failed frozen import source dedup")
	}
	foreign := importTestCreate(t, s, project, other, "bound", spec)
	added := importTestJob(t, importTestPut(t, s, project, foreign, foreign.Files[0], other, 0, data, 200))
	if added.SourceID != "other-source" || added.Counts.Accepted != 2 || eventCount(t, s) != 4 {
		t.Fatal("different source was not independent")
	}
	batch, err := s.deliveryBatch(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	bySource := map[string]int{}
	for _, event := range batch {
		bySource[event.SourceID]++
	}
	if bySource["warehouse"] != 2 || bySource["other-source"] != 2 {
		t.Fatal("bound source provenance was lost", bySource)
	}
}

func TestImportUploadInvalidBoundMetadataDoesNotPoisonFile(t *testing.T) {
	t.Parallel()
	s := service(t, 100)
	project, _ := project(t, s)
	payload, _ := json.Marshal(map[string]any{"name": "bound", "permissions": []string{"imports:write", "events:write"}, "projects": []string{project}, "source_id": "warehouse"})
	created := request(t, s, "POST", "/api/v1/access-keys", adminToken, string(payload), 201)
	token := created["token"].(string)
	bad, _ := json.Marshal(map[string]any{"uuid": "bad-context", "event": "page", "distinct_id": "customer", "timestamp": "2020-01-02T03:04:05Z", "properties": map[string]any{"$current_url": "https://example.test/" + strings.Repeat("x", 70000)}})
	data := append(append(bad, '\n'), importTestRow("valid-after-context")...)
	job := importTestCreate(t, s, project, token, "invalid-context", []importer.ImportUploadFileSpec{{Path: "events.jsonl", Kind: "events", SizeBytes: int64(len(data)), SHA256: importTestSHA(data)}})
	imported := importTestJob(t, importTestPut(t, s, project, job, job.Files[0], token, 0, data, 200))
	if imported.Counts.Rows != 2 || imported.Counts.Invalid != 1 || imported.Counts.Accepted != 1 || len(imported.Samples) != 1 || imported.Samples[0].Reason != "invalid_metadata" || eventCount(t, s) != 1 {
		t.Fatal("bad bound row metadata poisoned unrelated valid archive row", imported)
	}
	importTestRequest(t, s, "POST", importTestBase(project, job.ID)+"/files/"+job.Files[0].ID+"/complete", token, nil, nil, 200)
}
