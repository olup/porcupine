package porcupine

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Multipart behavior comes from the existing S3 HTTP fixture. This wrapper adds
// immutable commit markers and a fault hook at the actual publication boundary.
type pipelineRecoveryS3 struct {
	data       exportTestS3
	mu         sync.Mutex
	markers    map[string][]byte
	markerPuts int
	beforePut  func([]byte)
}

func (f *pipelineRecoveryS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !strings.HasSuffix(r.URL.Path, "/manifest.json") {
		f.data.ServeHTTP(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/xml")
	switch r.Method {
	case http.MethodGet:
		f.mu.Lock()
		data, exists := f.markers[r.URL.Path]
		f.mu.Unlock()
		if !exists {
			w.WriteHeader(http.StatusNotFound)
			io.WriteString(w, `<Error><Code>NoSuchKey</Code></Error>`)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(data)
	case http.MethodPut:
		data, err := io.ReadAll(io.LimitReader(r.Body, 4<<20))
		if err != nil || r.Header.Get("If-None-Match") != "*" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if f.beforePut != nil {
			f.beforePut(data)
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		if _, exists := f.markers[r.URL.Path]; exists {
			w.WriteHeader(http.StatusPreconditionFailed)
			io.WriteString(w, `<Error><Code>PreconditionFailed</Code></Error>`)
			return
		}
		f.markerPuts++
		f.markers[r.URL.Path] = append([]byte(nil), data...)
		w.Header().Set("ETag", `"immutable-marker"`)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

type pipelineRecoveryFixture struct {
	s                 *Service
	s3                *pipelineRecoveryS3
	clockMS           atomic.Int64
	queryBody         atomic.Value
	queries           atomic.Int32
	project, pipeline string
	start             time.Time
}

func newPipelineRecoveryFixture(t *testing.T, body string) *pipelineRecoveryFixture {
	t.Helper()
	f := &pipelineRecoveryFixture{s: service(t, 100), s3: &pipelineRecoveryS3{markers: map[string][]byte{}}, start: time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)}
	f.clockMS.Store(f.start.Add(61 * time.Second).UnixMilli())
	f.queryBody.Store(body)
	// The ordinary service starts without destinations or ClickHouse. Install all
	// dependencies before explicitly stepping the worker, avoiding scheduler races.
	enableTestPipelines(t, f.s)
	f.s.clock = func() time.Time { return time.UnixMilli(f.clockMS.Load()).UTC() }
	server := httptest.NewServer(f.s3)
	t.Cleanup(server.Close)
	f.s.pipelines.s3Origins[server.URL] = true
	limits, err := normalizeExportConfig(ExportConfig{FileBytes: 1024, BandwidthBytesPerSecond: 100 << 20, MaxExecutionSeconds: 10})
	if err != nil {
		t.Fatal(err)
	}
	f.s.pipelines.config.ExportLimits = limits
	ch := testClickHouse(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.Header.Get("X-ClickHouse-User"), "_pipeline_export") {
			f.queries.Add(1)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			io.WriteString(w, f.queryBody.Load().(string))
		}
	})
	configureAnalyticsTest(f.s, ch)
	f.project, _ = project(t, f.s)
	_, owner := key(t, f.s, []string{"events:read", "pipelines:write", "pipelines:read"}, []string{f.project})
	spec := testS3Spec()
	spec.SQL = "SELECT uuid, repeat(event,100) AS note FROM events ORDER BY uuid"
	spec.Destination.Endpoint = server.URL
	bodyBytes, err := json.Marshal(PipelineCreateBody{Name: "recovery contract", Kind: "s3", S3: &spec, Credentials: PipelineCredentials{AccessKeyID: "recovery-test-key", SecretAccessKey: "recovery-test-secret"}})
	if err != nil {
		t.Fatal(err)
	}
	created := request(t, f.s, "POST", "/api/v1/projects/"+f.project+"/pipelines", owner, string(bodyBytes), 201)
	f.pipeline = created["id"].(string)
	return f
}

func pipelineRecoveryBody(t *testing.T) string {
	t.Helper()
	data, err := json.Marshal(struct {
		Meta []AnalyticsResultColumn `json:"meta"`
		Data [][]string              `json:"data"`
		Rows int                     `json:"rows"`
	}{Meta: []AnalyticsResultColumn{{Name: "uuid", Type: "String"}, {Name: "note", Type: "String"}}, Data: [][]string{{"a", strings.Repeat("a", 750)}, {"b", strings.Repeat("b", 750)}, {"c", strings.Repeat("c", 750)}}, Rows: 3})
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func (f *pipelineRecoveryFixture) step(t *testing.T) {
	t.Helper()
	if progress, err := f.s.stepPipelineS3(context.Background()); err != nil || !progress {
		t.Fatalf("S3 worker step: progress=%v err=%v", progress, err)
	}
}

type pipelineRecoveryState struct {
	id, state, attempt, manifest string
	attempts, files              int
	cursor, rows                 int64
}

func (f *pipelineRecoveryFixture) state(t *testing.T) pipelineRecoveryState {
	t.Helper()
	var state pipelineRecoveryState
	err := f.s.db.QueryRow(`SELECT r.id,r.state,r.attempt_id,r.manifest,r.attempts,st.cursor_ms,r.row_count,(SELECT count(*) FROM pipeline_s3_files WHERE run_id=r.id) FROM pipeline_s3_runs r JOIN pipeline_s3_state st ON st.pipeline_id=r.pipeline_id WHERE r.pipeline_id=? AND r.window_start_ms=?`, f.pipeline, f.start.UnixMilli()).Scan(&state.id, &state.state, &state.attempt, &state.manifest, &state.attempts, &state.cursor, &state.rows, &state.files)
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func TestPipelineS3RecoveryCommittedMarkerFinalizesWithoutRequery(t *testing.T) {
	f := newPipelineRecoveryFixture(t, pipelineRecoveryBody(t))
	var checkpointSeen atomic.Bool
	f.s3.beforePut = func(marker []byte) {
		var checkpoint, state string
		var cursor int64
		err := f.s.db.QueryRow(`SELECT r.manifest,r.state,st.cursor_ms FROM pipeline_s3_runs r JOIN pipeline_s3_state st ON st.pipeline_id=r.pipeline_id WHERE r.pipeline_id=?`, f.pipeline).Scan(&checkpoint, &state, &cursor)
		if err != nil || checkpoint != string(marker) || state != "running" || cursor != f.start.UnixMilli() {
			t.Error("remote publication began without a durable validated manifest checkpoint")
		}
		checkpointSeen.Store(true)
		// Fail the real final SQLite transaction after the remote marker commits.
		// The trigger affects only this temporary database and is removed for retry.
		if _, err := f.s.db.Exec(`CREATE TRIGGER recovery_fail_cursor BEFORE UPDATE OF cursor_ms ON pipeline_s3_state BEGIN SELECT RAISE(ABORT,'injected final acknowledgment failure'); END`); err != nil {
			t.Error(err)
		}
	}
	f.step(t)
	first := f.state(t)
	if !checkpointSeen.Load() || first.state != "queued" || first.attempts != 1 || first.manifest == "" || first.cursor != f.start.UnixMilli() || first.files != 3 || f.queries.Load() != 1 {
		t.Fatal("lost SQLite acknowledgment discarded the checkpoint or advanced the cursor")
	}
	f.s3.mu.Lock()
	if len(f.s3.markers) != 1 || f.s3.markerPuts != 1 {
		t.Error("fault did not leave exactly one committed remote marker")
	}
	for _, marker := range f.s3.markers {
		if !bytes.Equal(marker, []byte(first.manifest)) {
			t.Error("remote commit marker differs from the durable checkpoint")
		}
	}
	f.s3.mu.Unlock()
	if _, err := f.s.db.Exec(`DROP TRIGGER recovery_fail_cursor`); err != nil {
		t.Fatal(err)
	}
	f.clockMS.Store(f.start.Add(64 * time.Second).UnixMilli())
	f.step(t)
	second := f.state(t)
	if second.state != "succeeded" || second.attempts != 2 || second.id != first.id || second.attempt != first.attempt || second.manifest != "" || second.files != 0 || second.rows != 3 || second.cursor != f.start.Add(time.Minute).UnixMilli() || f.queries.Load() != 1 {
		t.Fatal("committed checkpoint recovery re-executed SQL or lost its original window")
	}
	f.s3.mu.Lock()
	defer f.s3.mu.Unlock()
	if len(f.s3.markers) != 1 || f.s3.markerPuts != 1 {
		t.Fatal("recovery republished or replaced the immutable marker")
	}
	f.s3.data.mu.Lock()
	defer f.s3.data.mu.Unlock()
	if len(f.s3.data.objects) != 3 || f.s3.data.deletes != 0 {
		t.Fatal("checkpoint recovery removed committed data objects")
	}
}

func TestPipelineS3RecoveryLateHTTP200FailureDoesNotCommitWindow(t *testing.T) {
	valid := pipelineRecoveryBody(t)
	for _, fault := range []struct{ name, body string }{
		{"truncated footer", strings.TrimSuffix(valid, `,"rows":3}`)},
		{"late exception", valid + "\nCode: 241. DB::Exception: private upstream error"},
	} {
		t.Run(fault.name, func(t *testing.T) {
			f := newPipelineRecoveryFixture(t, fault.body)
			f.step(t)
			first := f.state(t)
			if first.state != "queued" || first.attempts != 1 || first.manifest != "" || first.cursor != f.start.UnixMilli() || first.files != 0 || f.queries.Load() != 1 {
				t.Fatal("HTTP 200 with an incomplete source advanced or checkpointed the window")
			}
			f.s3.mu.Lock()
			markers, puts := len(f.s3.markers), f.s3.markerPuts
			f.s3.mu.Unlock()
			f.s3.data.mu.Lock()
			completed, objects, uploads, aborted, deleted := f.s3.data.completes, len(f.s3.data.objects), len(f.s3.data.parts), f.s3.data.aborts, f.s3.data.deletes
			f.s3.data.mu.Unlock()
			if markers != 0 || puts != 0 || completed < 2 || objects != 0 || uploads != 0 || aborted == 0 || deleted < 2 {
				t.Fatal("late source failure published a marker or retained partial multipart data")
			}
			f.queryBody.Store(valid)
			f.clockMS.Store(f.start.Add(64 * time.Second).UnixMilli())
			f.step(t)
			second := f.state(t)
			if second.state != "succeeded" || second.attempts != 2 || second.id != first.id || second.attempt == first.attempt || second.manifest != "" || second.rows != 3 || second.cursor != f.start.Add(time.Minute).UnixMilli() || f.queries.Load() != 2 {
				t.Fatal("retry did not rerun the same uncommitted window with a fresh attempt")
			}
			f.s3.mu.Lock()
			defer f.s3.mu.Unlock()
			if len(f.s3.markers) != 1 || f.s3.markerPuts != 1 {
				t.Fatal("successful retry did not publish exactly one commit marker")
			}
		})
	}
}

func TestPipelineS3RecoveryForeignCommitMarkerIsNeverOverwritten(t *testing.T) {
	f := newPipelineRecoveryFixture(t, pipelineRecoveryBody(t))
	path := "/client-bucket/private/exports/" + f.pipeline + "/" + strconv.FormatInt(f.start.UnixMilli(), 10) + "/manifest.json"
	foreign := []byte(`{"run_id":"unrelated-committed-run"}`)
	f.s3.markers[path] = foreign
	f.step(t)
	state := f.state(t)
	if state.state != "queued" || state.manifest != "" || state.cursor != f.start.UnixMilli() || f.queries.Load() != 0 {
		t.Fatal("foreign commit marker was accepted or unnecessarily re-executed")
	}
	f.s3.mu.Lock()
	defer f.s3.mu.Unlock()
	if f.s3.markerPuts != 0 || !bytes.Equal(f.s3.markers[path], foreign) {
		t.Fatal("existing unrelated commit marker was replaced")
	}
}
