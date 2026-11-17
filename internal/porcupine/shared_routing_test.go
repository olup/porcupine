package porcupine

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestSharedRouteOriginValidation(t *testing.T) {
	for _, raw := range []string{"https://worker.internal", "https://worker.internal:8443/", "http://127.0.0.1:1234", "http://[::1]:1234", "http://localhost:1234"} {
		if _, err := validateSharedRouteURL(raw); err != nil {
			t.Fatal(raw, err)
		}
	}
	for _, raw := range []string{"http://worker.internal", "ftp://127.0.0.1", "https://user:secret@worker.internal", "https://worker.internal/path", "https://worker.internal?query=x", "https://worker.internal#fragment", "http://127.0.0.1.evil.test", "https://", "//worker.internal"} {
		if _, err := validateSharedRouteURL(raw); err == nil {
			t.Fatal("accepted unsafe worker origin", raw)
		}
	}
}

func TestSharedRouteBodyLimitIncludesLargeImportManifests(t *testing.T) {
	for _, cached := range []bool{false, true} {
		for _, size := range []int{maxBodyBytes + 1, sharedRouteMaxBodyBytes, sharedRouteMaxBodyBytes + 1} {
			body := bytes.Repeat([]byte{'x'}, size)
			r := httptest.NewRequest("POST", "http://public.example/api/v1/projects/project/imports", bytes.NewReader(body))
			if cached {
				r = sharedReplayRequest(r, body)
			}
			got, decoded, err := readSharedRouteBody(r)
			if size > sharedRouteMaxBodyBytes {
				if !errors.Is(err, errSharedRouteBodyLimit) {
					t.Fatal("oversized forwarding envelope was accepted", cached, size, err)
				}
				continue
			}
			if err != nil || len(got) != size || decoded != cached {
				t.Fatal("valid import envelope rejected or changed", cached, size, len(got), decoded, err)
			}
		}
	}
}

func TestSharedRouteLargeImportManifestForwarding(t *testing.T) {
	dsn := sharedPostgresDSN(t)
	var owner *Service
	var calls atomic.Int32
	target := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		owner.sharedRoutingMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, err := io.ReadAll(r.Body)
			if err != nil || len(body) != sharedRouteMaxBodyBytes {
				t.Error("forwarding changed the full-sized manifest", len(body), err)
			}
			calls.Add(1)
			w.WriteHeader(204)
		})).ServeHTTP(w, r)
	}))
	owner = sharedRoutingTestService(t, dsn, "owner", "http://"+target.Listener.Addr().String())
	target.Start()
	t.Cleanup(target.Close)
	edge := sharedRoutingTestService(t, dsn, "edge", "http://127.0.0.1:1")
	sharedRoutingProject(t, owner, "project")
	for _, size := range []int{sharedRouteMaxBodyBytes, sharedRouteMaxBodyBytes + 1} {
		r := httptest.NewRequest("POST", "http://public.example/api/v1/projects/project/imports", bytes.NewReader(bytes.Repeat([]byte{'x'}, size)))
		w := httptest.NewRecorder()
		if !edge.forwardSharedProject(w, r, "project") {
			t.Fatal("large manifest did not use its owner")
		}
		want := 204
		if size > sharedRouteMaxBodyBytes {
			want = 413
		}
		if w.Code != want {
			t.Fatal("wrong forwarding body boundary", size, w.Code, w.Body.String())
		}
	}
	// The receiving side independently applies the same transport bound even if
	// a peer presents a correctly signed oversized request.
	body := bytes.Repeat([]byte{'x'}, sharedRouteMaxBodyBytes+1)
	r := httptest.NewRequest("POST", "http://public.example/api/v1/projects/project/imports", bytes.NewReader(body))
	claim := sharedRouteClaim{Version: 1, Source: "edge", Target: "owner", Project: "project", Epoch: 1,
		IssuedAtMS: time.Now().UnixMilli(), Nonce: newID(), Method: r.Method, URI: r.URL.RequestURI(), Host: r.Host,
		Peer: "203.0.113.73", AuthHash: sharedRouteDigest(nil), BodyHash: sharedRouteDigest(body)}
	raw, _ := json.Marshal(claim)
	r.Header.Set(sharedRouteHeader, base64.RawURLEncoding.EncodeToString(raw))
	r.Header.Set(sharedRouteSignature, base64.RawURLEncoding.EncodeToString(sharedRouteMAC(owner.shared.config.RoutingSecret, raw)))
	w := httptest.NewRecorder()
	owner.sharedRoutingMiddleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("oversized signed body reached its operation")
	})).ServeHTTP(w, r)
	if w.Code != 413 || calls.Load() != 1 {
		t.Fatal("receiving transport accepted oversized body", w.Code, calls.Load(), w.Body.String())
	}
}

func sharedRoutingTestService(t *testing.T, dsn, worker, origin string) *Service {
	t.Helper()
	ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	control, err := openSharedControl(ctx, SharedConfig{DatabaseURL: dsn})
	if err != nil {
		t.Fatal(err)
	}
	queryConfig, _ := normalizeInteractiveQueries(nil)
	if err := initializeSharedControl(ctx, control, adminToken, queryConfig); err != nil {
		control.Close()
		t.Fatal(err)
	}
	local, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "journal.db"))
	if err != nil {
		control.Close()
		t.Fatal(err)
	}
	local.SetMaxOpenConns(1)
	if _, err := local.Exec(`CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);INSERT INTO metadata VALUES('initialized','1')`); err != nil {
		control.Close()
		local.Close()
		t.Fatal(err)
	}
	s := &Service{db: local, control: control, eventRules: &eventRules{clock: time.Now}, shared: &sharedRuntime{config: SharedConfig{DatabaseURL: dsn,
		WorkerID: worker, RouteURL: origin, RoutingSecret: bytes.Repeat([]byte{71}, 32)}}}
	if err := s.initializeSharedRouting(ctx); err != nil {
		control.Close()
		local.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { s.closeSharedRouting(); control.Close(); local.Close() })
	return s
}

func sharedRoutingProject(t *testing.T, owner *Service, project string) {
	t.Helper()
	if _, err := owner.control.Exec(`INSERT INTO projects(id,tenant_id,name,created_at,ingestion_mode) VALUES($1,'default','Routing test',$2,'mixed')`, project, now()); err != nil {
		t.Fatal(err)
	}
	if err := owner.assignSharedProjectOwner(context.Background(), project); err != nil {
		t.Fatal(err)
	}
}

func TestSharedRouteProxyBodyPeerAndMCPResource(t *testing.T) {
	dsn := sharedPostgresDSN(t)
	var owner *Service
	var calls atomic.Int32
	var captured atomic.Pointer[http.Request]
	target := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		copy := r.Clone(context.Background())
		copy.Body = io.NopCloser(bytes.NewReader(raw))
		captured.Store(copy)
		r.Body = io.NopCloser(bytes.NewReader(raw))
		owner.sharedRoutingMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if owner.forwardSharedProject(w, r, "project") {
				t.Error("owner request was forwarded again")
				return
			}
			mcp, _ := r.Context().Value(mcpDispatchKey{}).(bool)
			if r.Header.Get("Authorization") == "Bearer mcp-token" && !mcp {
				problem(w, 401, "incorrect OAuth resource")
				return
			}
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
				return
			}
			peer, err := owner.clientIP(r)
			if err != nil {
				t.Error(err)
				return
			}
			calls.Add(1)
			write(w, 200, map[string]any{"body": string(body), "peer": peer.String(), "mcp": mcp,
				"uri": r.URL.RequestURI(), "encoding": r.Header.Get("Content-Encoding"),
				"route_header": r.Header.Get(sharedRouteHeader), "authorization": r.Header.Get("Authorization")})
		})).ServeHTTP(w, r)
	}))
	owner = sharedRoutingTestService(t, dsn, "owner", "http://"+target.Listener.Addr().String())
	target.Start()
	t.Cleanup(target.Close)
	edge := sharedRoutingTestService(t, dsn, "edge", "http://127.0.0.1:1")
	sharedRoutingProject(t, owner, "project")
	for _, isMCP := range []bool{false, true} {
		r := httptest.NewRequest("POST", "http://public.example/api/v1/projects/project/imports?limit=1", strings.NewReader(`{"format":"native"}`))
		r.RemoteAddr = "203.0.113.73:1234"
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("User-Agent", "Porcupine-test")
		r.Header.Set("Authorization", "Bearer api-token")
		if isMCP {
			r = r.WithContext(context.WithValue(r.Context(), mcpDispatchKey{}, true))
			r.Header.Set("Authorization", "Bearer mcp-token")
		}
		w := httptest.NewRecorder()
		if !edge.forwardSharedProject(w, r, "project") || w.Code != 200 {
			t.Fatal("forward failed", w.Code, w.Body.String())
		}
		var result map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		if result["body"] != `{"format":"native"}` || result["peer"] != "203.0.113.73" || result["mcp"] != isMCP || result["route_header"] != "" || result["authorization"] != r.Header.Get("Authorization") {
			t.Fatal("forward changed request semantics", result)
		}
	}
	// Match the synthetic request built by the real MCP API dispatcher: relative
	// URL, no Host/RemoteAddr/User-Agent, and an explicitly empty query string.
	synthetic, err := http.NewRequestWithContext(context.WithValue(context.Background(), mcpDispatchKey{}, true), "GET", "/api/v1/projects/project/events?", nil)
	if err != nil {
		t.Fatal(err)
	}
	synthetic.Header.Set("Authorization", "Bearer mcp-token")
	synthetic.Header.Set("Content-Type", "application/json")
	syntheticResponse := httptest.NewRecorder()
	if !edge.forwardSharedProject(syntheticResponse, synthetic, "project") || syntheticResponse.Code != 200 {
		t.Fatal("synthetic MCP request lost signed wire metadata", syntheticResponse.Code, syntheticResponse.Body.String())
	}
	for _, path := range []string{"/v1/batch", "/capture/"} {
		segment := httptest.NewRequest("POST", "http://public.example"+path, nil)
		segment.SetBasicAuth("write-key", "")
		originalAuthorization := segment.Header.Get("Authorization")
		segment = sharedReplayRequest(segment, []byte(`{"batch":[]}`))
		segment.Header.Del("Authorization")
		response := httptest.NewRecorder()
		if !edge.forwardSharedProject(response, segment, "project") || response.Code != 200 {
			t.Fatal("stripped adapter authentication could not be forwarded", path, response.Code, response.Body.String())
		}
		var result map[string]any
		if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		want := ""
		if path == "/v1/batch" {
			want = originalAuthorization
		}
		if result["authorization"] != want {
			t.Fatal("wire authentication restoration escaped Segment", path, result)
		}
	}
	r := httptest.NewRequest("POST", "http://public.example/capture/?compression=gzip-js&other=value", strings.NewReader("already consumed"))
	r.RemoteAddr = "203.0.113.73:1234"
	r.Header.Set("Content-Encoding", "gzip")
	r = sharedReplayRequest(r, []byte(`{"event":"page_viewed"}`))
	w := httptest.NewRecorder()
	if !edge.forwardSharedProject(w, r, "project") || w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	var result map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result["body"] != `{"event":"page_viewed"}` || result["encoding"] != "" || result["uri"] != "/capture/?other=value" {
		t.Fatal("decoded ingestion replay kept compression metadata", result)
	}
	request := captured.Load()
	if request == nil {
		t.Fatal("missing captured signed request")
	}
	replay := httptest.NewRecorder()
	owner.sharedRoutingMiddleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Fatal("replayed signed request reached application") })).ServeHTTP(replay, request)
	if replay.Code != http.StatusConflict || calls.Load() != 6 {
		t.Fatal("forward proof replay was accepted", replay.Code, replay.Body.String())
	}
}

func TestSharedRouteRejectsTamperingExpiryAndStaleEpoch(t *testing.T) {
	dsn := sharedPostgresDSN(t)
	owner := sharedRoutingTestService(t, dsn, "owner", "http://127.0.0.1:1")
	edge := sharedRoutingTestService(t, dsn, "edge", "http://127.0.0.1:2")
	sharedRoutingProject(t, owner, "project")
	for _, tc := range []struct {
		name   string
		change func(*http.Request, *sharedRouteClaim)
		sign   bool
		status int
	}{
		{"invalid signature", func(r *http.Request, _ *sharedRouteClaim) { r.Header.Set(sharedRouteSignature, "invalid") }, false, 401},
		{"changed body", func(r *http.Request, _ *sharedRouteClaim) {
			r.Body = io.NopCloser(strings.NewReader(`{"changed":true}`))
		}, false, 401},
		{"changed method", func(r *http.Request, _ *sharedRouteClaim) { r.Method = "DELETE" }, false, 401},
		{"changed authorization", func(r *http.Request, _ *sharedRouteClaim) { r.Header.Set("Authorization", "Bearer other-token") }, false, 401},
		{"expired", func(_ *http.Request, c *sharedRouteClaim) { c.IssuedAtMS = time.Now().Add(-time.Minute).UnixMilli() }, true, 401},
		{"future", func(_ *http.Request, c *sharedRouteClaim) { c.IssuedAtMS = time.Now().Add(time.Minute).UnixMilli() }, true, 401},
		{"wrong target", func(_ *http.Request, c *sharedRouteClaim) { c.Target = "other-worker" }, true, 401},
		{"routing loop", func(_ *http.Request, c *sharedRouteClaim) { c.Source = c.Target }, true, 401},
		{"stale epoch", func(_ *http.Request, c *sharedRouteClaim) { c.Epoch++ }, true, 409},
		{"wrong project", func(_ *http.Request, c *sharedRouteClaim) { c.Project = "another-project" }, true, 409},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("POST", "http://public.example/api/v1/projects/project/imports", strings.NewReader(`{}`))
			r.Header.Set("Authorization", "Bearer original-token")
			claim := sharedRouteClaim{Version: 1, Source: edge.shared.config.WorkerID, Target: owner.shared.config.WorkerID,
				Project: "project", Epoch: 1, IssuedAtMS: time.Now().UnixMilli(), Nonce: newID(), Method: r.Method, URI: r.URL.RequestURI(),
				Host: r.Host, Peer: "203.0.113.73", AuthHash: sharedRouteDigest([]byte(r.Header.Get("Authorization"))), BodyHash: sharedRouteDigest([]byte(`{}`))}
			setProof := func() {
				raw, _ := json.Marshal(claim)
				r.Header.Set(sharedRouteHeader, base64.RawURLEncoding.EncodeToString(raw))
				r.Header.Set(sharedRouteSignature, base64.RawURLEncoding.EncodeToString(sharedRouteMAC(owner.shared.config.RoutingSecret, raw)))
			}
			setProof()
			tc.change(r, &claim)
			if tc.sign {
				setProof()
			}
			w := httptest.NewRecorder()
			owner.sharedRoutingMiddleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Fatal("invalid route reached application") })).ServeHTTP(w, r)
			if w.Code != tc.status {
				t.Fatal(w.Code, w.Body.String())
			}
		})
	}
	if _, err := owner.control.Exec(`INSERT INTO project_deletions(project_id,completed) VALUES('project',0)`); err != nil {
		t.Fatal(err)
	}
	if _, _, err := owner.sharedProjectOwner(context.Background(), "project"); err == nil {
		t.Fatal("deleting project retained ordinary owner access")
	}
	if worker, _, err := owner.sharedProjectOwner(allowSharedDeletedProject(context.Background()), "project"); err != nil || worker != "owner" {
		t.Fatal("deletion retry could not reach original owner", worker, err)
	}
	if err := owner.requireSharedProjectOwner(allowSharedDeletedProject(context.Background()), "project"); err == nil {
		t.Fatal("deletion-retry context granted background execution")
	}
}

func TestSharedRouteWorkerIdentityAndOwnerUnavailable(t *testing.T) {
	dsn := sharedPostgresDSN(t)
	owner := sharedRoutingTestService(t, dsn, "owner", "http://127.0.0.1:1")
	edge := sharedRoutingTestService(t, dsn, "edge", "http://127.0.0.1:2")
	sharedRoutingProject(t, owner, "project")
	if err := edge.assignSharedProjectOwner(context.Background(), "project"); err != nil {
		t.Fatal(err)
	}
	if worker, epoch, err := edge.sharedProjectOwner(context.Background(), "project"); err != nil || worker != "owner" || epoch != 1 {
		t.Fatal("assignment changed existing owner", worker, epoch, err)
	}
	if err := edge.requireSharedProjectOwner(context.Background(), "project"); err == nil {
		t.Fatal("non-owner worker retained background execution")
	}
	if err := owner.requireSharedProjectOwner(context.Background(), "project"); err != nil {
		t.Fatal("active owner could not execute project work", err)
	}
	r := httptest.NewRequest("POST", "http://public.example/capture/", strings.NewReader(`{}`))
	w := httptest.NewRecorder()
	if !edge.forwardSharedProject(w, r, "project") || w.Code != 503 || strings.Contains(w.Body.String(), "127.0.0.1") {
		t.Fatal("unavailable owner was not an opaque retryable error", w.Code, w.Body.String())
	}
	owner.closeSharedRouting()
	if err := owner.initializeSharedRouting(context.Background()); err != nil {
		t.Fatal("same worker volume could not restart", err)
	}
	if _, err := owner.sharedWorkerVolume(context.Background(), "new-worker-name"); err == nil {
		t.Fatal("renamed durable worker volume was accepted")
	}
	// A different volume cannot impersonate an existing worker, even after its
	// process releases the registration session lock.
	owner.closeSharedRouting()
	if _, err := edge.db.Exec(`UPDATE metadata SET value='owner' WHERE key='shared_worker_id'`); err != nil {
		t.Fatal(err)
	}
	edge.closeSharedRouting()
	edge.shared.config.WorkerID = "owner"
	if err := edge.initializeSharedRouting(context.Background()); err == nil {
		t.Fatal("different durable volume reused worker identity")
	}
}

func TestSharedRouteStripsUntrustedContextHeaders(t *testing.T) {
	for _, shared := range []bool{false, true} {
		s := &Service{}
		if shared {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s.shared = &sharedRuntime{routing: &sharedRoutingState{ctx: ctx}}
		}
		r := httptest.NewRequest("GET", "http://public.example/", nil)
		r.Header.Set("X-Porcupine-Route-MCP", "true")
		w := httptest.NewRecorder()
		s.sharedRoutingMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("X-Porcupine-Route-MCP") != "" || r.Context().Value(mcpDispatchKey{}) != nil {
				t.Fatal("untrusted routing header introduced MCP authority")
			}
			w.WriteHeader(204)
		})).ServeHTTP(w, r)
		if w.Code != 204 {
			t.Fatal(w.Code, w.Body.String())
		}
	}
}
