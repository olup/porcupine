package porcupine

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	lakeapi "github.com/olup/porcupine/internal/lake"
)

type sharedJourneyNode struct {
	service    *Service
	host       *httptest.Server
	controlDSN string
}

func sharedJourneyHTTP(t *testing.T, origin, method, path, credential string, body []byte, headers map[string]string, want int) map[string]any {
	t.Helper()
	r, err := http.NewRequestWithContext(t.Context(), method, origin+path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if len(body) > 0 {
		r.Header.Set("Content-Type", "application/json")
	}
	if credential != "" {
		r.Header.Set("Authorization", "Bearer "+credential)
	}
	for name, value := range headers {
		r.Header.Set(name, value)
	}
	client := &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Do(r)
	if err != nil {
		t.Fatal("shared HTTP request", method, path, err)
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	if err != nil || response.StatusCode != want {
		t.Fatalf("%s %s: status=%d want=%d error=%v body=%s", method, path, response.StatusCode, want, err, raw)
	}
	result := map[string]any{}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &result); err != nil {
			t.Fatalf("%s %s returned non-JSON: %s %v", method, path, raw, err)
		}
	}
	return result
}

func sharedJourneyProject(t *testing.T, origin, tenant, name string) (string, string) {
	t.Helper()
	raw, _ := json.Marshal(map[string]any{"name": name, "tenant_id": tenant})
	created := sharedJourneyHTTP(t, origin, "POST", "/api/v1/projects", adminToken, raw, nil, 201)
	return created["project"].(map[string]any)["id"].(string), created["ingest_token"].(string)
}

func sharedJourneyKey(t *testing.T, origin, project string, permissions []string) (string, string) {
	t.Helper()
	raw, _ := json.Marshal(map[string]any{"name": "shared journey reader", "permissions": permissions, "projects": []string{project}})
	created := sharedJourneyHTTP(t, origin, "POST", "/api/v1/access-keys", adminToken, raw, nil, 201)
	return created["id"].(string), created["token"].(string)
}

func sharedJourneyNative(t *testing.T, origin, token, body string, want int) map[string]any {
	t.Helper()
	return sharedJourneyHTTP(t, origin, "POST", "/ingest/v1/events", "", []byte(body), map[string]string{"X-Porcupine-Key": token}, want)
}

type sharedJourneyProcessConfig struct {
	DatabaseURL         string
	OAuthIssuer         string
	ReadyFile           string
	HostedLake          *lakeapi.DuckLakeConfig
	HostedFlushInterval time.Duration
}

func TestSharedWorkerProcessHelper(t *testing.T) {
	t.Parallel()
	config := os.Getenv("PORCUPINE_SHARED_JOURNEY_HELPER")
	if config == "" {
		t.Skip("subprocess helper")
	}
	var input sharedJourneyProcessConfig
	if err := json.Unmarshal([]byte(config), &input); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	origin := "http://" + listener.Addr().String()
	cfg := Config{Profile: ProfileHosted, AcceptanceMode: AcceptanceModeBuffered, BootstrapToken: adminToken,
		Shared: &SharedConfig{DatabaseURL: input.DatabaseURL}, DuckLake: input.HostedLake}
	if input.OAuthIssuer != "" {
		cfg.OAuth = &OAuthConfig{Issuer: input.OAuthIssuer}
	}
	s, err := Open(cfg)
	if err != nil {
		listener.Close()
		t.Fatal("start shared subprocess", err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Error("worker close:", err)
		}
	}()
	// Change the timer before the fixture accepts events. The scheduler reads
	// the interval under the same lock.
	if input.HostedLake != nil && input.HostedFlushInterval > 0 {
		buffer := s.hosted.buffer
		buffer.mu.Lock()
		buffer.config.flushInterval = input.HostedFlushInterval
		buffer.mu.Unlock()
	}
	server := &http.Server{Handler: s.Handler(), ReadHeaderTimeout: 5 * time.Second}
	defer server.Close()
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	if err := os.WriteFile(input.ReadyFile, []byte(origin), 0600); err != nil {
		t.Fatal(err)
	}
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	defer signal.Stop(interrupt)
	select {
	case <-interrupt:
		s.BeginDrain()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		server.Shutdown(ctx)
	case err := <-done:
		t.Fatal("shared subprocess HTTP listener stopped", err)
	}
}

type sharedJourneyProcess struct {
	origin string
	cmd    *exec.Cmd
	done   chan struct{}
	stop   atomic.Bool
	err    error
	log    string
}

func startSharedJourneyProcess(t *testing.T, config sharedJourneyProcessConfig) *sharedJourneyProcess {
	t.Helper()
	config.ReadyFile = filepath.Join(t.TempDir(), "ready")
	raw, _ := json.Marshal(config)
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	timeout := 60 * time.Second
	if config.HostedFlushInterval > 0 {
		timeout += 2 * config.HostedFlushInterval
	}
	command := exec.Command(binary, "-test.run=^TestSharedWorkerProcessHelper$", "-test.timeout="+timeout.String())
	command.Env = append(os.Environ(), "PORCUPINE_SHARED_JOURNEY_HELPER="+string(raw))
	worker := launchSharedJourneyProcess(t, command)
	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	for {
		if ready, err := os.ReadFile(config.ReadyFile); err == nil && strings.HasPrefix(string(ready), "http://127.0.0.1:") {
			worker.origin = string(ready)
			sharedJourneyHTTP(t, worker.origin, "GET", "/readyz", "", nil, nil, 200)
			return worker
		}
		select {
		case <-worker.done:
			diagnostic, _ := os.ReadFile(worker.log)
			t.Fatal("shared subprocess exited before readiness", string(diagnostic))
		case <-deadline.C:
			t.Fatal("shared subprocess did not become ready")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func launchSharedJourneyProcess(t *testing.T, command *exec.Cmd) *sharedJourneyProcess {
	t.Helper()
	logfile, err := os.Create(filepath.Join(t.TempDir(), "worker.log"))
	if err != nil {
		t.Fatal(err)
	}
	command.Stdout, command.Stderr = logfile, logfile
	if err := command.Start(); err != nil {
		logfile.Close()
		t.Fatal(err)
	}
	worker := &sharedJourneyProcess{cmd: command, done: make(chan struct{}), log: logfile.Name()}
	go func() { worker.err = command.Wait(); logfile.Close(); close(worker.done) }()
	t.Cleanup(func() { worker.stopProcess(t, false) })
	return worker
}

func (worker *sharedJourneyProcess) stopProcess(t *testing.T, crash bool) {
	t.Helper()
	if !worker.stop.CompareAndSwap(false, true) {
		return
	}
	if crash {
		worker.cmd.Process.Kill()
	} else {
		worker.cmd.Process.Signal(os.Interrupt)
	}
	select {
	case <-worker.done:
		if !crash && worker.err != nil {
			diagnostic, _ := os.ReadFile(worker.log)
			t.Error("worker did not stop cleanly", worker.err, string(diagnostic))
		}
	case <-time.After(50 * time.Second):
		worker.cmd.Process.Kill()
		<-worker.done
		t.Error("worker did not stop before the shutdown deadline")
	}
}

func hostedJourneyConfig(t *testing.T, control string, fixture duckLakeS3Fixture) Config {
	t.Helper()
	return Config{Profile: ProfileHosted, AcceptanceMode: AcceptanceModeBuffered, BootstrapToken: adminToken,
		Shared: &SharedConfig{DatabaseURL: control},
		DuckLake: &lakeapi.DuckLakeConfig{Directory: t.TempDir(), PostgresURL: control, S3: &fixture.storage, MemoryLimitMB: 128, Threads: 1, MaxOpenProjects: 2, MaintenanceInterval: 24 * time.Hour,
			CleanupGrace: 4 * time.Hour, RestoreWindow: time.Hour, RestoreMaxReaderTime: time.Hour, RestoreSafetyMargin: time.Hour}}
}

func sharedNativeJourneyPair(t *testing.T, fixture duckLakeS3Fixture, configure ...func(*Config)) (*sharedJourneyNode, *sharedJourneyNode) {
	t.Helper()
	control := sharedPostgresDSN(t)
	var nodes [2]*sharedJourneyNode
	for i := range nodes {
		config := hostedJourneyConfig(t, control, fixture)
		for _, configure := range configure {
			configure(&config)
		}
		s, host, _ := hostedCleanupWorker(t, config)
		nodes[i] = &sharedJourneyNode{service: s, host: host, controlDSN: control}
	}
	return nodes[0], nodes[1]
}

func sharedJourneyQuery(t *testing.T, origin, project, credential, sql string, want int) map[string]any {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"sql": sql})
	return sharedJourneyHTTP(t, origin, "POST", "/api/v1/projects/"+project+"/query", credential, body, nil, want)
}
