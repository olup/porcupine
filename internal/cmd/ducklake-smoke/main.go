// ducklake-smoke checks compiled artifacts against an isolated loopback server.
package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"time"
)

type queryResponse struct {
	Result struct {
		Rows uint64           `json:"rows"`
		Data []map[string]any `json:"data"`
	} `json:"result"`
}

type schemaResponse struct {
	Enabled bool `json:"enabled"`
	SQL     struct {
		Dialect string `json:"dialect"`
	} `json:"sql"`
	Delivery struct {
		Pending int64 `json:"pending"`
	} `json:"delivery"`
}

type captureResponse struct {
	Duplicates int `json:"duplicates"`
	Receipts   []struct {
		EventID string `json:"event_id"`
		UUID    string `json:"uuid"`
	} `json:"receipts"`
}

func main() {
	server := flag.String("server", "./bin/porcupine", "Compiled native server")
	cli := flag.String("cli", "./bin/cli/porcupine", "Compiled standalone CLI")
	timeout := flag.Duration("timeout", 60*time.Second, "Maximum duration of the smoke check")
	flag.Parse()
	if flag.NArg() != 0 || *timeout <= 0 {
		fmt.Fprintln(os.Stderr, "usage: ducklake-smoke -server PATH -cli PATH [-timeout 60s]")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	if err := run(ctx, *server, *cli); err != nil {
		fmt.Fprintln(os.Stderr, "DuckLake smoke failed:", err)
		os.Exit(1)
	}
	fmt.Println("ok: compiled server ingestion, query visibility, stable retry ID, CLI native query/schema/profile/status, scheduled native backup, visibility probe, private metrics and JSON logs")
}

func run(ctx context.Context, serverPath, cliPath string) error {
	serverPath, err := filepath.Abs(serverPath)
	if err != nil {
		return errors.New("invalid server path")
	}
	cliPath, err = filepath.Abs(cliPath)
	if err != nil {
		return errors.New("invalid CLI path")
	}
	for _, path := range []string{serverPath, cliPath} {
		info, err := os.Stat(path)
		if err != nil || !info.Mode().IsRegular() {
			return errors.New("build the server and CLI before running this check")
		}
	}
	directory, err := os.MkdirTemp("", "porcupine-ducklake-smoke-")
	if err != nil {
		return errors.New("cannot create private test directory")
	}
	defer os.RemoveAll(directory)
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return errors.New("cannot reserve a loopback port")
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		return errors.New("cannot release reserved loopback port")
	}
	origin := "http://" + address
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return errors.New("cannot generate temporary administrator credential")
	}
	admin := hex.EncodeToString(secret)
	logs, err := os.Create(filepath.Join(directory, "server.log"))
	if err != nil {
		return errors.New("cannot capture server logs")
	}
	defer logs.Close()
	server := exec.CommandContext(ctx, serverPath)
	server.Dir = directory
	server.Env = environment(map[string]string{
		"PORCUPINE_ADDR": address, "PORCUPINE_DB": filepath.Join(directory, "queue.db"),
		"PORCUPINE_BACKUP_DIR":      filepath.Join(directory, "backups"),
		"PORCUPINE_METRICS_ENABLED": "true", "PORCUPINE_METRICS_LISTEN": "127.0.0.1:0",
		"PORCUPINE_DUCKLAKE_DIR": filepath.Join(directory, "lake"), "PORCUPINE_BOOTSTRAP_TOKEN": admin,
	})
	server.Stdout, server.Stderr = io.Discard, logs
	if err := server.Start(); err != nil {
		return errors.New("cannot start compiled server")
	}
	done := make(chan error, 1)
	go func() { done <- server.Wait() }()
	defer func() {
		server.Process.Signal(os.Interrupt)
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			server.Process.Kill()
			<-done
		}
	}()
	transport := &http.Transport{Proxy: nil}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	if err := poll(ctx, func() (bool, error) {
		select {
		case err := <-done:
			done <- err
			return false, errors.New("compiled server exited before readiness")
		default:
		}
		return call(ctx, client, origin, http.MethodGet, "/readyz", nil, nil, 200, nil) == nil, nil
	}); err != nil {
		return fmt.Errorf("server readiness: %w", err)
	}
	adminHeaders := map[string]string{"Authorization": "Bearer " + admin}
	var created struct {
		Project struct {
			ID string `json:"id"`
		} `json:"project"`
		Token string `json:"ingest_token"`
	}
	if err := call(ctx, client, origin, http.MethodPost, "/api/v1/projects", adminHeaders, map[string]string{"name": "compiled DuckLake smoke"}, 201, &created); err != nil {
		return fmt.Errorf("create test project: %w", err)
	}
	if created.Project.ID == "" || created.Token == "" {
		return errors.New("project response omitted its ID or ingestion credential")
	}
	event := map[string]any{"type": "track", "message_id": "compiled-smoke-stable", "actor_id": "synthetic-visitor", "name": "compiled-smoke", "timestamp": "2026-11-01T12:00:00Z", "properties": map[string]any{"amount": 42, "amount_minor": 4200, "$sdk": "metadata"}}
	ingestHeaders := map[string]string{"X-Porcupine-Key": created.Token}
	var first captureResponse
	if err := call(ctx, client, origin, http.MethodPost, "/ingest/v1/events", ingestHeaders, event, 200, &first); err != nil {
		return fmt.Errorf("native ingestion: %w", err)
	}
	if len(first.Receipts) != 1 || first.Receipts[0].EventID == "" || first.Receipts[0].UUID == "" {
		return errors.New("native ingestion omitted its stable receipt")
	}
	query := map[string]any{"sql": "SELECT event_id,event FROM events WHERE event=$name", "parameters": map[string]string{"name": "compiled-smoke"}}
	base := "/api/v1/projects/" + created.Project.ID
	if err := poll(ctx, func() (bool, error) {
		var schema schemaResponse
		if err := call(ctx, client, origin, http.MethodGet, base+"/schema", adminHeaders, nil, 200, &schema); err != nil {
			return false, err
		}
		if schema.Delivery.Pending != 0 {
			return false, nil
		}
		var result queryResponse
		if err := call(ctx, client, origin, http.MethodPost, base+"/query", adminHeaders, query, 200, &result); err != nil {
			return false, err
		}
		return matches(result, first.Receipts[0].EventID), nil
	}); err != nil {
		return fmt.Errorf("query visibility: %w", err)
	}
	var retry captureResponse
	if err := call(ctx, client, origin, http.MethodPost, "/ingest/v1/events", ingestHeaders, event, 200, &retry); err != nil {
		return fmt.Errorf("native retry: %w", err)
	}
	if retry.Duplicates != 1 || len(retry.Receipts) != 1 || retry.Receipts[0].EventID != first.Receipts[0].EventID || retry.Receipts[0].UUID != first.Receipts[0].UUID {
		return errors.New("native retry changed the event identity")
	}
	cliEnv := environment(map[string]string{"PORCUPINE_API_KEY": admin, "PORCUPINE_URL": origin})
	invalid := exec.CommandContext(ctx, cliPath, "query-events", "--id", created.Project.ID, "--body", `{"sql":"SELECT missing_column FROM events"}`)
	invalid.Env = cliEnv
	var diagnostic bytes.Buffer
	invalid.Stderr = &diagnostic
	if err := invalid.Run(); err == nil || !strings.Contains(diagnostic.String(), "unknown_column") || !strings.Contains(diagnostic.String(), "get-event-schema") || strings.Contains(diagnostic.String(), admin) {
		return errors.New("compiled CLI did not preserve safe query recovery guidance")
	}
	var cliQuery queryResponse
	if err := runCLI(ctx, cliPath, cliEnv, query, &cliQuery, "query-events", "--id", created.Project.ID, "--body", "@-"); err != nil {
		return err
	}
	if !matches(cliQuery, first.Receipts[0].EventID) {
		return errors.New("compiled CLI query returned an unexpected event")
	}
	var catalog struct {
		Properties []struct {
			Key string `json:"key"`
		} `json:"properties"`
		Next string `json:"next_property_cursor"`
	}
	if err := runCLI(ctx, cliPath, cliEnv, nil, &catalog, "get-event-catalog", "--id", created.Project.ID, "--event", "compiled-smoke", "--property-kind", "custom", "--property-prefix", "amount", "--property-limit", "1"); err != nil {
		return err
	}
	if len(catalog.Properties) != 1 || catalog.Properties[0].Key != "amount" || catalog.Next == "" {
		return errors.New("compiled CLI filtered property page failed")
	}
	cursor := catalog.Next
	catalog.Properties = nil
	catalog.Next = ""
	if err := runCLI(ctx, cliPath, cliEnv, nil, &catalog, "get-event-catalog", "--id", created.Project.ID, "--event", "compiled-smoke", "--property-kind", "custom", "--property-prefix", "amount", "--property-limit", "1", "--property-cursor", cursor); err != nil {
		return err
	}
	if len(catalog.Properties) != 1 || catalog.Properties[0].Key != "amount_minor" || catalog.Next != "" {
		return errors.New("compiled CLI property continuation failed")
	}
	var profile struct {
		Event    string `json:"event"`
		Activity struct {
			Total string `json:"total"`
		} `json:"activity"`
		Sampling struct {
			Profiled int `json:"profiled_events"`
		} `json:"sampling"`
		Properties []struct {
			Key     string `json:"key"`
			Present int    `json:"present"`
		} `json:"properties"`
		Examples []struct {
			ID string `json:"event_id"`
		} `json:"examples"`
	}
	if err := runCLI(ctx, cliPath, cliEnv, nil, &profile, "get-event-profile", "--id", created.Project.ID, "--event", "compiled-smoke", "--property-kind", "custom", "--property-prefix", "amount", "--sample-limit", "10", "--example-limit", "1", "--include-values", "true"); err != nil {
		return err
	}
	if profile.Event != "compiled-smoke" || profile.Activity.Total != "1" || profile.Sampling.Profiled != 1 || len(profile.Properties) != 2 || profile.Properties[0].Key != "amount" || profile.Properties[0].Present != 1 || len(profile.Examples) != 1 || profile.Examples[0].ID != first.Receipts[0].EventID {
		return errors.New("compiled CLI event profile lost activity, sampled properties or stable event pointer")
	}
	var cliSchema schemaResponse
	if err := runCLI(ctx, cliPath, cliEnv, nil, &cliSchema, "get-event-schema", "--id", created.Project.ID); err != nil {
		return err
	}
	if !cliSchema.Enabled || cliSchema.SQL.Dialect != "duckdb" {
		return errors.New("compiled CLI schema did not describe the DuckDB engine")
	}
	var status struct {
		Backup *struct {
			State  string `json:"state"`
			Latest string `json:"latest_bundle"`
		} `json:"native_backup"`
		Cleanup *struct {
			Enabled   bool  `json:"enabled"`
			Retention int64 `json:"snapshot_retention_seconds"`
			Grace     int64 `json:"obsolete_file_grace_seconds"`
		} `json:"ducklake_cleanup"`
		Status string `json:"status"`
		Checks struct {
			SQLite   string `json:"sqlite"`
			DuckLake string `json:"ducklake"`
		} `json:"checks"`
	}
	if err := runCLI(ctx, cliPath, cliEnv, nil, &status, "get-instance-status"); err != nil {
		return err
	}
	if status.Status != "ok" || status.Checks.SQLite != "ok" || status.Checks.DuckLake != "ok" {
		return errors.New("compiled CLI status did not report the isolated DuckLake deployment")
	}
	if status.Cleanup == nil || !status.Cleanup.Enabled || status.Cleanup.Retention != 7*24*3600 || status.Cleanup.Grace != 48*3600 {
		return errors.New("compiled CLI did not report native cleanup defaults to the administrator")
	}
	if err := poll(ctx, func() (bool, error) {
		if err := runCLI(ctx, cliPath, cliEnv, nil, &status, "get-instance-status"); err != nil {
			return false, err
		}
		if status.Backup != nil && status.Backup.State == "failed" {
			return false, errors.New("compiled server automatic backup failed")
		}
		return status.Backup != nil && status.Backup.State == "ok" && status.Backup.Latest != "", nil
	}); err != nil {
		return err
	}
	if _, err := os.Stat(filepath.Join(directory, "backups", status.Backup.Latest, "manifest.json")); err != nil {
		return errors.New("compiled CLI advertised a missing backup bundle")
	}
	if err := runVisibilityProbe(ctx, client, cliPath, origin, adminHeaders); err != nil {
		return err
	}
	return checkTelemetry(ctx, client, origin, logs.Name(), admin, created.Token)
}

func runVisibilityProbe(ctx context.Context, client *http.Client, cliPath, origin string, adminHeaders map[string]string) error {
	var project struct {
		Project struct{ ID string } `json:"project"`
	}
	if err := call(ctx, client, origin, http.MethodPost, "/api/v1/projects", adminHeaders, map[string]string{"name": "visibility probe"}, 201, &project); err != nil {
		return fmt.Errorf("create dedicated probe project: %w", err)
	}
	credentials := map[string]string{}
	for env, permission := range map[string]string{"PORCUPINE_PROBE_INGEST_TOKEN": "events:write", "PORCUPINE_PROBE_QUERY_TOKEN": "events:read"} {
		var key struct{ Token string }
		body := map[string]any{"name": "visibility probe " + permission, "permissions": []string{permission}, "projects": []string{project.Project.ID}}
		if err := call(ctx, client, origin, http.MethodPost, "/api/v1/access-keys", adminHeaders, body, 201, &key); err != nil {
			return fmt.Errorf("create restricted probe key: %w", err)
		}
		credentials[env] = key.Token
	}
	probe := exec.CommandContext(ctx, cliPath, "visibility-probe", "--ingest-url", origin, "--project", project.Project.ID)
	probe.Env = environment(credentials)
	result, err := probe.CombinedOutput()
	if err != nil || !bytes.Contains(result, []byte("visibility probe visible after ")) {
		return fmt.Errorf("compiled visibility probe failed: %s", bytes.TrimSpace(result))
	}
	return nil
}

func checkTelemetry(ctx context.Context, client *http.Client, origin, logsPath string, secrets ...string) error {
	logs, err := os.ReadFile(logsPath)
	if err != nil {
		return errors.New("cannot read server logs")
	}
	checkPrivate := func(body []byte) error {
		for _, secret := range append(secrets, "compiled-smoke-stable", "SELECT missing_column") {
			if bytes.Contains(body, []byte(secret)) {
				return errors.New("server telemetry included secret or request data")
			}
		}
		return nil
	}
	if err := checkPrivate(logs); err != nil {
		return err
	}
	var metricsAddr string
	scanner := bufio.NewScanner(bytes.NewReader(logs))
	for scanner.Scan() {
		var entry struct {
			Level    string `json:"level"`
			Message  string `json:"msg"`
			Listener string `json:"listener"`
			Address  string `json:"address"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil || entry.Level == "" || entry.Message == "" {
			return errors.New("server stderr contained an invalid JSON log entry")
		}
		if entry.Message == "Porcupine listening" && entry.Listener == "metrics" {
			metricsAddr = entry.Address
		}
	}
	if scanner.Err() != nil || metricsAddr == "" {
		return errors.New("server did not log its private listener")
	}
	if err := call(ctx, client, origin, http.MethodGet, "/metrics", nil, nil, 404, nil); err != nil {
		return errors.New("public listener exposes /metrics")
	}
	metricsOrigin := "http://" + metricsAddr
	if err := call(ctx, client, metricsOrigin, http.MethodGet, "/readyz", nil, nil, 404, nil); err != nil {
		return errors.New("private listener exposes the application router")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, metricsOrigin+"/metrics", nil)
	if err != nil {
		return errors.New("invalid logged metrics listener")
	}
	res, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("scrape private listener: %w", err)
	}
	defer res.Body.Close()
	metrics, err := io.ReadAll(io.LimitReader(res.Body, 256<<10))
	if err != nil || res.StatusCode != 200 || !strings.HasPrefix(res.Header.Get("Content-Type"), "text/plain; version=0.0.4") {
		return errors.New("private listener did not return Prometheus metrics")
	}
	if err := checkPrivate(metrics); err != nil {
		return err
	}
	for _, sample := range []string{
		`porcupine_ingestion_events_total{profile="standalone",durability="durable",outcome="accepted"} 2`,
		`porcupine_ingestion_events_total{profile="standalone",durability="durable",outcome="duplicate"} 1`,
		`porcupine_flush_attempts_total{profile="standalone",outcome="ok"}`,
		`porcupine_query_backend_duration_seconds_count{profile="standalone",operation="sql",outcome="ok"}`,
		`porcupine_queue_events{profile="standalone"}`,
	} {
		if !bytes.Contains(metrics, []byte(sample)) {
			return fmt.Errorf("private listener missing %s", sample)
		}
	}
	return nil
}

func matches(result queryResponse, id string) bool {
	return result.Result.Rows == 1 && len(result.Result.Data) == 1 && result.Result.Data[0]["event_id"] == id && result.Result.Data[0]["event"] == "compiled-smoke"
}

func environment(values map[string]string) []string {
	var env []string
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if !strings.HasPrefix(name, "PORCUPINE_") {
			env = append(env, entry)
		}
	}
	for name, value := range values {
		env = append(env, name+"="+value)
	}
	return env
}

func poll(ctx context.Context, check func() (bool, error)) error {
	for {
		ready, err := check()
		if err != nil {
			return err
		}
		if ready {
			return nil
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func call(ctx context.Context, client *http.Client, origin, method, path string, headers map[string]string, body any, status int, out any) error {
	var data []byte
	var err error
	if body != nil {
		data, err = json.Marshal(body)
		if err != nil {
			return errors.New("cannot encode test request")
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, origin+path, bytes.NewReader(data))
	if err != nil {
		return errors.New("cannot create loopback request")
	}
	req.Header.Set("Content-Type", "application/json")
	for name, value := range headers {
		req.Header.Set(name, value)
	}
	res, err := client.Do(req)
	if err != nil {
		return errors.New("loopback request failed")
	}
	defer res.Body.Close()
	if res.StatusCode != status {
		return fmt.Errorf("unexpected HTTP status %d", res.StatusCode)
	}
	data, err = io.ReadAll(io.LimitReader(res.Body, 2<<20+1))
	if err != nil || len(data) > 2<<20 {
		return errors.New("invalid response size")
	}
	if out != nil && json.Unmarshal(data, out) != nil {
		return errors.New("invalid response JSON")
	}
	return nil
}

func runCLI(ctx context.Context, path string, env []string, body any, out any, args ...string) error {
	command := exec.CommandContext(ctx, path, args...)
	command.Env = env
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return errors.New("cannot encode CLI request")
		}
		command.Stdin = bytes.NewReader(data)
	}
	data, err := command.Output()
	if err != nil {
		return fmt.Errorf("compiled CLI %s failed", args[0])
	}
	if len(data) > 2<<20 || json.Unmarshal(data, out) != nil {
		return fmt.Errorf("compiled CLI %s returned invalid JSON", args[0])
	}
	return nil
}
