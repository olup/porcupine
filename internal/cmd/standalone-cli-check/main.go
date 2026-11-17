// Command standalone-cli-check exercises the built CLI against a private local server.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func artifact(path string) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(absolute)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("not a regular artifact: %s", path)
	}
	return absolute, nil
}
func serverEnvironment(directory, address, bootstrap string) []string {
	result := []string{}
	for _, key := range []string{"HOME", "PATH", "TMPDIR", "SSL_CERT_FILE", "SSL_CERT_DIR", "SystemRoot", "WINDIR"} {
		if value, ok := os.LookupEnv(key); ok {
			result = append(result, key+"="+value)
		}
	}
	return append(result, "PORCUPINE_ADDR="+address, "PORCUPINE_DB="+filepath.Join(directory, "test.db"), "PORCUPINE_BOOTSTRAP_TOKEN="+bootstrap, "PORCUPINE_OAUTH_ISSUER=http://"+address)
}
func build(directory, output string, native bool) error {
	cmd := exec.Command("go", "build", "-o", output, "./cmd/porcupine")
	cmd.Dir = directory
	setting := "0"
	if native {
		setting = "1"
	}
	cmd.Env = append(os.Environ(), "CGO_ENABLED="+setting)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}
func run() error {
	serverFlag := flag.String("server", "", "already built server artifact (not rebuilt)")
	cliFlag := flag.String("cli", "", "already built CLI artifact (not rebuilt)")
	flag.Parse()
	root, err := os.Getwd()
	if err != nil {
		return err
	}
	directory, err := os.MkdirTemp("", "porcupine-cli-check-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(directory)
	suffix := ""
	if runtime.GOOS == "windows" {
		suffix = ".exe"
	}
	server := *serverFlag
	if server == "" {
		server = filepath.Join(directory, "server"+suffix)
		if err := build(root, server, true); err != nil {
			return err
		}
	} else {
		server, err = artifact(server)
		if err != nil {
			return err
		}
	}
	binDir := filepath.Join(directory, "bin")
	if err := os.Mkdir(binDir, 0755); err != nil {
		return err
	}
	binary := filepath.Join(binDir, "porcupine"+suffix)
	if *cliFlag == "" {
		if err := build(filepath.Join(root, "cli"), binary, false); err != nil {
			return err
		}
	} else {
		source, err := artifact(*cliFlag)
		if err != nil {
			return err
		}
		data, err := os.ReadFile(source)
		if err != nil {
			return err
		}
		if err := os.WriteFile(binary, data, 0755); err != nil {
			return err
		}
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	address := listener.Addr().String()
	listener.Close()
	origin := "http://" + address
	random := make([]byte, 32)
	if _, err := rand.Read(random); err != nil {
		return err
	}
	bootstrap := hex.EncodeToString(random)
	process := exec.Command(server)
	process.Dir = directory
	process.Env = serverEnvironment(directory, address, bootstrap)
	process.Stdout = io.Discard
	process.Stderr = io.Discard
	if err := process.Start(); err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() { done <- process.Wait() }()
	defer func() {
		_ = process.Process.Signal(os.Interrupt)
		select {
		case <-done:
		case <-time.After(12 * time.Second):
			_ = process.Process.Kill()
			<-done
		}
	}()
	client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{Proxy: nil}, CheckRedirect: func(*http.Request, []*http.Request) error { return fmt.Errorf("redirect refused") }}
	deadline := time.Now().Add(20 * time.Second)
	for {
		response, e := client.Get(origin + "/readyz")
		if e == nil {
			response.Body.Close()
			if response.StatusCode == 200 {
				break
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("test server did not become ready")
		}
		select {
		case err := <-done:
			done <- err
			return fmt.Errorf("test server exited: %v", err)
		case <-time.After(50 * time.Millisecond):
		}
	}
	checker := journey{directory: directory, binary: binary, origin: origin, bootstrap: bootstrap, client: client}
	if err := checker.check(); err != nil {
		return err
	}
	fmt.Println("Standalone CLI passed readiness, status, API errors, project rename/deletion, isolation, binary upload/retry and key revocation.")
	return nil
}

type object = map[string]any
type journey struct {
	directory, binary, origin, bootstrap, admin string
	client                                      *http.Client
}

func (j *journey) request(method, path string, body []byte, basic bool) (object, error) {
	request, err := http.NewRequest(method, j.origin+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	if basic {
		request.SetBasicAuth("bootstrap", j.bootstrap)
		request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	} else {
		request.Header.Set("Authorization", "Bearer "+j.admin)
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := j.client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("setup %s %s: HTTP %d", method, path, response.StatusCode)
	}
	if response.StatusCode == 204 {
		return nil, nil
	}
	var result object
	err = json.NewDecoder(io.LimitReader(response.Body, 4<<20)).Decode(&result)
	return result, err
}
func (j *journey) setup(method, path string, data any) (object, error) {
	var body []byte
	if data != nil {
		body, _ = json.Marshal(data)
	}
	return j.request(method, "/api/v1"+path, body, false)
}
func (j *journey) service(name string, projects, permissions []string) (object, error) {
	grant := object{"name": name, "projects": projects, "permissions": permissions}
	account, err := j.setup("POST", "/oauth-clients", grant)
	if err != nil {
		return nil, err
	}
	grant["audience"] = "api"
	return j.setup("POST", "/oauth-clients/"+text(account["client_id"])+"/api-keys", grant)
}
func text(value any) string                 { return fmt.Sprint(value) }
func field(value object, key string) object { return value[key].(map[string]any) }
func list(value object, key string) []any   { return value[key].([]any) }
func (j *journey) cli(key string, input []byte, denied int, message string, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, j.binary, args...)
	cmd.Dir = filepath.Dir(j.binary)
	cmd.Env = []string{"PATH=" + filepath.Dir(j.binary), "XDG_CONFIG_HOME=" + filepath.Join(j.directory, "config"), "PORCUPINE_URL=" + j.origin, "NO_COLOR=1"}
	if key != "" {
		cmd.Env = append(cmd.Env, "PORCUPINE_API_KEY="+key)
	}
	cmd.Stdin = bytes.NewReader(input)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	for _, secret := range []string{key, j.admin, j.bootstrap} {
		if secret != "" && (bytes.Contains(stdout.Bytes(), []byte(secret)) || bytes.Contains(stderr.Bytes(), []byte(secret))) {
			return nil, fmt.Errorf("CLI exposed a credential")
		}
	}
	if denied != 0 {
		if err == nil || !strings.Contains(stderr.String(), fmt.Sprint(denied)) || stdout.Len() != 0 {
			return nil, fmt.Errorf("%s should report HTTP %d only on stderr", args[0], denied)
		}
	} else if err != nil {
		return nil, fmt.Errorf("%s failed: %w: %s", args[0], err, stderr.String())
	}
	if message != "" && !strings.Contains(stderr.String(), message) {
		return nil, fmt.Errorf("%s omitted API detail %q", args[0], message)
	}
	return stdout.Bytes(), nil
}
func (j *journey) cliJSON(key string, input []byte, args ...string) (object, error) {
	data, err := j.cli(key, input, 0, "", args...)
	if err != nil {
		return nil, err
	}
	var result object
	err = json.Unmarshal(data, &result)
	return result, err
}
func require(ok bool, message string) error {
	if !ok {
		return fmt.Errorf("%s", message)
	}
	return nil
}
func (j *journey) check() error {
	token, err := j.request("POST", "/oauth/token", []byte(url.Values{"grant_type": {"client_credentials"}, "resource": {j.origin + "/api/v1"}}.Encode()), true)
	if err != nil {
		return err
	}
	j.admin = text(token["access_token"])
	for _, help := range []struct{ command, contains string }{{"upload-import-chunk", "--x-chunk-sha256"}, {"login", "login"}} {
		out, err := j.cli("", nil, 0, "", help.command, "--help")
		if err != nil {
			return err
		}
		if err := require(strings.Contains(string(out), help.contains), "CLI help missing "+help.contains); err != nil {
			return err
		}
	}
	out, err := j.cli("", nil, 0, "", "--url", j.origin, "status")
	if err != nil {
		return err
	}
	if err := require(bytes.Contains(out, []byte("ready")), "public readiness missing"); err != nil {
		return err
	}
	admin, err := j.service("project provisioner", []string{"*"}, []string{"management:read", "management:write", "events:write"})
	if err != nil {
		return err
	}
	adminKey := text(admin["token"])
	operator, err := j.service("deployment operator", []string{"*"}, []string{"instance:admin"})
	if err != nil {
		return err
	}
	operatorKey := text(operator["token"])
	status, err := j.cliJSON(adminKey, nil, "get-instance-status")
	if err != nil {
		return err
	}
	if err := require(status["status"] == "ok" && field(status, "checks")["sqlite"] == "ok" && field(status, "checks")["ducklake"] == "ok", "instance health mismatch"); err != nil {
		return err
	}
	for name, value := range field(status, "configuration") {
		if err := require(value == (name == "analytics"), "unexpected optional service: "+name); err != nil {
			return err
		}
	}
	if _, err := j.cli(adminKey, nil, 403, "", "create-project", "--body", `{"name":"not authorized"}`); err != nil {
		return err
	}
	if _, err := j.cli(operatorKey, nil, 400, "name must contain 1 to 200 UTF-8 bytes", "create-project", "--body", `{"name":"   "}`); err != nil {
		return err
	}
	if _, err := j.cli(operatorKey, nil, 400, "validation failed", "create-project", "--body", `{"name":`); err != nil {
		return err
	}
	ids := []string{}
	projectFile := filepath.Join(j.directory, "project with spaces.json")
	for _, name := range []string{"CLI private", "CLI other"} {
		data, _ := json.Marshal(object{"name": name, "ingestion_mode": "server_only"})
		if err := os.WriteFile(projectFile, data, 0600); err != nil {
			return err
		}
		created, err := j.cliJSON(operatorKey, nil, "create-project", "--body", "@"+projectFile)
		if err != nil {
			return err
		}
		project := field(created, "project")
		if err := require(project["name"] == name, "project name changed"); err != nil {
			return err
		}
		ids = append(ids, text(project["id"]))
	}
	projects, err := j.cliJSON(adminKey, nil, "list-projects")
	if err != nil {
		return err
	}
	if err := require(len(list(projects, "projects")) == 2, "admin project list mismatch"); err != nil {
		return err
	}
	renamed, err := j.cliJSON(adminKey, nil, "rename-project", "--id", ids[0], "--body", `{"name":"CLI renamed"}`)
	if err != nil {
		return err
	}
	if err := require(field(renamed, "project")["id"] == ids[0] && field(renamed, "project")["name"] == "CLI renamed", "rename changed identity"); err != nil {
		return err
	}
	if _, err := j.cli(adminKey, nil, 403, "", "delete-project", "--id", ids[0]); err != nil {
		return err
	}
	scoped, err := j.service("importer", []string{ids[0]}, []string{"management:read", "events:read", "events:write", "imports:write"})
	if err != nil {
		return err
	}
	key := text(scoped["token"])
	projects, err = j.cliJSON(key, nil, "list-projects")
	if err != nil {
		return err
	}
	rows := list(projects, "projects")
	if err := require(len(rows) == 1 && rows[0].(map[string]any)["id"] == ids[0], "scoped project list leaked"); err != nil {
		return err
	}
	scopedStatus, err := j.cliJSON(key, nil, "get-instance-status")
	if err != nil {
		return err
	}
	serialized, _ := json.Marshal(scopedStatus)
	for _, id := range ids {
		if bytes.Contains(serialized, []byte(id)) {
			return fmt.Errorf("instance status leaked project")
		}
	}
	if _, err := j.cli(key, nil, 403, "", "get-event-schema", "--id", ids[1]); err != nil {
		return err
	}
	contract, err := j.cliJSON(key, nil, "get-event-input-contract", "--id", ids[0])
	if err != nil {
		return err
	}
	if err := require(contract["version"] == "1" && field(contract, "live_schema")["type"] == "object" && contract["native_historical_import_available"] == true && contract["historical_schema_status"] == "stable", "event contract mismatch"); err != nil {
		return err
	}
	if _, err := j.cli(key, nil, 403, "", "get-event-input-contract", "--id", ids[1]); err != nil {
		return err
	}
	data := []byte("{\"uuid\":\"cli-demo\",\"event\":\"Café\",\"distinct_id\":\"cli\",\"timestamp\":\"2020-01-02T03:04:05Z\",\"properties\":{}}\n")
	sum := sha256.Sum256(data)
	digest := hex.EncodeToString(sum[:])
	path := filepath.Join(j.directory, "events.jsonl")
	if err := os.WriteFile(path, data, 0600); err != nil {
		return err
	}
	manifest, _ := json.Marshal(object{"idempotency_key": "cli-import", "files": []any{object{"kind": "events", "path": "events.jsonl", "size_bytes": len(data), "sha256": digest}}})
	job, err := j.cliJSON(key, manifest, "create-import", "--id", ids[0], "--body", "@-")
	if err != nil {
		return err
	}
	jobArgs := []string{"--id", ids[0], "--import-id", text(job["id"])}
	fileArgs := append(append([]string{}, jobArgs...), "--file-id", text(list(job, "files")[0].(map[string]any)["id"]))
	for _, source := range []string{"@" + path, "@-"} {
		args := append([]string{"upload-import-chunk"}, fileArgs...)
		args = append(args, "--upload-offset", "0", "--x-chunk-sha256", digest, "--body", source)
		uploaded, err := j.cliJSON(key, data, args...)
		if err != nil {
			return err
		}
		if err := require(list(uploaded, "files")[0].(map[string]any)["offset_bytes"] == float64(len(data)) && field(uploaded, "counts")["accepted"] == float64(1), "retry changed import count or offset"); err != nil {
			return err
		}
	}
	if _, err := j.cliJSON(key, nil, append([]string{"complete-import-file"}, fileArgs...)...); err != nil {
		return err
	}
	finished, err := j.cliJSON(key, nil, append([]string{"complete-import"}, jobArgs...)...)
	if err != nil {
		return err
	}
	if err := require(finished["state"] == "completed", "import incomplete"); err != nil {
		return err
	}
	if _, err := j.setup("DELETE", "/access-keys/"+text(scoped["id"]), nil); err != nil {
		return err
	}
	for _, command := range []string{"list-projects", "get-instance-status"} {
		if _, err := j.cli(key, nil, 401, "", command); err != nil {
			return err
		}
	}
	for range 2 {
		if _, err := j.cli(j.admin, nil, 0, "", "delete-project", "--id", ids[0]); err != nil {
			return err
		}
	}
	remaining, err := j.cliJSON(adminKey, nil, "list-projects")
	if err != nil {
		return err
	}
	rows = list(remaining, "projects")
	return require(len(rows) == 1 && rows[0].(map[string]any)["id"] == ids[1], "delete touched another project")
}
