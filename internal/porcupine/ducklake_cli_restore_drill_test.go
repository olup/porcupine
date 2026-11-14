//go:build ducklake

package porcupine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDuckLakeCLIRestoreDrill(t *testing.T) {
	image, binary := os.Getenv("PORCUPINE_RESTORE_DOCKER_IMAGE"), os.Getenv("PORCUPINE_RESTORE_CLI")
	if image == "" || binary == "" {
		t.Skip("set restore image and standalone CLI paths, or run make ducklake-restore-drill")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	docker := func(args ...string) string {
		t.Helper()
		out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
		if err != nil {
			t.Fatalf("Docker drill step failed: %v: %s", err, out)
		}
		return strings.TrimSpace(string(out))
	}
	volume := "porcupine-restore-drill-" + newID()
	container := "porcupine-restored-drill-" + newID()
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		_ = exec.CommandContext(cleanup, "docker", "rm", "-f", container).Run()
		_ = exec.CommandContext(cleanup, "docker", "volume", "rm", volume).Run()
	})
	s, config := nativeBackupService(t)
	a, ingest := project(t, s)
	b, otherIngest := project(t, s)
	state := lakeMaintenanceLifecycle(t, s, a, ingest, "cli-restore", true)
	nativeRequest(t, s, otherIngest, `{"type":"track","message_id":"other","actor_id":"other","name":"untouched"}`, 200)
	duckLakeJourneyDrain(t, s)
	if err := s.createNativeBackup(ctx); err != nil {
		t.Fatal(err)
	}
	bundle := filepath.Join(config.NativeBackup.Directory, s.nativeBackupStatus().LatestBundle)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Dir(config.DBPath)); err != nil {
		t.Fatal(err)
	}
	recoveryToken := token() + token()
	secret := filepath.Join(t.TempDir(), "recovery-token")
	if err := os.WriteFile(secret, []byte(recoveryToken+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, binary, "restore", "--bundle", bundle, "--volume", volume,
		"--bootstrap-token-file", secret, "--reset-access", "--image", image)
	cmd.Env = restoreDrillEnvironment()
	var output, diagnostics bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &diagnostics
	if err := cmd.Run(); err != nil {
		t.Fatalf("standalone CLI restore failed: %v: %s", err, diagnostics.String())
	}
	if strings.Contains(output.String()+diagnostics.String(), recoveryToken) {
		t.Fatal("recovery bootstrap secret was printed")
	}
	var restored NativeRestoreResult
	if err := json.Unmarshal(output.Bytes(), &restored); err != nil {
		t.Fatal("restore did not produce its JSON result", err)
	}
	var rotated string
	for _, key := range restored.IngestionKeys {
		if key.ProjectID == a {
			rotated = key.Token
		}
	}
	if rotated == "" || rotated == ingest {
		t.Fatal("ingestion credentials were not rotated")
	}
	// The deployed image must need neither the original source nor the archive.
	if err := os.RemoveAll(config.NativeBackup.Directory); err != nil {
		t.Fatal(err)
	}
	docker("run", "-d", "--name", container, "--read-only", "--memory=2g", "--cpus=2",
		"--tmpfs", "/tmp:rw,nosuid,nodev,size=256m", "--mount", "type=volume,src="+volume+",dst=/data",
		"-p", "127.0.0.1::8080", image)
	if user := docker("inspect", "--format", "{{.Config.User}}", container); user != "65532:65532" {
		t.Fatalf("recovered server is not nonroot: %s", user)
	}
	port := docker("inspect", "--format", `{{(index (index .NetworkSettings.Ports "8080/tcp") 0).HostPort}}`, container)
	origin := "http://127.0.0.1:" + port
	client := &http.Client{Timeout: 5 * time.Second}
	call := func(method, path, credential, body string, status int) map[string]any {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, method, origin+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		if strings.HasPrefix(path, "/ingest/") {
			req.Header.Set("X-Porcupine-Key", credential)
		} else if credential != "" {
			req.Header.Set("Authorization", "Bearer "+credential)
		}
		response, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		raw, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		if err != nil || response.StatusCode != status {
			t.Fatalf("restored %s %s: status=%d want=%d error=%v body=%s", method, path, response.StatusCode, status, err, raw)
		}
		var data map[string]any
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &data); err != nil {
				t.Fatal(err)
			}
		}
		return data
	}
	ready := false
	for attempt := 0; attempt < 80; attempt++ {
		response, err := client.Get(origin + "/readyz")
		if err == nil {
			response.Body.Close()
			ready = response.StatusCode == 200
		}
		if ready {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !ready {
		t.Fatal("recovered nonroot server did not become ready")
	}
	call("GET", "/api/v1/projects", adminToken, "", 401)
	call("POST", "/ingest/v1/events", ingest, lakeMaintenancePages("cli-restore"), 401)
	retried := call("POST", "/ingest/v1/events", rotated, lakeMaintenancePages("cli-restore"), 200)
	if retried["duplicates"] != float64(2) || receiptFrom(t, retried, 0)["event_id"] != state.receipts[0]["event_id"] {
		t.Fatal("CLI recovery changed producer retry IDs")
	}
	for projectID, want := range map[string]string{a: "1", b: "1"} {
		query := exec.CommandContext(ctx, binary, "query-events", "--id", projectID, "--body", `{"sql":"SELECT count(*) AS total FROM events","use_cache":false}`)
		query.Env = append(restoreDrillEnvironment(), "PORCUPINE_URL="+origin, "PORCUPINE_API_KEY="+recoveryToken)
		result, err := query.Output()
		if err != nil {
			t.Fatal("standalone CLI cannot query recovered data", err)
		}
		var decoded struct {
			Result struct {
				Data []map[string]string `json:"data"`
			} `json:"result"`
		}
		if err := json.Unmarshal(result, &decoded); err != nil || len(decoded.Result.Data) != 1 || decoded.Result.Data[0]["total"] != want {
			t.Fatalf("recovered project count mismatch: %v %s", err, result)
		}
	}
	t.Log(fmt.Sprintf("standalone restore -> nonroot image -> two project queries passed, source=%s", restored.SourceID))
}

func restoreDrillEnvironment() []string {
	var environment []string
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(name, "PORCUPINE_") || strings.HasPrefix(name, "AWS_") {
			continue
		}
		environment = append(environment, entry)
	}
	return append(environment, "AWS_EC2_METADATA_DISABLED=true")
}
