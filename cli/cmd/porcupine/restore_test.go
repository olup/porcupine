package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/urfave/cli/v3"
)

const restoreTestToken = "private-recovery-bootstrap-token-with-more-than-32-characters"

type restoreDockerCall struct {
	args, env []string
	input     string
}
type fakeRestoreDocker struct {
	t                                 *testing.T
	calls                             []restoreDockerCall
	existing, wrongOwner, failRestore bool
	owner                             string
	cancel                            context.CancelFunc
	cleanupIndependent                bool
	stderr                            string
}

func (f *fakeRestoreDocker) run(ctx context.Context, args, env []string, input io.Reader, output io.Writer) error {
	f.t.Helper()
	var body []byte
	if input != nil {
		body, _ = io.ReadAll(input)
	}
	f.calls = append(f.calls, restoreDockerCall{slices.Clone(args), slices.Clone(env), string(body)})
	switch {
	case slices.Equal(args[:2], []string{"volume", "ls"}):
		if f.existing {
			_, _ = io.WriteString(output, "existing-volume\n")
		}
	case slices.Equal(args[:2], []string{"volume", "create"}):
		for _, arg := range args {
			if strings.HasPrefix(arg, restoreOwnerLabel+"=") {
				f.owner = strings.TrimPrefix(arg, restoreOwnerLabel+"=")
			}
		}
	case slices.Equal(args[:2], []string{"volume", "inspect"}):
		owner := f.owner
		if f.wrongOwner {
			owner = "someone-else"
		}
		_ = json.NewEncoder(output).Encode(map[string]string{restoreOwnerLabel: owner})
	case args[0] == "run":
		if f.stderr != "" {
			var diagnostics restoreDiagnostics
			_, _ = diagnostics.Write([]byte(f.stderr))
			return &restoreDockerFailure{diagnostics: diagnostics}
		}
		if f.cancel != nil {
			f.cancel()
			return ctx.Err()
		}
		if f.failRestore {
			_, _ = output.Write(body)
			return errors.New(string(body))
		}
		_, _ = io.WriteString(output, `{"backup_id":"pc_backup_test","recovery_admin_id":"new-admin","ingestion_keys":[]}`+"\n")
	case args[0] == "rm":
		deadline, ok := ctx.Deadline()
		f.cleanupIndependent = ctx.Err() == nil && ok && time.Until(deadline) > 0 && time.Until(deadline) <= 10*time.Second
	default:
		f.t.Fatalf("unexpected Docker operation: %v", args)
	}
	return nil
}

func invokeRestore(t *testing.T, ctx context.Context, docker *fakeRestoreDocker, args ...string) (string, string, error) {
	t.Helper()
	tokenFile := filepath.Join(t.TempDir(), "bootstrap-token")
	if err := os.WriteFile(tokenFile, []byte(restoreTestToken+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	var out, progress bytes.Buffer
	app := &cli.Command{Name: "porcupine", Writer: &out, ErrWriter: &progress, Commands: []*cli.Command{restoreCommandWithRunner(docker.run)}}
	all := []string{"porcupine", "restore", "--image", "test-native-image", "--bootstrap-token-file", tokenFile, "--reset-access"}
	err := app.Run(ctx, append(all, args...))
	return out.String(), progress.String(), err
}

func remoteRestoreFlags() []string {
	return []string{"--backup-id", "pc_backup_test", "--source-id", "source-one", "--volume", "new-restore-volume"}
}

func restoreRunCall(t *testing.T, docker *fakeRestoreDocker) restoreDockerCall {
	t.Helper()
	for _, call := range docker.calls {
		if call.args[0] == "run" {
			return call
		}
	}
	t.Fatal("native restore did not run")
	return restoreDockerCall{}
}

func TestRestoreOfflineCreatesOwnedRootVolume(t *testing.T) {
	t.Setenv("PORCUPINE_API_KEY", "must-not-be-forwarded")
	t.Setenv("PORCUPINE_BOOTSTRAP_TOKEN", "must-not-be-forwarded-either")
	t.Setenv("PORCUPINE_BACKUP_S3_SECRET_ACCESS_KEY", "aws-private-secret")
	docker := &fakeRestoreDocker{t: t}
	out, progress, err := invokeRestore(t, context.Background(), docker, remoteRestoreFlags()...)
	if err != nil {
		t.Fatal(err)
	}
	if !json.Valid([]byte(out)) || !strings.Contains(progress, "new-restore-volume") {
		t.Fatalf("unexpected output %q / %q", out, progress)
	}
	call := restoreRunCall(t, docker)
	var request offlineRestoreRequest
	if err := json.Unmarshal([]byte(call.input), &request); err != nil {
		t.Fatal(err)
	}
	if request.BootstrapToken != restoreTestToken || !request.ChownToNonroot || !request.ResetCredentialsAcknowledged || request.TargetDBPath != "/data/porcupine.db" || request.TargetDuckLakeDirectory != "/data/ducklake" {
		t.Fatalf("invalid restore request fields")
	}
	for _, pair := range [][2]string{{"--user", "0:0"}, {"--memory", "2g"}, {"--cpus", "2"}, {"--tmpfs", "/tmp:rw,nosuid,nodev"}, {"--env", "PORCUPINE_BACKUP_S3_SECRET_ACCESS_KEY"}} {
		if !strings.Contains(strings.Join(call.args, "\n"), strings.Join(pair[:], "\n")) {
			t.Errorf("missing Docker option %v", pair)
		}
	}
	if !slices.Contains(call.args, "--read-only") || call.args[len(call.args)-1] != "restore-native-backup" {
		t.Fatal("missing native container restrictions")
	}
	for _, sensitive := range []string{restoreTestToken, "aws-private-secret", "PORCUPINE_API_KEY", "PORCUPINE_BOOTSTRAP_TOKEN"} {
		if strings.Contains(strings.Join(call.args, " ")+out+progress, sensitive) {
			t.Errorf("private value or unauthorized environment name exposed: %s", sensitive)
		}
	}
	if !docker.cleanupIndependent {
		t.Fatal("cleanup must use an independent bounded context")
	}
	if docker.owner == "" {
		t.Fatal("volume missing unique ownership marker")
	}
}

func TestRestoreRefusesExistingAndConcurrentVolumes(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		existing, wrongOwner bool
		wantCalls            int
	}{{"existing", true, false, 1}, {"concurrent", false, true, 3}} {
		t.Run(tc.name, func(t *testing.T) {
			docker := &fakeRestoreDocker{t: t, existing: tc.existing, wrongOwner: tc.wrongOwner}
			out, _, err := invokeRestore(t, context.Background(), docker, remoteRestoreFlags()...)
			if err == nil || out != "" || len(docker.calls) != tc.wantCalls {
				t.Fatalf("restore did not refuse safely: %v / %d calls", err, len(docker.calls))
			}
			for _, call := range docker.calls {
				if call.args[0] == "run" || slices.Contains(call.args, "rm") {
					t.Fatal("existing volume was used or removed")
				}
			}
		})
	}
}

func TestRestoreFailurePreservesVolumeAndSuppressesPrivateOutput(t *testing.T) {
	docker := &fakeRestoreDocker{t: t, failRestore: true}
	out, progress, err := invokeRestore(t, context.Background(), docker, remoteRestoreFlags()...)
	if err == nil || !strings.Contains(err.Error(), "retained") || out != "" {
		t.Fatalf("unexpected failure behavior: %q / %v", out, err)
	}
	if strings.Contains(progress+err.Error(), restoreTestToken) {
		t.Fatal("private stdin leaked on Docker failure")
	}
	if !docker.cleanupIndependent {
		t.Fatal("failed container was not cleaned up")
	}
	for _, call := range docker.calls {
		if slices.Equal(call.args[:2], []string{"volume", "rm"}) {
			t.Fatal("failed volume must be retained")
		}
	}
	if len(docker.calls) < 2 || !slices.Contains(docker.calls[1].args, "io.porcupine.restore.attempt=true") {
		t.Fatal("retained volume missing restore attempt marker")
	}
}

func TestRestoreCancellationCleansContainerWithIndependentContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	docker := &fakeRestoreDocker{t: t, cancel: cancel}
	_, _, err := invokeRestore(t, ctx, docker, remoteRestoreFlags()...)
	if !errors.Is(err, context.Canceled) || !docker.cleanupIndependent {
		t.Fatalf("cancellation cleanup failed: %v", err)
	}
}

func TestRestoreInfersBoundedLocalManifestAndEscapesMount(t *testing.T) {
	bundle := filepath.Join(t.TempDir(), "bundle,with,commas")
	if err := os.Mkdir(bundle, 0700); err != nil {
		t.Fatal(err)
	}
	manifest := filepath.Join(bundle, "manifest.json")
	if err := os.WriteFile(manifest, []byte(`{"format":"porcupine-native-v1","id":"pc_backup_local","source_id":"local-source"}`), 0600); err != nil {
		t.Fatal(err)
	}
	bundle, _ = filepath.EvalSymlinks(bundle)
	docker := &fakeRestoreDocker{t: t}
	_, _, err := invokeRestore(t, context.Background(), docker, "--bundle", bundle)
	if err != nil {
		t.Fatal(err)
	}
	call := restoreRunCall(t, docker)
	var request offlineRestoreRequest
	if err := json.Unmarshal([]byte(call.input), &request); err != nil {
		t.Fatal(err)
	}
	if request.BackupID != "pc_backup_local" || request.SourceID != "local-source" || request.LocalBundlePath != "/backup/pc_backup_local" {
		t.Fatal("local backup identity not inferred")
	}
	found := false
	for i, arg := range call.args {
		if arg != "--mount" {
			continue
		}
		fields, err := csv.NewReader(strings.NewReader(call.args[i+1])).Read()
		if err != nil {
			t.Fatal(err)
		}
		if slices.Contains(fields, "src="+bundle) {
			found = true
			if !slices.Contains(fields, "readonly") || !slices.Contains(fields, "dst="+request.LocalBundlePath) {
				t.Fatal("local bundle mount is not private and readonly")
			}
		}
	}
	if !found {
		t.Fatal("local bundle was not mounted")
	}
	f, err := os.OpenFile(manifest, os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate((16 << 20) + 1); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	if _, _, _, err := restoreBundleIdentity(bundle); err == nil {
		t.Fatal("accepted oversized manifest")
	}
}

func TestRestoreEnvironmentAllowlistAndFileMapping(t *testing.T) {
	file := filepath.Join(t.TempDir(), "credentials,private")
	if err := os.WriteFile(file, []byte("private credentials"), 0600); err != nil {
		t.Fatal(err)
	}
	file, _ = filepath.EvalSymlinks(file)
	host := []string{"PATH=/bin", "PORCUPINE_API_KEY=private", "PORCUPINE_OAUTH_SECRET=private", "PORCUPINE_SMTP_PASSWORD=private", "PORCUPINE_BOOTSTRAP_TOKEN=private", "PORCUPINE_DUCKLAKE_DIR=/production", "AWS_SECRET_ACCESS_KEY=aws-private", "PORCUPINE_BACKUP_S3_BUCKET=archive", "PORCUPINE_DUCKLAKE_S3_BUCKET=target", "PORCUPINE_DUCKLAKE_MEMORY_MB=512", "AWS_SHARED_CREDENTIALS_FILE=" + file, "PORCUPINE_BACKUP_S3_CA_CERT_FILE=" + file}
	env, args, err := restoreEnvironment(host)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"AWS_SECRET_ACCESS_KEY", "PORCUPINE_BACKUP_S3_BUCKET", "PORCUPINE_DUCKLAKE_S3_BUCKET", "PORCUPINE_DUCKLAKE_MEMORY_MB", "AWS_SHARED_CREDENTIALS_FILE", "PORCUPINE_BACKUP_S3_CA_CERT_FILE"} {
		if !slices.Contains(args, name) {
			t.Errorf("missing allowed variable %s", name)
		}
	}
	for _, denied := range []string{"PORCUPINE_API_KEY", "PORCUPINE_OAUTH_SECRET", "PORCUPINE_SMTP_PASSWORD", "PORCUPINE_BOOTSTRAP_TOKEN", "PORCUPINE_DUCKLAKE_DIR", "aws-private"} {
		if slices.Contains(args, denied) {
			t.Errorf("unauthorized forwarded name or value %s", denied)
		}
	}
	for _, name := range []string{"AWS_SHARED_CREDENTIALS_FILE", "PORCUPINE_BACKUP_S3_CA_CERT_FILE"} {
		mapped := false
		for _, entry := range env {
			if strings.HasPrefix(entry, name+"=/run/porcupine-restore/file-") {
				mapped = true
			}
		}
		if !mapped {
			t.Errorf("file environment %s was not rewritten", name)
		}
	}
	if !reflect.DeepEqual(host[0:2], env[0:2]) {
		t.Fatal("Docker client environment unexpectedly changed")
	}
	for i, arg := range args {
		if arg == "--mount" {
			fields, err := csv.NewReader(strings.NewReader(args[i+1])).Read()
			if err != nil || !slices.Contains(fields, "src="+file) || !slices.Contains(fields, "readonly") {
				t.Fatalf("invalid credential mount %v / %v", fields, err)
			}
		}
	}
}

func TestRestoreRequiresAcknowledgmentPrivateTokenAndExclusiveSources(t *testing.T) {
	for _, args := range [][]string{
		{"--reset-access=false", "--backup-id", "pc_backup_test", "--source-id", "source"},
		{"--bundle", "/does/not/exist", "--backup-id", "pc_backup_test"},
		{"--backup-id", "pc_backup_test"},
		{"--backup-id", "../escape", "--source-id", "source"},
		{"--timeout", "0s", "--backup-id", "pc_backup_test", "--source-id", "source"},
	} {
		docker := &fakeRestoreDocker{t: t}
		_, _, err := invokeRestore(t, context.Background(), docker, args...)
		if err == nil || len(docker.calls) != 0 {
			t.Fatalf("invalid inputs reached Docker: %v", args)
		}
	}
	file := filepath.Join(t.TempDir(), "token")
	for _, token := range []string{"too-short", strings.Repeat("x", 31) + " " + strings.Repeat("x", 32), strings.Repeat("x", 4097)} {
		if err := os.WriteFile(file, []byte(token), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := restoreBootstrapToken(file); err == nil {
			t.Fatal("accepted invalid token")
		}
	}
	if err := os.WriteFile(file, []byte(restoreTestToken), 0600); err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" {
		if err := os.Chmod(file, 0644); err != nil {
			t.Fatal(err)
		}
		if _, err := restoreBootstrapToken(file); err == nil {
			t.Fatal("accepted publicly readable token file")
		}
	}
}

func TestRestoreStableImageDefaultAndDevelopmentRequirement(t *testing.T) {
	previous := version
	t.Cleanup(func() { version = previous })
	for _, release := range []string{"dev", "dev-custom", "v1.2.3"} {
		version = release
		docker := &fakeRestoreDocker{t: t}
		_, _, err := invokeRestore(t, context.Background(), docker, append(remoteRestoreFlags(), "--image", "")...)
		if strings.HasPrefix(release, "dev") {
			if err == nil || len(docker.calls) != 0 {
				t.Fatal("development build selected an implicit image")
			}
		} else {
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Contains(restoreRunCall(t, docker).args, "ghcr.io/olup/porcupine:1.2.3") {
				t.Fatal("stable CLI did not select matching image")
			}
		}
	}
}

func TestRestoreOutputLimitCannotBeBypassedByCopy(t *testing.T) {
	var out restoreOutput
	_, err := io.Copy(&out, strings.NewReader(strings.Repeat("x", (16<<20)+1)))
	if err == nil || len(out.Bytes()) > 16<<20 {
		t.Fatal("Docker output was not bounded")
	}
}

func TestRestoreNativeDiagnosticsAreBoundedAndRedacted(t *testing.T) {
	t.Setenv("PORCUPINE_BACKUP_S3_SECRET_ACCESS_KEY", "forwarded-private-secret")
	for _, tc := range []struct{ name, diagnostic, want string }{
		{"version", "native restore failed: backup version differs from this release\n", "backup version differs from this release"},
		{"overlap", "native restore failed: restore source and target paths overlap\n", "restore source and target paths overlap"},
		{"missing file", "native restore failed: open /backup/pc_backup_test/acceptance.sqlite: no such file or directory\n", "no such file or directory"},
		{"token echo", "native restore failed: rejected " + restoreTestToken + " and forwarded-private-secret\n", "rejected [redacted] and [redacted]"},
		{"terminal controls", "native restore failed: invalid\tversion\x1b\u202e\r\nprivate following line", "invalid version"},
		{"unknown Docker diagnostic", "docker: unrecognized failure " + restoreTestToken, "Docker diagnostics suppressed"},
		{"untrusted prefix", "unexpected output\nnative restore failed: reflected " + restoreTestToken, "Docker diagnostics suppressed"},
		{"private request", `native restore failed: {"bootstrap_token":"` + restoreTestToken + `"}`, "Docker diagnostics suppressed"},
		{"oversized stderr", "native restore failed: " + strings.Repeat("x", 64<<10), "Docker diagnostics suppressed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			docker := &fakeRestoreDocker{t: t, stderr: tc.diagnostic}
			out, progress, err := invokeRestore(t, context.Background(), docker, remoteRestoreFlags()...)
			if err == nil || !strings.Contains(err.Error(), tc.want) || !strings.Contains(err.Error(), "retained") || out != "" {
				t.Fatalf("unexpected native failure: %q / %v", out, err)
			}
			for _, private := range []string{restoreTestToken, "forwarded-private-secret", "private following line", "\x1b", "\u202e", "bootstrap_token"} {
				if strings.Contains(err.Error()+progress, private) {
					t.Errorf("unsafe diagnostic contains %q", private)
				}
			}
		})
	}
	var diagnostics restoreDiagnostics
	raw := strings.Repeat("x", (64<<10)+200)
	if n, err := io.Copy(&diagnostics, strings.NewReader(raw)); err != nil || n != int64(len(raw)) || len(diagnostics.data) != 64<<10 || !diagnostics.overflow {
		t.Fatal("native stderr capture is not bounded")
	}
	var long restoreDiagnostics
	_, _ = long.Write([]byte("native restore failed: " + strings.Repeat("é", 2000)))
	message := restoreNativeDiagnostic(&restoreDockerFailure{diagnostics: long}, restoreTestToken, nil, nil)
	if len([]rune(message)) != 1025 || !strings.HasSuffix(message, "…") {
		t.Fatal("native diagnostic display limit not enforced")
	}
}
