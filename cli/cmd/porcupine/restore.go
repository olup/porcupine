package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/urfave/cli/v3"
)

const restoreOwnerLabel = "io.porcupine.restore.owner"

var (
	restoreSegment       = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]*$`)
	restoreStableVersion = regexp.MustCompile(`^v?[0-9]+\.[0-9]+\.[0-9]+$`)
)

// The standalone CLI delegates database work to the matching native image.
type restoreDockerRunner func(context.Context, []string, []string, io.Reader, io.Writer) error

func runRestoreDocker(ctx context.Context, args, env []string, input io.Reader, output io.Writer) error {
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Env, cmd.Stdin, cmd.Stdout = env, input, output
	var diagnostics restoreDiagnostics
	cmd.Stderr = &diagnostics
	if err := cmd.Run(); err != nil {
		return &restoreDockerFailure{diagnostics: diagnostics}
	}
	return nil
}

type restoreDockerFailure struct{ diagnostics restoreDiagnostics }

func (*restoreDockerFailure) Error() string { return "Docker restore command failed" }

// Excess stderr is discarded rather than breaking the child's stderr pipe.
type restoreDiagnostics struct {
	data     []byte
	overflow bool
}

func (d *restoreDiagnostics) Write(p []byte) (int, error) {
	n := len(p)
	remaining := (64 << 10) - len(d.data)
	if len(p) > remaining {
		d.overflow = true
		p = p[:remaining]
	}
	d.data = append(d.data, p...)
	return n, nil
}

func restoreNativeDiagnostic(err error, token string, env, args []string) string {
	var failure *restoreDockerFailure
	if !errors.As(err, &failure) || failure.diagnostics.overflow {
		return ""
	}
	const prefix = "native restore failed: "
	raw := string(failure.diagnostics.data)
	if !strings.HasPrefix(raw, prefix) {
		return ""
	}
	message, _, _ := strings.Cut(strings.TrimPrefix(raw, prefix), "\n")
	// A reflected private JSON request is never an operator diagnostic.
	if strings.Contains(message, "bootstrap_token") || strings.HasPrefix(strings.TrimSpace(message), "{") {
		return ""
	}
	secrets := []string{token}
	for i, arg := range args {
		if arg != "--env" || i+1 >= len(args) {
			continue
		}
		name := args[i+1]
		if !strings.Contains(name, "SECRET") && !strings.Contains(name, "TOKEN") && !strings.Contains(name, "ACCESS_KEY") {
			continue
		}
		for _, entry := range env {
			key, value, _ := strings.Cut(entry, "=")
			if key == name && value != "" {
				secrets = append(secrets, value)
			}
		}
	}
	// Redact before trimming the message so truncation cannot expose a token prefix.
	slices.SortFunc(secrets, func(a, b string) int { return len(b) - len(a) })
	for _, secret := range secrets {
		message = strings.ReplaceAll(message, secret, "[redacted]")
		encoded, _ := json.Marshal(secret)
		message = strings.ReplaceAll(message, string(encoded[1:len(encoded)-1]), "[redacted]")
	}
	message = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) || unicode.In(r, unicode.Cf, unicode.Zl, unicode.Zp) {
			return ' '
		}
		return r
	}, message)
	message = strings.Join(strings.Fields(message), " ")
	if runes := []rune(message); len(runes) > 1024 {
		message = string(runes[:1024]) + "…"
	}
	return message
}

func restoreCommand() *cli.Command { return restoreCommandWithRunner(runRestoreDocker) }

func restoreCommandWithRunner(run restoreDockerRunner) *cli.Command {
	return &cli.Command{
		Name: "restore", Usage: "Restore a native backup offline into a new Docker volume and reset access",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "bundle", Usage: "Absolute path to a completed local backup bundle"},
			&cli.StringFlag{Name: "backup-id", Usage: "Completed S3 backup ID"},
			&cli.StringFlag{Name: "source-id", Usage: "S3 backup source ID"},
			&cli.StringFlag{Name: "volume", Usage: "New Docker volume name; defaults to a unique generated name"},
			&cli.StringFlag{Name: "image", Usage: "Matching native server image; required for development CLI builds"},
			&cli.StringFlag{Name: "bootstrap-token-file", Required: true, Usage: "Private file containing a fresh recovery token of at least 32 characters; restrict access with file permissions or Windows ACLs"},
			&cli.BoolFlag{Name: "reset-access", Usage: "Acknowledge revocation of archived credentials and cancellation of outbound work"},
			&cli.StringFlag{Name: "memory", Value: "2g", Usage: "Docker memory limit"},
			&cli.StringFlag{Name: "cpus", Value: "2", Usage: "Docker CPU limit"},
			&cli.DurationFlag{Name: "timeout", Value: time.Hour, Usage: "Maximum time for the whole restore"},
		},
		Action: func(ctx context.Context, c *cli.Command) error { return restoreOffline(ctx, c, run) },
	}
}

type offlineRestoreRequest struct {
	BackupID                     string `json:"backup_id"`
	SourceID                     string `json:"source_id"`
	LocalBundlePath              string `json:"local_bundle_path,omitempty"`
	TargetDBPath                 string `json:"target_db_path"`
	TargetDuckLakeDirectory      string `json:"target_ducklake_directory"`
	ResetCredentialsAcknowledged bool   `json:"reset_credentials_acknowledged"`
	BootstrapToken               string `json:"bootstrap_token"`
	ChownToNonroot               bool   `json:"chown_to_nonroot"`
}

func restoreOffline(parent context.Context, c *cli.Command, run restoreDockerRunner) error {
	if c.NArg() != 0 {
		return errors.New("restore accepts flags only")
	}
	if !c.Bool("reset-access") {
		return errors.New("restore requires --reset-access to revoke archived credentials and cancel outbound work")
	}
	if c.Duration("timeout") <= 0 {
		return errors.New("--timeout must be positive")
	}
	ctx, cancel := context.WithTimeout(parent, c.Duration("timeout"))
	defer cancel()
	image := c.String("image")
	if image == "" {
		if !restoreStableVersion.MatchString(version) {
			return errors.New("development builds require an explicit --image matching the backup")
		}
		image = "ghcr.io/olup/porcupine:" + strings.TrimPrefix(version, "v")
	}
	if strings.HasPrefix(image, "-") || strings.ContainsAny(image, " \t\r\n") {
		return errors.New("invalid --image")
	}
	request := offlineRestoreRequest{BackupID: c.String("backup-id"), SourceID: c.String("source-id"), TargetDBPath: "/data/porcupine.db", TargetDuckLakeDirectory: "/data/ducklake", ResetCredentialsAcknowledged: true, ChownToNonroot: true}
	bundle := c.String("bundle")
	if bundle != "" {
		if c.IsSet("backup-id") || c.IsSet("source-id") {
			return errors.New("--bundle cannot be combined with --backup-id or --source-id")
		}
		var err error
		bundle, request.BackupID, request.SourceID, err = restoreBundleIdentity(bundle)
		if err != nil {
			return err
		}
		request.LocalBundlePath = "/backup/" + request.BackupID
	} else if !strings.HasPrefix(request.BackupID, "pc_backup_") || len(request.BackupID) <= len("pc_backup_") || !restoreSegment.MatchString(request.BackupID) || !restoreSegment.MatchString(request.SourceID) {
		return errors.New("provide --bundle or both valid --backup-id and --source-id")
	}
	token, err := restoreBootstrapToken(c.String("bootstrap-token-file"))
	if err != nil {
		return err
	}
	request.BootstrapToken = token
	privateInput, err := json.Marshal(request)
	if err != nil {
		return errors.New("cannot encode private restore request")
	}
	owner, err := restoreRandomID()
	if err != nil {
		return err
	}
	volume := c.String("volume")
	if volume == "" {
		volume = "porcupine-restore-" + owner
	}
	if !restoreSegment.MatchString(volume) || len(volume) < 2 {
		return errors.New("--volume requires a valid Docker volume name of at least two characters")
	}
	container := "porcupine-restore-" + owner
	env, envArgs, err := restoreEnvironment(os.Environ())
	if err != nil {
		return err
	}
	args := []string{"run", "--rm", "--interactive", "--name", container, "--user", "0:0", "--read-only", "--tmpfs", "/tmp:rw,nosuid,nodev", "--memory", c.String("memory"), "--cpus", c.String("cpus"), "--mount", restoreMount("type=volume", "src="+volume, "dst=/data")}
	if bundle != "" {
		args = append(args, "--mount", restoreMount("type=bind", "src="+bundle, "dst="+request.LocalBundlePath, "readonly"))
	}
	args = append(args, envArgs...)
	args = append(args, image, "restore-native-backup")
	var volumes restoreOutput
	if err = run(ctx, []string{"volume", "ls", "--filter", "name=^" + regexp.QuoteMeta(volume) + "$", "--format", "{{.Name}}"}, env, nil, &volumes); err != nil {
		return restoreDockerError(ctx, "cannot check Docker volumes")
	}
	if strings.TrimSpace(volumes.String()) != "" {
		return errors.New("restore refuses an existing Docker volume; choose a new --volume")
	}
	if err = run(ctx, []string{"volume", "create", "--label", restoreOwnerLabel + "=" + owner, "--label", "io.porcupine.restore.attempt=true", volume}, env, nil, io.Discard); err != nil {
		return restoreDockerError(ctx, "cannot create restore volume; any created volume has been retained")
	}
	var labels restoreOutput
	if err = run(ctx, []string{"volume", "inspect", "--format", "{{json .Labels}}", volume}, env, nil, &labels); err != nil {
		return restoreDockerError(ctx, "cannot verify restore volume ownership; volume retained")
	}
	var ownership map[string]string
	if json.Unmarshal(labels.Bytes(), &ownership) != nil || ownership[restoreOwnerLabel] != owner {
		return errors.New("Docker volume was created concurrently by another operation; restore refused and volume retained")
	}
	fmt.Fprintf(c.Root().ErrWriter, "Restoring into new Docker volume %s. Failed volumes are retained with the restore attempt label.\n", volume)
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if run(cleanup, []string{"rm", "--force", container}, env, nil, io.Discard) != nil && ctx.Err() != nil {
			fmt.Fprintf(c.Root().ErrWriter, "Container cleanup could not be confirmed for %s; volume %s retained.\n", container, volume)
		}
	}()
	var result restoreOutput
	if err = run(ctx, args, env, bytes.NewReader(privateInput), &result); err != nil {
		if diagnostic := restoreNativeDiagnostic(err, token, env, args); diagnostic != "" && ctx.Err() == nil {
			return fmt.Errorf("native restore failed; new volume %s retained for inspection: %s", volume, diagnostic)
		}
		return restoreDockerError(ctx, "native restore failed; new volume "+volume+" retained for inspection")
	}
	if !json.Valid(result.Bytes()) || bytes.Contains(result.Bytes(), []byte(token)) {
		return errors.New("native restore returned an invalid result; new volume " + volume + " retained for inspection")
	}
	fmt.Fprintf(c.Root().ErrWriter, "Restore completed in Docker volume %s. Save the private JSON result securely.\n", volume)
	_, err = c.Root().Writer.Write(result.Bytes())
	return err
}

func restoreDockerError(ctx context.Context, message string) error {
	if ctx.Err() != nil {
		return fmt.Errorf("%s: %w", message, ctx.Err())
	}
	return errors.New(message + "; Docker diagnostics suppressed to protect private credentials")
}

// Bounded output prevents Docker or an unexpected image from exhausting CLI memory.
type restoreOutput struct{ buffer bytes.Buffer }

func (b *restoreOutput) Bytes() []byte  { return b.buffer.Bytes() }
func (b *restoreOutput) String() string { return b.buffer.String() }

func (b *restoreOutput) Write(p []byte) (int, error) {
	if len(p) > (16<<20)-b.buffer.Len() {
		return 0, errors.New("Docker output exceeds the restore output limit")
	}
	return b.buffer.Write(p)
}

func restoreRandomID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", errors.New("cannot generate restore ownership ID")
	}
	return hex.EncodeToString(raw[:]), nil
}

func restoreBootstrapToken(path string) (string, error) {
	info, err := os.Lstat(path)
	// Windows reports POSIX permission bits independently of the file's ACL.
	if err != nil || !info.Mode().IsRegular() || runtime.GOOS != "windows" && info.Mode().Perm()&0077 != 0 {
		return "", errors.New("--bootstrap-token-file must be a private regular file with no group or other permissions")
	}
	file, err := os.Open(path)
	if err != nil {
		return "", errors.New("cannot read private bootstrap token file")
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, 4097))
	token := strings.TrimSpace(string(data))
	if err != nil || len(data) > 4096 || !utf8.ValidString(token) || utf8.RuneCountInString(token) < 32 || strings.ContainsFunc(token, unicode.IsSpace) {
		return "", errors.New("bootstrap token file must contain one fresh token of at least 32 characters and at most 4096 bytes")
	}
	return token, nil
}

func restoreBundleIdentity(path string) (string, string, string, error) {
	if !filepath.IsAbs(path) {
		return "", "", "", errors.New("--bundle requires an absolute completed bundle path")
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", "", "", errors.New("cannot read local backup bundle")
	}
	if strings.HasSuffix(filepath.Base(resolved), ".incomplete") {
		return "", "", "", errors.New("--bundle requires a completed backup bundle")
	}
	manifest := filepath.Join(resolved, "manifest.json")
	info, err := os.Lstat(manifest)
	if err != nil || !info.Mode().IsRegular() || info.Size() > 16<<20 {
		return "", "", "", errors.New("bundle requires a regular manifest.json of at most 16 MiB")
	}
	file, err := os.Open(manifest)
	if err != nil {
		return "", "", "", errors.New("cannot read bundle manifest")
	}
	defer file.Close()
	var identity struct {
		Format   string `json:"format"`
		ID       string `json:"id"`
		SourceID string `json:"source_id"`
	}
	decoder := json.NewDecoder(io.LimitReader(file, (16<<20)+1))
	var trailing any
	if decoder.Decode(&identity) != nil || decoder.Decode(&trailing) != io.EOF || identity.Format != "porcupine-native-v1" || !strings.HasPrefix(identity.ID, "pc_backup_") || len(identity.ID) <= len("pc_backup_") || !restoreSegment.MatchString(identity.ID) || !restoreSegment.MatchString(identity.SourceID) {
		return "", "", "", errors.New("bundle manifest has invalid native backup identity")
	}
	return resolved, identity.ID, identity.SourceID, nil
}

func restoreMount(fields ...string) string {
	var out strings.Builder
	w := csv.NewWriter(&out)
	_ = w.Write(fields)
	w.Flush()
	return strings.TrimSuffix(out.String(), "\n")
}

func restoreEnvironment(host []string) ([]string, []string, error) {
	allowed := map[string]bool{}
	for _, name := range []string{
		"PORCUPINE_DUCKLAKE_MEMORY_MB", "PORCUPINE_DUCKLAKE_THREADS", "PORCUPINE_DUCKLAKE_MAX_OPEN_PROJECTS",
		"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_REGION", "AWS_DEFAULT_REGION", "AWS_PROFILE", "AWS_DEFAULT_PROFILE", "AWS_SDK_LOAD_CONFIG", "AWS_EC2_METADATA_DISABLED", "AWS_EC2_METADATA_SERVICE_ENDPOINT", "AWS_EC2_METADATA_SERVICE_ENDPOINT_MODE", "AWS_CONTAINER_CREDENTIALS_FULL_URI", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_AUTHORIZATION_TOKEN", "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", "AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_ROLE_ARN", "AWS_ROLE_SESSION_NAME", "AWS_CONFIG_FILE", "AWS_SHARED_CREDENTIALS_FILE", "AWS_CA_BUNDLE", "AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3", "AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", "SSL_CERT_FILE",
	} {
		allowed[name] = true
	}
	files := map[string]bool{"PORCUPINE_BACKUP_S3_CA_CERT_FILE": true, "PORCUPINE_DUCKLAKE_S3_CA_CERT_FILE": true, "AWS_CONFIG_FILE": true, "AWS_SHARED_CREDENTIALS_FILE": true, "AWS_WEB_IDENTITY_TOKEN_FILE": true, "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE": true, "AWS_CA_BUNDLE": true, "SSL_CERT_FILE": true}
	env := slices.Clone(host)
	var args []string
	for i, entry := range host {
		name, value, found := strings.Cut(entry, "=")
		if !found || value == "" || !(allowed[name] || strings.HasPrefix(name, "PORCUPINE_BACKUP_S3_") || strings.HasPrefix(name, "PORCUPINE_DUCKLAKE_S3_")) {
			continue
		}
		if files[name] {
			path, err := filepath.Abs(value)
			if err != nil {
				return nil, nil, fmt.Errorf("cannot resolve restore file for %s", name)
			}
			path, err = filepath.EvalSymlinks(path)
			if err != nil {
				return nil, nil, fmt.Errorf("cannot read restore file for %s", name)
			}
			info, err := os.Stat(path)
			if err != nil || !info.Mode().IsRegular() {
				return nil, nil, fmt.Errorf("%s must identify a regular file", name)
			}
			containerPath := fmt.Sprintf("/run/porcupine-restore/file-%d", i)
			args = append(args, "--mount", restoreMount("type=bind", "src="+path, "dst="+containerPath, "readonly"))
			env[i] = name + "=" + containerPath
		}
		args = append(args, "--env", name)
	}
	return env, args, nil
}
