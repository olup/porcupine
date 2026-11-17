package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestConfigFileStrictDecoding(t *testing.T) {
	t.Parallel()
	for _, body := range []string{
		`{"unexpected":"secret-value"}`, `{"query":{"max_rowz":1}}`,
		`{"query":{"max_rows":"secret-value"}}`, `{} {}`, `null`, `[]`,
		`{"bootstrap_token":secret-value}`, strings.Repeat("x", (1<<20)+1),
	} {
		t.Run(body[:min(len(body), 50)], func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "porcupine.json")
			if err := os.WriteFile(path, []byte(body), 0600); err != nil {
				t.Fatal(err)
			}
			_, err := loadSettings(path, true)
			if err == nil || strings.Contains(err.Error(), "secret-value") {
				t.Fatalf("unsafe or absent error: %v", err)
			}
		})
	}
}

func TestConfigEnvironmentOverrideAndUnknownVariable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "porcupine.json")
	if err := os.WriteFile(path, []byte(`{"listen":"127.0.0.1:7777","query":{"max_rows":900},"ducklake":{"cleanup_enabled":false}}`), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PORCUPINE_ADDR", "127.0.0.1:8888")
	t.Setenv("PORCUPINE_QUERY_MAX_ROWS", "800")
	s, err := loadSettings(path, true)
	if err != nil {
		t.Fatal(err)
	}
	if s.get("PORCUPINE_ADDR") != "127.0.0.1:8888" || s.get("PORCUPINE_QUERY_MAX_ROWS") != "800" || s.get("PORCUPINE_DUCKLAKE_CLEANUP_ENABLED") != "false" {
		t.Fatal("file/env precedence or bool conversion failed")
	}
	t.Setenv("PORCUPINE_QUERY_MAX_ROWZ", "private-secret")
	if _, err := loadSettings(path, true); err == nil || !strings.Contains(err.Error(), "PORCUPINE_QUERY_MAX_ROWZ") || strings.Contains(err.Error(), "private-secret") {
		t.Fatalf("unknown setting: %v", err)
	}
}

func TestConfigInitAndCheck(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "private", "porcupine.json")
	var output bytes.Buffer
	if err := run([]string{"init", "--config", path}, nil, &output); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var config FileConfig
	if err := json.Unmarshal(raw, &config); err != nil {
		t.Fatal(err)
	}
	key, err := base64.StdEncoding.DecodeString(config.Pipeline.Key)
	if err != nil || len(key) != 32 || len(config.BootstrapToken) < 32 {
		t.Fatal("invalid initial secrets")
	}
	if strings.Contains(output.String(), config.BootstrapToken) || strings.Contains(output.String(), config.Pipeline.Key) {
		t.Fatal("init leaked a secret")
	}
	if config.Storage.Control == config.Storage.Journal {
		t.Fatal("journal and control must be separate")
	}
	if config.Queue.ReceiptRetention != "0" {
		t.Fatal("init must preserve local retry receipts by default")
	}
	if runtime.GOOS != "windows" {
		info, _ := os.Stat(path)
		dir, _ := os.Stat(filepath.Dir(path))
		if info.Mode().Perm() != 0600 || dir.Mode().Perm() != 0700 {
			t.Fatal("initial config must be private")
		}
	}
	if err := run([]string{"init", "--config", path}, nil, &output); err == nil {
		t.Fatal("init overwrote configuration")
	}
	unchanged, _ := os.ReadFile(path)
	if !bytes.Equal(raw, unchanged) {
		t.Fatal("existing config changed")
	}
	output.Reset()
	if err := run([]string{"config", "check", "--config", path}, nil, &output); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(output.String(), config.BootstrapToken) || strings.Contains(output.String(), config.Pipeline.Key) || !strings.Contains(output.String(), "[redacted]") {
		t.Fatal("config check leaked secrets or omitted masking")
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(path), "data")); !os.IsNotExist(err) {
		t.Fatal("config check opened runtime storage")
	}
}

func TestConfigCheckRedactsAllCredentialFields(t *testing.T) {
	t.Parallel()
	s := settings{
		"PORCUPINE_BOOTSTRAP_TOKEN": "secret", "PORCUPINE_DUCKLAKE_POSTGRES_URL": "postgres://user:secret@db/test",
		"PORCUPINE_CONTROL_DATABASE_URL": "postgres://user:secret@db/control", "PORCUPINE_SHARED_ROUTING_SECRET": "secret",
		"PORCUPINE_WORKER_URL":         "https://user:secret@worker.example.com",
		"PORCUPINE_OIDC_CLIENT_SECRET": "secret", "PORCUPINE_SMTP_PASSWORD_FILE": "/secret/file",
		"PORCUPINE_DUCKLAKE_S3_SECRET_ACCESS_KEY": "secret", "PORCUPINE_EXPORT_S3_SESSION_TOKEN": "secret", "PORCUPINE_BACKUP_S3_ACCESS_KEY_ID": "secret",
	}
	for name, value := range s.redacted() {
		if value != "[redacted]" {
			t.Fatalf("%s leaked %q", name, value)
		}
	}
}

func TestConfigInitDockerAndMissingFile(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "porcupine.json")
	if _, err := loadSettings(path, false); err != nil {
		t.Fatal(err)
	}
	if _, err := loadSettings(path, true); err == nil {
		t.Fatal("explicit missing config accepted")
	}
	if err := writeInitialConfig(path, true); err != nil {
		t.Fatal(err)
	}
	s, err := loadSettings(path, true)
	if err != nil {
		t.Fatal(err)
	}
	if s.get("PORCUPINE_ADDR") != "0.0.0.0:8080" || s.get("PORCUPINE_DB") != "/data/porcupine.db" || s.get("PORCUPINE_CONTROL_DB") != "/data/porcupine.db.control" {
		t.Fatal("incorrect Docker config")
	}
}

func TestConfigCheckNoOutputOnFailure(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "porcupine.json")
	if err := os.WriteFile(path, []byte(`{"bootstrap_token":"secret","query":{"max_rows":-1}}`), 0600); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := run([]string{"config", "check", "--config", path}, nil, &output); err == nil || output.Len() != 0 {
		t.Fatalf("failed config produced output: %v %s", err, output.String())
	}
}

func TestConfigReceiptRetention(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		value   string
		want    time.Duration
		invalid bool
	}{
		{"", 0, false}, {"0", 0, false}, {"48h", 48 * time.Hour, false},
		{"-1h", 0, true}, {"2h", 0, true}, {"never", 0, true},
	} {
		t.Run(tc.value, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "porcupine.json")
			body, _ := json.Marshal(map[string]any{"queue": map[string]string{"receipt_retention": tc.value}})
			if err := os.WriteFile(path, body, 0600); err != nil {
				t.Fatal(err)
			}
			s, err := loadSettings(path, true)
			if err != nil {
				t.Fatal(err)
			}
			_, config, err := s.serviceConfig()
			if (err != nil) != tc.invalid {
				t.Fatalf("unexpected validation %v", err)
			}
			if !tc.invalid && config.ReceiptRetention != tc.want {
				t.Fatalf("got %v want %v", config.ReceiptRetention, tc.want)
			}
		})
	}
}

func TestReceiptRetentionDefaultsAndEnvironmentOverride(t *testing.T) {
	// Environment overrides are process-wide, so this test runs before the
	// parallel configuration readers.
	for _, test := range []struct {
		file, environment string
		want              time.Duration
	}{
		{"", "", 0}, {"", "0", 0}, {"", "48h", 48 * time.Hour},
		{"720h", "0", 0}, {"0", "48h", 48 * time.Hour},
	} {
		t.Setenv("PORCUPINE_RECEIPT_RETENTION", test.environment)
		var config FileConfig
		config.Queue.ReceiptRetention = test.file
		got, err := fileSettings(config).receiptRetention()
		if err != nil || got != test.want {
			t.Fatalf("file=%q env=%q got=%v want=%v error=%v", test.file, test.environment, got, test.want, err)
		}
	}
}

func TestConfigCheckValidatesRuntimeSemantics(t *testing.T) {
	t.Parallel()
	for _, body := range []string{
		`{"storage":{"journal":"same.db","control":"same.db"}}`,
		`{"allowed_origins":["https://*.example.com"]}`,
		`{"trusted_proxies":["not-a-network"]}`,
		`{"oauth":{"issuer":"http://insecure.example.com"}}`,
		`{"export":{"s3":{"bucket":"analytics","prefix":"results","region":"us-east-1"},"max_execution_seconds":999999999}}`,
		`{"storage":{"ducklake":"data/lake"},"backup":{"directory":"data/lake/backup"}}`,
		`{"bootstrap_token":"short"}`,
		`{"ducklake":{"metadata_timeout_seconds":2}}`,
		`{"export":{"s3":{"bucket":"invalid*bucket","prefix":"results","region":"us-east-1"}}}`,
	} {
		t.Run(body, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "porcupine.json")
			if err := os.WriteFile(path, []byte(body), 0600); err != nil {
				t.Fatal(err)
			}
			var output bytes.Buffer
			if err := run([]string{"config", "check", "--config", path}, nil, &output); err == nil || output.Len() != 0 {
				t.Fatalf("invalid runtime config accepted: %v %s", err, output.String())
			}
		})
	}
	path := filepath.Join(t.TempDir(), "existing-instance.json")
	if err := os.WriteFile(path, []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"config", "check", "--config", path}, nil, &bytes.Buffer{}); err != nil {
		t.Fatalf("an initialized instance may omit bootstrap: %v", err)
	}
}

func TestConfigInstallKeepsOldFileOnInvalidInput(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "porcupine.json")
	old := []byte(`{"listen":"127.0.0.1:8080"}`)
	if err := os.WriteFile(path, old, 0600); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []string{
		`{"not_an_option":"secret-value"}`, `{"query":{"max_rows":-1}}`,
		`{"bootstrap_token":"short"}`,
		`{"ducklake":{"metadata_timeout_seconds":2}}`, strings.Repeat("x", (1<<20)+1),
	} {
		var output bytes.Buffer
		if err := run([]string{"config", "install", "--config", path}, strings.NewReader(invalid), &output); err == nil || output.Len() != 0 {
			t.Fatalf("invalid replacement accepted: %v", err)
		}
		current, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(current, old) {
			t.Fatal("invalid input replaced existing configuration")
		}
	}
	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil || len(entries) != 1 {
		t.Fatal("temporary replacement files leaked")
	}
}

func TestConfigInstallReplacesPrivatelyAndMasksOutput(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "porcupine.json")
	if err := os.WriteFile(path, []byte(`{}`), 0400); err != nil {
		t.Fatal(err)
	}
	input := `{"bootstrap_token":"private-replacement-token","query":{"max_rows":123}}`
	var output bytes.Buffer
	if err := run([]string{"config", "install", "--config", path}, strings.NewReader(input), &output); err != nil {
		t.Fatal(err)
	}
	current, err := os.ReadFile(path)
	if err != nil || string(current) != input {
		t.Fatal("configuration was not replaced")
	}
	info, err := os.Stat(path)
	if err != nil || runtime.GOOS != "windows" && info.Mode().Perm() != 0600 {
		t.Fatal("replacement file must be private")
	}
	if strings.Contains(output.String(), "private-replacement-token") {
		t.Fatal("install printed a secret")
	}
	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil || len(entries) != 1 {
		t.Fatal("temporary replacement file leaked")
	}
}

func TestConfigInitConcurrentWritersKeepOneCompleteFile(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "porcupine.json")
	results := make(chan error, 2)
	for range 2 {
		go func() { results <- writeInitialConfig(path, false) }()
	}
	succeeded := 0
	for range 2 {
		if <-results == nil {
			succeeded++
		}
	}
	if succeeded != 1 {
		t.Fatalf("expected one successful initialization, got %d", succeeded)
	}
	if _, err := loadSettings(path, true); err != nil {
		t.Fatalf("initialization left an incomplete file: %v", err)
	}
	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil || len(entries) != 1 {
		t.Fatal("initialization left temporary files")
	}
}

func TestListenDefaultFileAndEnvironmentPrecedence(t *testing.T) {
	// This test changes a link-time default, so it runs before parallel tests.
	nativeDefault := defaultListenAddress
	if nativeDefault != "127.0.0.1:8080" {
		t.Fatalf("native default is %q", nativeDefault)
	}
	t.Cleanup(func() { defaultListenAddress = nativeDefault })
	defaultListenAddress = "0.0.0.0:8080"
	for _, tc := range []struct {
		name     string
		settings settings
		want     string
	}{
		{"container fallback", settings{}, "0.0.0.0:8080"},
		{"file wins", settings{"PORCUPINE_ADDR": "127.0.0.1:9090"}, "127.0.0.1:9090"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			address, _, err := tc.settings.serviceConfig()
			if err != nil || address != tc.want {
				t.Fatalf("address=%q err=%v", address, err)
			}
		})
	}
	t.Setenv("PORCUPINE_ADDR", "0.0.0.0:9191")
	address, _, err := (settings{"PORCUPINE_ADDR": "127.0.0.1:9090"}).serviceConfig()
	if err != nil || address != "0.0.0.0:9191" {
		t.Fatalf("environment did not win: %q %v", address, err)
	}
}
