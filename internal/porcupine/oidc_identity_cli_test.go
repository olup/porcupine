package porcupine

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestOIDCIdentityGeneratedCLIRecoveryJourney(t *testing.T) {
	t.Parallel()
	binary := filepath.Join(t.TempDir(), "porcupine")
	build := exec.Command("go", "build", "-o", binary, "./cmd/porcupine")
	build.Dir = "../../cli"
	build.Env = append(os.Environ(), "CGO_ENABLED=0")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build standalone CLI: %s %v", output, err)
	}
	f := newOIDCFixture(t)
	f.login(t)
	operator := accountTestToken(t, f.s)
	config := t.TempDir()
	run := func(credential string, denied bool, args ...string) map[string]any {
		t.Helper()
		command := exec.Command(binary, append([]string{"--url", f.host.URL}, args...)...)
		command.Env = append(os.Environ(), "XDG_CONFIG_HOME="+config, "PORCUPINE_API_KEY="+credential)
		output, err := command.CombinedOutput()
		if strings.Contains(string(output), credential) {
			t.Fatal("CLI exposed credential")
		}
		if denied {
			if err == nil || !strings.Contains(string(output), "403") {
				t.Fatalf("CLI permission boundary failed: %s %v", output, err)
			}
			return nil
		}
		if err != nil {
			t.Fatalf("CLI recovery operation failed: %s %v", output, err)
		}
		var result map[string]any
		if len(output) != 0 {
			if err := json.Unmarshal(output, &result); err != nil {
				t.Fatalf("CLI output is not JSON: %s %v", output, err)
			}
		}
		return result
	}
	list := func() []any {
		t.Helper()
		return run(operator, false, "list-human-identities", "--id", f.humanID, "--limit", "1")["identities"].([]any)
	}
	identities := list()
	if len(identities) != 1 {
		t.Fatalf("CLI lost identity binding: %v", identities)
	}
	binding := identities[0].(map[string]any)["id"].(string)
	f.resource = f.host.URL + "/api/v1"
	reader := f.issueToken(t, "project-a")
	run(reader, true, "list-human-identities", "--id", f.humanID)
	run(reader, true, "unlink-human-identity", "--id", f.humanID, "--identity-id", binding)
	run(operator, false, "unlink-human-identity", "--id", f.humanID, "--identity-id", binding)
	if len(list()) != 0 {
		t.Fatal("CLI unlink left identity binding")
	}
	f.issueToken(t, "project-a")
	run(operator, false, "unlink-human-identity", "--id", f.humanID, "--identity-id", binding)
	if fresh := list(); len(fresh) != 1 || fresh[0].(map[string]any)["id"] == binding {
		t.Fatal("CLI retry removed freshly relinked identity")
	}
}
