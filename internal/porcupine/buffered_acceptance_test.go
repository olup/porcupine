package porcupine

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/acceptance"
)

func testBufferConfig() acceptance.BufferConfig {
	c := acceptance.DefaultBufferConfig()
	c.FlushInterval = time.Hour
	return c
}
func newTestBuffer(t testing.TB, c acceptance.BufferConfig, writer acceptance.FlushFunc) *acceptance.Buffer {
	t.Helper()
	buffer, err := acceptance.NewBuffer(t.Context(), c, writer, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return buffer
}
func testBufferBatch(t testing.TB, project string, n int) acceptance.Batch {
	t.Helper()
	inputs := make(eventBatch, n)
	for i := range inputs {
		inputs[i] = normalizedTrack("native", "stable-message")
	}
	batch, err := prepareLiveBatch(project, "source", inputs, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	return batch
}

func TestLiveAcceptanceRejectsUnavailableModeBeforeStorage(t *testing.T) {
	t.Parallel()
	for _, mode := range []AcceptanceMode{AcceptanceModeBuffered, "unknown"} {
		path := filepath.Join(t.TempDir(), "no-storage", "journal.db")
		if _, err := Open(Config{DBPath: path, AcceptanceMode: mode}); err == nil {
			t.Fatal("unavailable acceptance mode opened", mode)
		}
		if _, err := os.Stat(filepath.Dir(path)); !os.IsNotExist(err) {
			t.Fatal("invalid mode created storage", err)
		}
	}
}
