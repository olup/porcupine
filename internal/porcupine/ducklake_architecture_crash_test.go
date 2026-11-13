//go:build ducklake

package porcupine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

type architectureCrashState struct {
	Project, Ingest     string
	IDs                 []string
	ProjectedBeforeKill int
}

func architectureCrash(t *testing.T) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, checkpoint := range []string{"accepted", "projected_before_ack"} {
		root := t.TempDir()
		var output bytes.Buffer
		command := func(stage string) *exec.Cmd {
			cmd := exec.Command(exe, "-test.run=^TestArchitectureCrashHelper$", "-test.v", "-test.timeout=90s")
			cmd.Env = append(os.Environ(), "PORCUPINE_ARCH_CRASH_ROOT="+root, "PORCUPINE_ARCH_CRASH_STAGE="+stage)
			return cmd
		}
		cmd := command(checkpoint)
		cmd.Stdout, cmd.Stderr = &output, &output
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		deadline := time.Now().Add(30 * time.Second)
		ready := false
		for time.Now().Before(deadline) {
			if _, err := os.Stat(filepath.Join(root, "ready.json")); err == nil {
				ready = true
				break
			}
			select {
			case err := <-done:
				t.Fatalf("crash child exited early: %v %s", err, output.String())
			default:
			}
			time.Sleep(20 * time.Millisecond)
		}
		if !ready {
			_ = cmd.Process.Kill()
			<-done
			t.Fatalf("crash barrier not reached: %s", output.String())
		}
		if err := cmd.Process.Kill(); err != nil {
			t.Fatal(err)
		}
		<-done
		start := time.Now()
		result, err := command("resume").CombinedOutput()
		if err != nil {
			t.Fatalf("resume failed: %v %s", err, result)
		}
		raw, err := os.ReadFile(filepath.Join(root, "result.json"))
		if err != nil {
			t.Fatal(err)
		}
		var report map[string]any
		if json.Unmarshal(raw, &report) != nil {
			t.Fatal("invalid crash result")
		}
		report["checkpoint"], report["restart_seconds"], report["verdict"] = checkpoint, time.Since(start).Seconds(), "VERIFIED"
		report["resources"] = architectureResources(root)
		architectureEmit("crash", "ducklake", report)
	}
}

func TestArchitectureCrashHelper(t *testing.T) {
	root := os.Getenv("PORCUPINE_ARCH_CRASH_ROOT")
	if root == "" {
		t.Skip("subprocess helper")
	}
	stage := os.Getenv("PORCUPINE_ARCH_CRASH_STAGE")
	config := Config{DBPath: filepath.Join(root, "queue.db"), BootstrapToken: adminToken, QueueMaxEvents: 10000}
	if stage == "resume" {
		config.DuckLake = &DuckLakeConfig{Directory: filepath.Join(root, "lake"), MemoryLimitMB: 512}
	}
	s, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	if stage != "resume" {
		lake, err := newDuckLake(DuckLakeConfig{Directory: filepath.Join(root, "lake"), MemoryLimitMB: 512})
		if err != nil {
			t.Fatal(err)
		}
		s.lake = lake
		s.lakeMemoryMB = 512
		s.deliveryMaxEvents = 500
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	h := s.Handler()
	var state architectureCrashState
	if stage == "resume" {
		raw, err := os.ReadFile(filepath.Join(root, "ready.json"))
		if err != nil || json.Unmarshal(raw, &state) != nil {
			t.Fatal("cannot read private recovery state")
		}
		if _, err := s.db.Exec("DROP TRIGGER IF EXISTS arch_crash_ack"); err != nil {
			t.Fatal(err)
		}
		if architectureDrain(t, s, 30*time.Second) != 0 {
			t.Fatal("restarted delivery worker did not drain")
		}
		count, _ := queueUsage(t, s)
		if count != 0 || architectureCount(t, h, state.Project) != 2000 {
			t.Fatal("hard-kill recovery lost or duplicated events")
		}
		if len(state.IDs) != 2000 {
			t.Fatal("missing saved event IDs")
		}
		for batch := 0; batch < 20; batch++ {
			retry := nativeRequest(t, s, state.Ingest, string(architectureLivePayload(batch*100, 100, 0)), 200)
			if retry["duplicates"] != float64(100) {
				t.Fatal("retry was not fully deduplicated")
			}
			for i := 0; i < 100; i++ {
				if fmt.Sprint(receiptFrom(t, retry, i)["event_id"]) != state.IDs[batch*100+i] {
					t.Fatal("retry changed a durable event ID after process kill")
				}
			}
		}
		count, _ = queueUsage(t, s)
		if count != 0 || architectureCount(t, h, state.Project) != 2000 {
			t.Fatal("retry changed counts after recovery")
		}
		report, _ := json.Marshal(map[string]any{"accepted": 2000, "projected": 2000, "duplicates_on_retry": 2000, "all_event_ids_stable": true, "projected_before_kill": state.ProjectedBeforeKill})
		if err := os.WriteFile(filepath.Join(root, "result.json"), report, 0600); err != nil {
			t.Fatal(err)
		}
		return
	}
	if stage != "accepted" && stage != "projected_before_ack" {
		t.Fatal("invalid crash checkpoint")
	}
	state.Project, state.Ingest = project(t, s)
	comparisonQuery(t, h, state.Project, "SELECT count(*) AS total FROM events")
	for i := 0; i < 20; i++ {
		response := nativeRequest(t, s, state.Ingest, string(architectureLivePayload(i*100, 100, 0)), 200)
		for j := 0; j < 100; j++ {
			state.IDs = append(state.IDs, fmt.Sprint(receiptFrom(t, response, j)["event_id"]))
		}
	}
	if count, _ := queueUsage(t, s); count != 2000 {
		t.Fatal("unexpected accepted count at crash barrier")
	}
	if stage == "projected_before_ack" {
		if _, err := s.db.Exec("CREATE TRIGGER arch_crash_ack BEFORE DELETE ON events BEGIN SELECT RAISE(ABORT,'crash checkpoint'); END"); err != nil {
			t.Fatal(err)
		}
		if err := s.flushDelivery(context.Background()); err == nil {
			t.Fatal("acknowledgment failure not reached")
		}
		state.ProjectedBeforeKill = architectureCount(t, h, state.Project)
		if state.ProjectedBeforeKill != 500 {
			t.Fatalf("unexpected commit boundary: %d", state.ProjectedBeforeKill)
		}
		if count, _ := queueUsage(t, s); count != 2000 {
			t.Fatal("failed acknowledgment removed queued events")
		}
	}
	raw, _ := json.Marshal(state)
	if err := os.WriteFile(filepath.Join(root, "ready.tmp"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(root, "ready.tmp"), filepath.Join(root, "ready.json")); err != nil {
		t.Fatal(err)
	}
	// The parent kills the process without running deferred closes.
	time.Sleep(60 * time.Second)
	t.Fatal("parent did not kill helper")
}
