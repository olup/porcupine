package porcupine

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"testing"
	"time"
)

func TestSharedQueryWatchdogTimingProtocol(t *testing.T) {
	t.Parallel()
	started := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	for _, sqlMS := range []int{1, 250, 5000, 60000} {
		for _, s3 := range []int{0, 5, 30} {
			for _, metadata := range []int{0, 10, 30} {
				limits := InteractiveQueryConfig{TimeoutMS: sqlMS}
				hard := started.Add(time.Duration(sqlMS) * time.Millisecond)
				killAt := sharedQueryWatchdogDeadline(hard, s3, metadata)
				expireAt := started.Add(sharedQueryPermitLifetime(limits, s3, metadata))
				if killAt.Sub(hard) != time.Duration(s3+metadata)*time.Second+15*time.Second {
					t.Fatal("watchdog omitted an I/O deadline", sqlMS, s3, metadata)
				}
				if expireAt.Sub(killAt) != 15*time.Second || !killAt.Add(sharedQueryWatchdogLogGrace).Before(expireAt) {
					t.Fatal("permit could be recycled before fail-stop", sqlMS, s3, metadata)
				}
				// A permit inserted after admission started expires even later. The known
				// SQL deadline remains fixed instead of restarting after admission finishes.
				for _, admission := range []time.Duration{time.Millisecond, 100 * time.Millisecond, 3 * time.Second} {
					if !killAt.Before(started.Add(admission).Add(sharedQueryPermitLifetime(limits, s3, metadata))) {
						t.Fatal("admission delay broke fencing")
					}
				}
			}
		}
	}
}

type blockedSharedWatchdogLog struct{}

func (blockedSharedWatchdogLog) Write([]byte) (int, error) { select {} }

func TestSharedQueryWatchdogScopeHelper(t *testing.T) {
	t.Parallel()
	dsn, mode := os.Getenv("PORCUPINE_SHARED_WATCHDOG_DSN"), os.Getenv("PORCUPINE_SHARED_WATCHDOG_MODE")
	if dsn == "" || mode == "" {
		t.Skip("watchdog subprocess helper")
	}
	s := sharedQueryTestService(t, dsn, "watchdog-"+mode)
	ctx, cancel := context.WithCancel(context.Background())
	scope, _, release, err := s.beginInteractiveQuery(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	if mode == "hung" || mode == "hung-log-blocked" {
		if mode == "hung-log-blocked" {
			slog.SetDefault(slog.New(slog.NewTextHandler(blockedSharedWatchdogLog{}, nil)))
		}
		// A disconnected handler is insufficient: native work can still be blocked.
		// The actual scope release is deliberately not called in this process.
		cancel()
		<-scope.Done()
		fmt.Println("shared-query-watchdog-ready")
		_, _ = io.ReadAll(os.Stdin)
		t.Fatal("unreturned shared execution survived its fail-stop horizon")
	}
	if mode != "released" {
		t.Fatal("unknown watchdog helper mode")
	}
	release()
	release()
	cancel()
	canceled, stop := context.WithCancel(context.Background())
	scope, _, release, err = s.beginInteractiveQuery(canceled, "a")
	if err != nil {
		t.Fatal(err)
	}
	stop()
	<-scope.Done()
	release()
	release()
	// A stuck default-local installation must not inherit the shared fail-stop
	// policy. It remains alive beyond the same 15-second safety horizon.
	local := service(t, 20)
	local.interactiveConfig.TimeoutMS = 250
	projectID, _ := project(t, local)
	_, _, localRelease, err := local.beginInteractiveQuery(context.Background(), projectID)
	if err != nil {
		t.Fatal(err)
	}
	defer localRelease()
	fmt.Println("shared-query-watchdog-ready")
	_, _ = io.ReadAll(os.Stdin)
}

type sharedWatchdogProcess struct {
	command  *exec.Cmd
	stdin    io.WriteCloser
	stderr   *bytes.Buffer
	finished chan error
	started  time.Time
}

func startSharedWatchdogProcess(t *testing.T, dsn, mode string) *sharedWatchdogProcess {
	t.Helper()
	command := exec.Command(os.Args[0], "-test.run=^TestSharedQueryWatchdogScopeHelper$", "-test.timeout=30s", "-test.v=false")
	command.Env = append(os.Environ(), "PORCUPINE_SHARED_WATCHDOG_DSN="+dsn, "PORCUPINE_SHARED_WATCHDOG_MODE="+mode)
	stdin, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	output, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	process := &sharedWatchdogProcess{command: command, stdin: stdin, stderr: &bytes.Buffer{}, finished: make(chan error, 1), started: time.Now()}
	command.Stderr = process.stderr
	if err = command.Start(); err != nil {
		stdin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stdin.Close(); _ = command.Process.Kill() })
	ready := make(chan bool, 1)
	go func() {
		scanner := bufio.NewScanner(output)
		for scanner.Scan() {
			if scanner.Text() == "shared-query-watchdog-ready" {
				ready <- true
				process.finished <- command.Wait()
				return
			}
		}
		ready <- false
		process.finished <- command.Wait()
	}()
	select {
	case ok := <-ready:
		if !ok {
			<-process.finished
			t.Fatal("watchdog helper failed before query admission", process.stderr.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("watchdog helper did not acquire execution scope")
	}
	return process
}

func TestSharedQueryWatchdogStopsBeforePermitExpiry(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"hung", "hung-log-blocked"} {
		t.Run(mode, func(t *testing.T) {
			dsn, a, b := sharedQueryTestFixture(t)
			if _, err := a.control.Exec(`UPDATE tenants SET hot_query_timeout_ms=250 WHERE id='one'`); err != nil {
				t.Fatal(err)
			}
			process := startSharedWatchdogProcess(t, dsn, mode)
			if _, _, err := b.acquireSharedQueryPermit(context.Background(), "b", "one"); queryStatus(err) != 429 {
				t.Fatal("hung execution lost its permit", err)
			}
			select {
			case err := <-process.finished:
				var exited *exec.ExitError
				if !errors.As(err, &exited) || exited.ExitCode() != 70 {
					t.Fatal("watchdog did not use fail-stop exit 70", err, process.stderr.String())
				}
			case <-time.After(27 * time.Second):
				t.Fatal("watchdog did not terminate stuck query process")
			}
			if time.Since(process.started) < 15*time.Second {
				t.Fatal("watchdog bypassed the real native safety margin")
			}
			if mode == "hung" && !bytes.Contains(process.stderr.Bytes(), []byte("shared analytics execution did not stop before its safety horizon; terminating worker")) {
				t.Fatal("missing constant fail-stop explanation")
			}
			var remainingMS float64
			if err := a.control.QueryRow(`SELECT extract(epoch FROM expires_at-clock_timestamp())*1000 FROM shared_query_permits WHERE project_id='a'`).Scan(&remainingMS); err != nil || remainingMS <= 0 {
				t.Fatal("lease expired before process termination", remainingMS, err)
			}
			if _, _, err := b.acquireSharedQueryPermit(context.Background(), "b", "one"); queryStatus(err) != 429 {
				t.Fatal("terminated worker permit was released before safety expiry", err)
			}
		})
	}
}
func TestSharedQueryWatchdogReleasedAndLocalScopesSurvive(t *testing.T) {
	t.Parallel()
	dsn, a, _ := sharedQueryTestFixture(t)
	if _, err := a.control.Exec(`UPDATE tenants SET hot_query_timeout_ms=250 WHERE id='one'`); err != nil {
		t.Fatal(err)
	}
	process := startSharedWatchdogProcess(t, dsn, "released")
	var count int
	if err := a.control.QueryRow(`SELECT count(*) FROM shared_query_permits`).Scan(&count); err != nil || count != 0 {
		t.Fatal("finished shared query leaked a permit", count, err)
	}
	select {
	case err := <-process.finished:
		t.Fatal("completed/canceled scope or local mode retained a shared watchdog", err, process.stderr.String())
	case <-time.After(17 * time.Second):
	}
	if err := process.stdin.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-process.finished:
		if err != nil {
			t.Fatal("normal helper failed", err, process.stderr.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("normal helper did not finish")
	}
}
