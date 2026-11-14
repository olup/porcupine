package porcupine

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestDuckLakeCLIHistoricalFormatsAndInterruptedResume(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "porcupine")
	build := exec.Command("go", "build", "-o", binary, "./cmd/porcupine")
	build.Dir = "../../cli"
	build.Env = append(os.Environ(), "CGO_ENABLED=0")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build standalone CLI: %s %v", output, err)
	}
	for _, format := range []string{"native", "posthog", "segment"} {
		t.Run(format, func(t *testing.T) {
			s := duckLakeJourneyService(t)
			s.queueMax = 2000
			projectID, _ := project(t, s)
			other, _ := project(t, s)
			_, token := key(t, s, []string{"imports:write", "events:write", "events:read"}, []string{projectID})
			var cut atomic.Bool
			committed := make(chan struct{})
			if format == "native" {
				cut.Store(true)
			}
			handler := s.Handler()
			host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "PUT" && cut.CompareAndSwap(true, false) {
					record := httptest.NewRecorder()
					handler.ServeHTTP(record, r)
					if record.Code != 200 {
						t.Errorf("first upload failed: %d %s", record.Code, record.Body.String())
					}
					close(committed)
					<-r.Context().Done()
					return
				}
				handler.ServeHTTP(w, r)
			}))
			defer host.Close()
			data := historicalJourney(t, format)
			expected := "2"
			if format == "native" {
				var buffer bytes.Buffer
				for i := 0; i < 300; i++ {
					row := map[string]any{"type": "track", "message_id": fmt.Sprintf("bulk-%d", i), "name": "bulk_event", "actor_id": "a", "timestamp": "2020-01-01T23:15:00.123456789-02:00", "properties": map[string]any{"padding": strings.Repeat("x", 1024), "large": json.Number("9007199254740993")}}
					encoded, _ := json.Marshal(row)
					buffer.Write(encoded)
					buffer.WriteByte('\n')
				}
				data = buffer.Bytes()
				expected = "300"
			}
			directory := t.TempDir()
			filename := filepath.Join(directory, "events.jsonl")
			input := filename
			if format == "native" {
				filename += ".gz"
				input = filename
				var compressed bytes.Buffer
				writer := gzip.NewWriter(&compressed)
				if _, err := writer.Write(data); err != nil {
					t.Fatal(err)
				}
				if err := writer.Close(); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filename, compressed.Bytes(), 0600); err != nil {
					t.Fatal(err)
				}
			} else {
				input = directory
				if err := os.WriteFile(filename, data, 0600); err != nil {
					t.Fatal(err)
				}
				if format == "posthog" {
					if err := os.Mkdir(filepath.Join(directory, "persons"), 0700); err != nil {
						t.Fatal(err)
					}
					person := []byte(`{"person_id":"internal-browser","distinct_id":"browser-7","person_version":1,"person_distinct_id_version":1,"properties":{"campaign":"archive"},"created_at":"2020-01-01T00:00:00Z"}` + "\n")
					if err := os.WriteFile(filepath.Join(directory, "persons", "part.jsonl"), person, 0600); err != nil {
						t.Fatal(err)
					}
				}
			}
			command := func(args ...string) *exec.Cmd {
				cmd := exec.Command(binary, append([]string{"--url", host.URL}, args...)...)
				cmd.Env = append(os.Environ(), "PORCUPINE_API_KEY="+token)
				return cmd
			}
			args := []string{"import", "--project", projectID, "--format", format, "--wait-timeout", "30s", input}
			if format == "native" {
				first := command(args...)
				var diagnostic bytes.Buffer
				first.Stderr = &diagnostic
				if err := first.Start(); err != nil {
					t.Fatal(err)
				}
				select {
				case <-committed:
				case <-time.After(30 * time.Second):
					first.Process.Kill()
					first.Wait()
					t.Fatalf("no durable checkpoint: %s", diagnostic.String())
				}
				first.Process.Kill()
				first.Wait()
				var offset, size int64
				if err := s.db.QueryRow(`SELECT offset_bytes,size_bytes FROM import_upload_files`).Scan(&offset, &size); err != nil {
					t.Fatal(err)
				}
				if offset <= 0 || offset >= size {
					t.Fatalf("test did not interrupt a multi-chunk file: %d/%d", offset, size)
				}
			}
			deliveryCtx, cancelDelivery := context.WithCancel(t.Context())
			done := make(chan error, 1)
			go func() {
				ticker := time.NewTicker(20 * time.Millisecond)
				defer ticker.Stop()
				for {
					select {
					case <-deliveryCtx.Done():
						done <- nil
						return
					case <-ticker.C:
						if err := s.flushDelivery(t.Context()); err != nil {
							done <- err
							return
						}
					}
				}
			}()
			cmd := command(args...)
			var output, diagnostic bytes.Buffer
			cmd.Stdout = &output
			cmd.Stderr = &diagnostic
			err := cmd.Run()
			cancelDelivery()
			deliveryErr := <-done
			if err != nil {
				t.Fatalf("CLI import failed: %v %s", err, diagnostic.String())
			}
			if deliveryErr != nil && deliveryErr != context.Canceled {
				t.Fatal(deliveryErr)
			}
			var job ImportUploadJob
			if err = json.Unmarshal(output.Bytes(), &job); err != nil {
				t.Fatal(err)
			}
			if job.State != "completed" || job.Counts.Invalid != 0 || job.Delivery == nil || !job.Delivery.Ready || job.Delivery.Pending != 0 {
				t.Fatalf("import did not become queryable: %s", output.String())
			}
			if !strings.Contains(diagnostic.String(), "queryable") {
				t.Fatal("CLI omitted analytical completion")
			}
			rows := duckLakeJourneyQuery(t, s, projectID, token, "SELECT count(*) AS total FROM events", nil)
			if rows[0].(map[string]any)["total"] != expected {
				t.Fatalf("SQL count differs from accepted history: %v", rows)
			}
			if format == "native" {
				rows = duckLakeJourneyQuery(t, s, projectID, token, "SELECT min(timestamp) AS first FROM events", nil)
				if rows[0].(map[string]any)["first"] != "2020-01-02T01:15:00.123456789Z" {
					t.Fatal("historical occurrence precision changed", rows)
				}
			}
			_, reader := key(t, s, []string{"events:read"}, []string{other})
			if rows := duckLakeJourneyQuery(t, s, other, reader, "SELECT count(*) AS total FROM events", nil); rows[0].(map[string]any)["total"] != "0" {
				t.Fatal("CLI import crossed projects")
			}
			again := command("import", "--project", projectID, "--format", format, "--resume", job.ID, "--wait-timeout", "10s", input)
			var repeated bytes.Buffer
			again.Stdout = &repeated
			if err = again.Run(); err != nil {
				t.Fatal("explicit resume of completed job failed", err)
			}
			var replayed ImportUploadJob
			json.Unmarshal(repeated.Bytes(), &replayed)
			if replayed.ID != job.ID || replayed.Counts != job.Counts {
				t.Fatal("completed-job resume changed counts")
			}
			live := duckLakeJourneyQuery(t, s, projectID, token, "SELECT count(*) AS total FROM events", nil)
			if live[0].(map[string]any)["total"] != expected {
				t.Fatal("CLI resume duplicated analytics", live)
			}
		})
	}
}
