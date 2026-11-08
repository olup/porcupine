package porcupine

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/exportstore"
)

func TestClickHouseIntegrationExports(t *testing.T) {
	if os.Getenv("PORCUPINE_TEST_S3_ENDPOINT") == "" {
		t.Skip("run make integration-s3 for real export objects")
	}
	config := integrationClickHouseConfig(t)
	s, err := Open(Config{DBPath: filepath.Join(t.TempDir(), "exports.db"), BootstrapToken: adminToken, QueueMaxEvents: 4000, ClickHouse: &config,
		Exports: &ExportConfig{S3: exportstore.Config{Region: os.Getenv("PORCUPINE_TEST_S3_REGION"), Bucket: os.Getenv("PORCUPINE_TEST_S3_BUCKET"), Prefix: "exports/", Endpoint: os.Getenv("PORCUPINE_TEST_S3_ENDPOINT"), AccessKeyID: os.Getenv("PORCUPINE_TEST_S3_ACCESS_KEY_ID"), SecretAccessKey: os.Getenv("PORCUPINE_TEST_S3_SECRET_ACCESS_KEY"), SessionToken: os.Getenv("PORCUPINE_TEST_S3_SESSION_TOKEN"), UsePathStyle: true}, FileBytes: 64 << 10, BandwidthBytesPerSecond: 1 << 20, MaxExecutionSeconds: 60}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	client := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{DisableCompression: true}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	defer client.CloseIdleConnections()
	call := func(method, path, token string, body any, status int, output any) int {
		t.Helper()
		var encoded []byte
		if body != nil {
			encoded, err = json.Marshal(body)
			if err != nil {
				t.Fatal("encode integration request")
			}
		}
		r, err := http.NewRequest(method, host.URL+path, bytes.NewReader(encoded))
		if err != nil {
			t.Fatal("create integration request")
		}
		if token != "" {
			r.Header.Set("Authorization", "Bearer "+token)
		}
		r.Header.Set("Content-Type", "application/json")
		response, err := client.Do(r)
		if err != nil {
			t.Fatal("export API request failed")
		}
		defer response.Body.Close()
		if status != 0 && response.StatusCode != status {
			t.Fatalf("%s export API status %d, want %d", method, response.StatusCode, status)
		}
		if output != nil && json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(output) != nil {
			t.Fatal("invalid export API JSON")
		}
		return response.StatusCode
	}
	a, ingest := project(t, s)
	b, ingestB := project(t, s)
	_, reader := key(t, s, []string{"events:read"}, []string{a})
	_, manager := key(t, s, []string{"events:read", "events:edit", "events:delete"}, []string{a})
	_, other := key(t, s, []string{"events:read"}, []string{b})
	base := "/api/v1/projects/" + a + "/exports"
	var metadata ExportList
	call("GET", base, reader, nil, 200, &metadata)
	if !metadata.Enabled || metadata.Limits == nil || metadata.Limits.MaxConcurrentJobs != 1 || metadata.Limits.FileBytes != 64<<10 || metadata.Limits.BandwidthBytesPerSecond != 1<<20 {
		t.Fatal("exports must advertise enabled limits and a single worker")
	}

	const rows = 1105
	payloads := make([]string, rows)
	eventName := func(i int) string {
		if i%2 == 0 {
			return "export,東京\nrow"
		}
		return "other"
	}
	for start := 0; start < rows; start += 100 {
		batch := []map[string]any{}
		for i := start; i < min(start+100, rows); i++ {
			var text strings.Builder
			text.WriteString("café,\"東京\"\n")
			for block := range 32 {
				sum := sha256.Sum256([]byte(fmt.Sprintf("%d-%d", i, block)))
				text.WriteString(hex.EncodeToString(sum[:]))
			}
			payloads[i] = text.String()
			batch = append(batch, map[string]any{"uuid": fmt.Sprintf("row-%06d", i), "event": eventName(i), "distinct_id": fmt.Sprintf("user-%d", i%7), "timestamp": "2026-01-01T12:00:00.123Z", "properties": map[string]any{"payload": payloads[i]}})
		}
		call("POST", "/batch/", "", map[string]any{"api_key": ingest, "batch": batch}, 200, nil)
	}
	call("POST", "/capture/", "", map[string]any{"api_key": ingestB, "uuid": "private-other-project", "event": "secret-other-project", "distinct_id": "other-private-user", "properties": map[string]string{"payload": "private-payload"}}, 200, nil)
	exportStart := time.Now()
	deadline := exportStart.Add(90 * time.Second)
	for {
		var pending int
		if err := s.db.QueryRow(`SELECT count(*) FROM events`).Scan(&pending); err != nil {
			t.Fatal(err)
		}
		if pending == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("export seed delivery did not drain")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if config.StoragePolicy != "" {
		// Native S3 cold parts must remain queryable by the separate export reader.
		// Freeze merges so the enumerated part names remain valid during moves.
		if _, err := s.ch.admin(context.Background(), "SYSTEM STOP MERGES "+config.Database+"."+eventTable); err != nil {
			t.Fatal("pause merges for cold move")
		}
		defer func() {
			if _, err := s.ch.admin(context.Background(), "SYSTEM START MERGES "+config.Database+"."+eventTable); err != nil {
				t.Error("resume merges after cold export")
			}
		}()
		raw, err := s.ch.admin(context.Background(), "SELECT name FROM system.parts WHERE database='"+config.Database+"' AND table='"+eventTable+"' AND active AND disk_name='default' FORMAT JSONEachRow")
		if err != nil {
			t.Fatal("read parts before cold move")
		}
		decoder := json.NewDecoder(bytes.NewReader(raw))
		for decoder.More() {
			var part struct {
				Name string `json:"name"`
			}
			if decoder.Decode(&part) != nil || !mcpPathValue.MatchString(part.Name) {
				t.Fatal("invalid cold part name")
			}
			if _, err := s.ch.admin(context.Background(), "ALTER TABLE "+config.Database+"."+eventTable+" MOVE PART '"+part.Name+"' TO VOLUME 'cold'"); err != nil {
				t.Fatal("move export seed to cold S3 storage")
			}
		}
	}

	waitJob := func(id string) ExportJob {
		t.Helper()
		for {
			var job ExportJob
			call("GET", base+"/"+id, reader, nil, 200, &job)
			switch job.State {
			case ExportSucceeded:
				t.Logf("completed %s export: rows=%d files=%d attempts=%d elapsed=%s", job.Format, job.Rows, job.FileCount, job.Attempts, time.Since(exportStart).Round(time.Millisecond))
				return job
			case ExportFailed, ExportCanceled, ExportExpired:
				t.Fatalf("export terminated with state %s and error %q", job.State, job.Error)
			default:
				status := call("GET", base+"/"+id+"/files", reader, nil, 0, nil)
				if status != 409 && status != 200 {
					t.Fatal("nonterminal export returned unexpected file status")
				}
				if status == 200 {
					var completed ExportJob
					call("GET", base+"/"+id, reader, nil, 200, &completed)
					if completed.State != ExportSucceeded {
						t.Fatal("artifact URL available before success")
					}
				}
			}
			if time.Now().After(deadline) {
				t.Fatalf("export exceeded integration budget: state=%s rows=%d files=%d attempts=%d", job.State, job.Rows, job.FileCount, job.Attempts)
			}
			time.Sleep(25 * time.Millisecond)
		}
	}
	create := func(sql, format string) ExportJob {
		t.Helper()
		var job ExportJob
		call("POST", base, reader, ExportCreateBody{IdempotencyKey: newID(), SQL: sql, Format: format}, 202, &job)
		return waitJob(job.ID)
	}
	files := func(job ExportJob) []ExportFile {
		t.Helper()
		result := []ExportFile{}
		offset := 0
		for {
			var page ExportFiles
			call("GET", base+"/"+job.ID+"/files?limit=2&offset="+strconv.Itoa(offset), reader, nil, 200, &page)
			if len(page.Files) > 2 {
				t.Fatal("unbounded export file metadata page")
			}
			result = append(result, page.Files...)
			if page.NextOffset == nil {
				break
			}
			if *page.NextOffset <= offset {
				t.Fatal("export files pagination did not advance")
			}
			offset = *page.NextOffset
		}
		return result
	}
	download := func(file ExportFile) []byte {
		t.Helper()
		if file.URL == "" || file.SHA256 == "" {
			t.Fatal("successful export lacks artifact URL or SHA256")
		}
		response, err := client.Get(file.URL)
		if err != nil {
			t.Fatal("signed export download failed")
		}
		compressed, readErr := io.ReadAll(io.LimitReader(response.Body, 4<<20))
		response.Body.Close()
		if response.Header.Get("Content-Type") != "application/gzip" || response.Header.Get("Content-Encoding") != "" {
			t.Fatal("incorrect stored gzip export metadata")
		}
		if response.StatusCode != 200 || readErr != nil || int64(len(compressed)) != file.Bytes {
			t.Fatal("export download status or byte count mismatch")
		}
		sum := sha256.Sum256(compressed)
		if hex.EncodeToString(sum[:]) != file.SHA256 {
			t.Fatal("export SHA256 mismatch")
		}
		gz, err := gzip.NewReader(bytes.NewReader(compressed))
		if err != nil {
			t.Fatal("export artifact is not gzip")
		}
		decoded, err := io.ReadAll(io.LimitReader(gz, 4<<20))
		closeErr := gz.Close()
		if err != nil || closeErr != nil || int64(len(decoded)) != file.DecodedBytes {
			t.Fatal("gzip checksum or decoded byte count mismatch")
		}
		return decoded
	}
	sql := "SELECT uuid,event,distinct_id,JSONExtractString(properties,'payload') AS payload,toUInt64('18446744073709551615') AS precise FROM events ORDER BY uuid"
	call("POST", "/api/v1/projects/"+a+"/query", reader, map[string]string{"sql": sql}, 429, nil)
	columns := []string{"uuid", "event", "distinct_id", "payload", "precise"}
	var firstJob ExportJob
	for _, format := range []string{"jsonl", "csv"} {
		job := create(sql, format)
		firstJob = job
		if job.Rows != rows || job.DecodedBytes <= 1<<20 || job.Bytes <= 1<<20 || job.FileCount <= 1 {
			t.Fatal("export did not exceed interactive rows/bytes or rotate files")
		}
		actualColumns := []string{}
		for _, column := range job.Columns {
			actualColumns = append(actualColumns, column.Name)
		}
		if !slices.Equal(actualColumns, columns) {
			t.Fatal("export column metadata mismatch")
		}
		seen := 0
		var byteCount, decodedCount int64
		for fileIndex, file := range files(job) {
			if file.Index != fileIndex+1 {
				t.Fatal("export files not ordered")
			}
			decoded := download(file)
			byteCount += file.Bytes
			decodedCount += file.DecodedBytes
			var records [][]string
			if format == "csv" {
				parsed, err := csv.NewReader(bytes.NewReader(decoded)).ReadAll()
				if err != nil || len(parsed) == 0 || !slices.Equal(parsed[0], columns) {
					t.Fatal("CSV per-file header or encoding mismatch")
				}
				records = parsed[1:]
			} else {
				decoder := json.NewDecoder(bytes.NewReader(decoded))
				for decoder.More() {
					var row map[string]string
					if decoder.Decode(&row) != nil {
						t.Fatal("JSONL row values lost string or integer precision")
					}
					values := []string{}
					for _, name := range columns {
						values = append(values, row[name])
					}
					records = append(records, values)
				}
			}
			if int64(len(records)) != file.Rows {
				t.Fatal("file row metadata mismatch")
			}
			for _, row := range records {
				if seen >= rows || len(row) != 5 || row[0] != fmt.Sprintf("row-%06d", seen) || row[1] != eventName(seen) || row[2] != fmt.Sprintf("user-%d", seen%7) || row[3] != payloads[seen] || row[4] != "18446744073709551615" {
					t.Fatal("export row changed, duplicated, leaked another project, or lost UInt64 precision")
				}
				seen++
			}
		}
		if seen != rows || byteCount != job.Bytes || decodedCount != job.DecodedBytes {
			t.Fatal("export manifest totals mismatch")
		}
	}
	// The exporter sends one SELECT, preserving global aggregate, DISTINCT, JOIN and LIMIT semantics.
	for _, check := range []struct {
		sql  string
		want []map[string]string
	}{
		{"SELECT count() AS total FROM events", []map[string]string{{"total": strconv.Itoa(rows)}}},
		{"SELECT DISTINCT distinct_id FROM events ORDER BY distinct_id LIMIT 3", []map[string]string{{"distinct_id": "user-0"}, {"distinct_id": "user-1"}, {"distinct_id": "user-2"}}},
		{"SELECT a.uuid AS uuid,b.distinct_id AS distinct_id FROM events a INNER JOIN events b ON a.uuid=b.uuid ORDER BY a.uuid LIMIT 2", []map[string]string{{"uuid": "row-000000", "distinct_id": "user-0"}, {"uuid": "row-000001", "distinct_id": "user-1"}}},
	} {
		job := create(check.sql, "jsonl")
		got := []map[string]string{}
		for _, file := range files(job) {
			decoder := json.NewDecoder(bytes.NewReader(download(file)))
			for decoder.More() {
				var row map[string]string
				if decoder.Decode(&row) != nil {
					t.Fatal("invalid SQL export JSONL")
				}
				got = append(got, row)
			}
		}
		actual, _ := json.Marshal(got)
		expected, _ := json.Marshal(check.want)
		if !bytes.Equal(actual, expected) {
			t.Fatal("global SQL export semantics changed")
		}
	}
	call("GET", "/api/v1/projects/"+b+"/exports", reader, nil, 403, nil)
	call("GET", base+"/"+firstJob.ID+"/files", other, nil, 403, nil)
	call("POST", "/api/v1/projects/"+b+"/exports", reader, ExportCreateBody{IdempotencyKey: "cross-project", SQL: sql, Format: "jsonl"}, 403, nil)
	mcpCtx, session := mcpTestSession(t, host.URL, reader, "")
	mcpTestCall(t, mcpCtx, session, mcpCallInput{OperationID: "create-event-export", Path: map[string]string{"id": b}, Body: map[string]any{"idempotency_key": "mcp-cross-project", "sql": sql}}, 403)
	mcpTestCall(t, mcpCtx, session, mcpCallInput{OperationID: "list-event-export-files", Path: map[string]string{"id": b, "export_id": firstJob.ID}}, 403)

	createdMCP := mcpTestCall(t, mcpCtx, session, mcpCallInput{OperationID: "create-event-export", Path: map[string]string{"id": a}, Body: map[string]any{"idempotency_key": "mcp-typed-export", "sql": "SELECT count() AS total FROM events WHERE event={kind:String}", "parameters": map[string]string{"kind": "other"}, "format": "jsonl"}}, 202)
	mcpJob := waitJob(createdMCP["id"].(string))
	mcpFiles := mcpTestCall(t, mcpCtx, session, mcpCallInput{OperationID: "list-event-export-files", Path: map[string]string{"id": a, "export_id": mcpJob.ID}, Query: map[string]string{"limit": "1"}}, 200)
	if len(mcpFiles["files"].([]any)) != 1 {
		t.Fatal("MCP export did not expose successful artifact")
	}
	mcpArtifacts := files(mcpJob)
	var parameterized map[string]string
	if len(mcpArtifacts) != 1 || json.Unmarshal(bytes.TrimSpace(download(mcpArtifacts[0])), &parameterized) != nil || parameterized["total"] != strconv.Itoa(rows/2) {
		t.Fatal("MCP export lost typed SQL parameter semantics")
	}

	// Accepted mutations must cross the job's delivery barrier before export execution.
	call("PATCH", "/api/v1/projects/"+a+"/events/row-000000", manager, map[string]any{"expected_revision": "0", "idempotency_key": "export-correction", "snapshot": map[string]any{"event": "corrected-export", "distinct_id": "changed-user", "properties": map[string]string{"payload": "corrected"}, "timestamp": "2026-01-01T12:00:00Z"}}, 202, nil)
	call("DELETE", "/api/v1/projects/"+a+"/events/row-000001", manager, map[string]any{"expected_revision": "0", "idempotency_key": "export-tombstone"}, 202, nil)
	job := create("SELECT uuid,event,distinct_id FROM events WHERE uuid IN ('row-000000','row-000001') ORDER BY uuid", "jsonl")
	if job.Rows != 1 {
		t.Fatal("export resurrected tombstone or missed correction")
	}
	artifact := files(job)
	if len(artifact) != 1 {
		t.Fatal("small corrected export has wrong file count")
	}
	var corrected map[string]string
	if json.Unmarshal(bytes.TrimSpace(download(artifact[0])), &corrected) != nil || corrected["uuid"] != "row-000000" || corrected["event"] != "corrected-export" || corrected["distinct_id"] != "changed-user" {
		t.Fatal("export did not include latest corrected logical row")
	}
}
