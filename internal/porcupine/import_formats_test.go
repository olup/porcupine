package porcupine

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
)

func createFormatImport(t *testing.T, s *Service, project, token, format, key string, data []byte) ImportUploadJob {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"format": format, "idempotency_key": key, "files": []ImportUploadFileSpec{{Path: "events.jsonl", Kind: "events", SizeBytes: int64(len(data)), SHA256: importTestSHA(data)}}})
	return importTestJob(t, importTestRequest(t, s, "POST", "/api/v1/projects/"+project+"/imports", token, body, nil, 201))
}
func finishFormatImport(t *testing.T, s *Service, project, token string, job ImportUploadJob, data []byte) ImportUploadJob {
	t.Helper()
	importTestPut(t, s, project, job, job.Files[0], token, job.Files[0].OffsetBytes, data, 200)
	importTestRequest(t, s, "POST", importTestBase(project, job.ID)+"/files/"+job.Files[0].ID+"/complete", token, nil, nil, 200)
	return importTestJob(t, importTestRequest(t, s, "POST", importTestBase(project, job.ID)+"/complete", token, nil, nil, 200))
}
func historicalJourney(t *testing.T, format string) []byte {
	t.Helper()
	raw := contractExample(t, format)
	var messages []json.RawMessage
	if format == "native" {
		if err := json.Unmarshal(raw, &messages); err != nil {
			t.Fatal(err)
		}
	} else {
		var envelope struct {
			Batch []json.RawMessage `json:"batch"`
		}
		if err := json.Unmarshal(raw, &envelope); err != nil {
			t.Fatal(err)
		}
		messages = envelope.Batch
	}
	var output bytes.Buffer
	for _, message := range messages {
		if format == "posthog" {
			var event map[string]json.RawMessage
			var props map[string]json.RawMessage
			json.Unmarshal(message, &event)
			json.Unmarshal(event["properties"], &props)
			event["distinct_id"] = props["distinct_id"]
			message, _ = json.Marshal(event)
		}
		var compact bytes.Buffer
		if err := json.Compact(&compact, message); err != nil {
			t.Fatal(err)
		}
		output.Write(compact.Bytes())
		output.WriteByte('\n')
	}
	return output.Bytes()
}
func TestHistoricalFormatsResumeAndCrossAdapterDedup(t *testing.T) {
	for _, format := range []string{"native", "posthog", "segment"} {
		t.Run(format, func(t *testing.T) {
			config := Config{DBPath: filepath.Join(t.TempDir(), "imports.db"), BootstrapToken: adminToken, QueueMaxEvents: 100}
			s, err := Open(config)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { s.Close() }()
			a, ingest := project(t, s)
			b, _ := project(t, s)
			_, writer := key(t, s, []string{"imports:write", "events:write"}, []string{a})
			bindIngestionSource(t, s, writer, "checkout")
			bindIngestionSource(t, s, ingest, "checkout")
			data := historicalJourney(t, format)
			job := createFormatImport(t, s, a, writer, format, "resumable", data)
			split := bytes.IndexByte(data, '\n') + 12
			first := importTestJob(t, importTestPut(t, s, a, job, job.Files[0], writer, 0, data[:split], 200))
			if first.Counts.Accepted != 1 || first.Format != format {
				t.Fatal("format or first checkpoint lost", first)
			}
			again := importTestJob(t, importTestPut(t, s, a, job, job.Files[0], writer, 0, data[:split], 200))
			if again.Counts != first.Counts {
				t.Fatal("lost ACK replay changed counts")
			}
			importTestRequest(t, s, "GET", importTestBase(b, job.ID), writer, nil, nil, 403)
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s, err = Open(config)
			if err != nil {
				t.Fatal(err)
			}
			resumed := importTestJob(t, importTestRequest(t, s, "GET", importTestBase(a, job.ID), writer, nil, nil, 200))
			if resumed.Files[0].OffsetBytes != int64(split) || resumed.Format != format {
				t.Fatal("restart lost format or offset")
			}
			completed := finishFormatImport(t, s, a, writer, resumed, data[split:])
			if completed.Counts.Accepted != 3 || completed.Counts.Invalid != 0 || completed.Delivery.Pending != 3 || completed.Delivery.Ready {
				t.Fatalf("wrong import result: %+v", completed)
			}
			var timestamp, props string
			var imported bool
			if err := s.db.QueryRow(`SELECT timestamp,properties,is_imported FROM events WHERE project_id=? AND event='order_completed'`, a).Scan(&timestamp, &props, &imported); err != nil {
				t.Fatal(err)
			}
			if timestamp != "2026-01-10T08:02:00Z" || !imported || !strings.Contains(props, "9007199254740993") {
				t.Fatal("occurrence time, marker or numeric precision changed", timestamp, props)
			}
			var target string
			if err := s.db.QueryRow(`SELECT identified_id FROM identity_links WHERE project_id=? AND anonymous_id='browser-7'`, a).Scan(&target); err != nil || target != "user-42" {
				t.Fatal("explicit historical evidence missing", target, err)
			}
			traits, _, _ := storedIdentityProfile(t, s, a, "user-42")
			if traits["plan"] != nil {
				t.Fatal("historical identify replayed incremental traits")
			}
			live := nativeRequest(t, s, ingest, string(contractExample(t, "native")), 200)
			if live["duplicates"] != float64(3) || eventCount(t, s) != 3 {
				t.Fatal("history and live namespace differ", live)
			}
			for _, adapter := range []string{"native", "posthog", "segment"} {
				otherData := historicalJourney(t, adapter)
				otherJob := createFormatImport(t, s, a, writer, adapter, "retry-"+adapter, otherData)
				duplicate := finishFormatImport(t, s, a, writer, otherJob, otherData)
				if duplicate.Counts.Duplicates != 3 || duplicate.Counts.Accepted != 0 {
					t.Fatal("cross-format history retry created events", duplicate.Counts)
				}
			}
			if eventCount(t, s) != 3 {
				t.Fatal("cross-protocol retries duplicated events")
			}
		})
	}
}

func TestHistoricalFormatValidationAndProtectedLiveIdentity(t *testing.T) {
	s := service(t, 50)
	enableTestPipelines(t, s)
	a, ingest := project(t, s)
	b, _ := project(t, s)
	request(t, s, "POST", "/api/v1/projects/"+a+"/pipelines", adminToken, strings.ReplaceAll(pipelineBody("https://hooks.example.com/"), "checkout", "order_completed"), 201)
	nativeRequest(t, s, ingest, `{"type":"identify","message_id":"live-profile","user_id":"user-42","set":{"plan":"new-live"}}`, 200)
	for _, format := range []string{"native", "segment"} {
		data := historicalJourney(t, format)
		job := createFormatImport(t, s, a, adminToken, format, "protect-"+format, data)
		result := finishFormatImport(t, s, a, adminToken, job, data)
		if result.Counts.Unresolved != 1 {
			t.Fatal("live identity must prevent archive ownership", result.Counts)
		}
	}
	traits, _, _ := storedIdentityProfile(t, s, a, "user-42")
	if traits["plan"] != "new-live" {
		t.Fatal("archive overwrote live profile")
	}
	var outbox int
	if err := s.db.QueryRow(`SELECT count(*) FROM webhook_deliveries`).Scan(&outbox); err != nil {
		t.Fatal(err)
	}
	if outbox != 0 {
		t.Fatal("history triggered webhook")
	}
	for _, format := range []string{"native", "segment"} {
		for _, raw := range []string{
			`{"type":"track","name":"bad","event":"bad","timestamp":"2020-01-01T00:00:00Z"}`,
			`{"type":"track","message_id":"x","messageId":"x","name":"bad","event":"bad","timestamp":"invalid"}`,
			`{"type":"track","message_id":"x","messageId":"x","name":"bad","event":"bad","timestamp":"2020-01-01T00:00:00Z","project_id":"forged"}`,
			`{"type":"pageview","message_id":"x","message_id":"changed","timestamp":"2020-01-01T00:00:00Z"}`,
		} {
			if _, err := parseHistoricalImportRow([]byte(raw), "events", format); err == nil {
				t.Fatal("accepted malformed historical row", format, raw)
			}
		}
	}
	actorless := []byte(`{"type":"track","message_id":"actorless","name":"stock_updated","timestamp":"2020-01-01T23:15:00.123456789-02:00","properties":{"quantity":3}}` + "\n")
	job := createFormatImport(t, s, a, adminToken, "native", "actorless", actorless)
	finishFormatImport(t, s, a, adminToken, job, actorless)
	var actor, timestamp string
	if err := s.db.QueryRow(`SELECT distinct_id,timestamp FROM events WHERE project_id=? AND event='stock_updated'`, a).Scan(&actor, &timestamp); err != nil {
		t.Fatal(err)
	}
	if actor != "" || timestamp != "2020-01-02T01:15:00.123456789Z" {
		t.Fatal("actorless event or original fractional time changed", actor, timestamp)
	}
	var foreign int
	s.db.QueryRow(`SELECT count(*) FROM events WHERE project_id=?`, b).Scan(&foreign)
	if foreign != 0 {
		t.Fatal("import crossed project boundary")
	}
}

func TestHistoricalFormatFrozenManifestAndRevocation(t *testing.T) {
	s := service(t, 10)
	a, _ := project(t, s)
	id, token := key(t, s, []string{"imports:write", "events:write"}, []string{a})
	data := historicalJourney(t, "native")
	job := createFormatImport(t, s, a, token, "native", "fixed", data)
	body, _ := json.Marshal(map[string]any{"format": "segment", "idempotency_key": "fixed", "files": []ImportUploadFileSpec{{Path: "events.jsonl", Kind: "events", SizeBytes: int64(len(data)), SHA256: importTestSHA(data)}}})
	importTestRequest(t, s, "POST", "/api/v1/projects/"+a+"/imports", token, body, nil, 409)
	request(t, s, "DELETE", "/api/v1/access-keys/"+id, adminToken, "", 204)
	importTestPut(t, s, a, job, job.Files[0], token, 0, data, 401)
	if eventCount(t, s) != 0 {
		t.Fatal("revoked import key wrote events")
	}
}

func TestImportDeliveryCursorExcludesLaterTraffic(t *testing.T) {
	s := service(t, 20)
	a, ingest := project(t, s)
	data := historicalJourney(t, "native")
	job := createFormatImport(t, s, a, adminToken, "native", "delivery", data)
	completed := finishFormatImport(t, s, a, adminToken, job, data)
	if completed.Delivery == nil || completed.Delivery.Pending != 3 {
		t.Fatal("missing pending prefix")
	}
	nativeRequest(t, s, ingest, `{"type":"track","message_id":"later","name":"later"}`, 200)
	after, err := s.loadImportJob(context.Background(), a, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.Delivery.Pending != 3 {
		t.Fatal("later live traffic delays historical readiness", after.Delivery)
	}
	if _, err = s.db.Exec(`DELETE FROM events WHERE project_id=? AND event!='later'`, a); err != nil {
		t.Fatal(err)
	}
	after, err = s.loadImportJob(context.Background(), a, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.Delivery.Pending != 0 || after.Delivery.Ready {
		t.Fatal("unconfigured analytics must not claim queryable", after.Delivery)
	}
	for _, format := range []string{"native", "segment"} {
		raw, _ := json.Marshal(map[string]any{"format": format, "idempotency_key": "persons-" + format, "files": []ImportUploadFileSpec{{Path: "persons.jsonl", Kind: "persons", SHA256: importTestSHA(nil)}}})
		importTestRequest(t, s, "POST", "/api/v1/projects/"+a+"/imports", adminToken, raw, nil, 400)
	}
}
