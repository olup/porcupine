package porcupine

import (
	"github.com/olup/porcupine/internal/importer"
	"testing"
	"time"
)

func TestExternalImportsRequireFreshStableAuthority(t *testing.T) {
	t.Parallel()
	f := newExternalAuthFixture(t)
	s := f.s
	p := request(t, s, "POST", "/api/v1/projects", f.operator(t), `{"name":"history"}`, 201)
	id := p["project"].(map[string]any)["id"].(string)
	token := func(subject, scopes string) string {
		return f.token(t, subject, defaultTenantID, scopes, s.resources.apiResource, id)
	}
	original := token("person", "imports:write events:write")
	data := importTestRow("historical-event")
	job := importTestCreate(t, s, id, original, "stable", []importer.ImportUploadFileSpec{{Path: "events.jsonl", Kind: "events", SizeBytes: int64(len(data)), SHA256: importTestSHA(data)}})
	f.now.Store(f.clock.Add(6 * time.Minute).UnixMilli())
	importTestPut(t, s, id, job, job.Files[0], original, 0, data, 401)
	importTestPut(t, s, id, job, job.Files[0], token("other-person", "imports:write events:write"), 0, data, 403)
	importTestPut(t, s, id, job, job.Files[0], token("person", "events:write"), 0, data, 403)
	refreshed := token("person", "imports:write events:write")
	accepted := importTestJob(t, importTestPut(t, s, id, job, job.Files[0], refreshed, 0, data, 200))
	if accepted.Counts.Accepted != 1 || accepted.Files[0].OffsetBytes != int64(len(data)) {
		t.Fatal("fresh authority lost import checkpoint", accepted)
	}
	retry := importTestJob(t, importTestPut(t, s, id, job, job.Files[0], refreshed, 0, data, 200))
	if retry.Counts.Accepted != 1 {
		t.Fatal("retry duplicated accepted import", retry)
	}
	importTestRequest(t, s, "POST", importTestBase(id, job.ID)+"/files/"+job.Files[0].ID+"/complete", refreshed, nil, nil, 200)
	importTestRequest(t, s, "POST", importTestBase(id, job.ID)+"/complete", refreshed, nil, nil, 200)
	batch, err := s.deliveryBatch(t.Context())
	if err != nil || len(batch) != 1 || batch[0].Timestamp != "2020-01-02T03:04:05Z" {
		t.Fatal("expired submitting JWT changed accepted historical intent", batch, err)
	}
}
