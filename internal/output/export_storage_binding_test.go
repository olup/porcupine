package output

import (
	"net/http/httptest"
	"testing"

	"github.com/olup/porcupine/internal/exportstore"
	lakeapi "github.com/olup/porcupine/internal/lake"
)

func TestExportCleanupRequiresCreationTimeStorageBinding(t *testing.T) {
	t.Parallel()
	oldS3, otherS3 := &exportTestS3{}, &exportTestS3{}
	s, job := exportWorkerFixture(t, oldS3)
	ctx := t.Context()
	oldStore := s.store
	sink := newExportSink(ctx, s, job)
	if err := sink.setColumns([]lakeapi.AnalyticsResultColumn{{Name: "event", Type: "VARCHAR"}}); err != nil {
		t.Fatal(err)
	}
	if err := sink.beginFile(); err != nil {
		t.Fatal(err)
	}
	var storage, key, upload string
	if err := s.db.QueryRow(`SELECT storage_id,object_key,upload_id FROM export_files WHERE job_id=$1`, job.ID).Scan(&storage, &key, &upload); err != nil || storage != oldStore.OwnershipID() {
		t.Fatal("upload checkpoint was not bound before upload", storage, err)
	}
	server := httptest.NewServer(otherS3)
	defer server.Close()
	otherStore, err := exportstore.New(ctx, exportstore.Config{Region: "us-east-1", Bucket: "other-private-exports", Prefix: "other", Endpoint: server.URL, AccessKeyID: "new-key", SecretAccessKey: "new-secret", UsePathStyle: true})
	if err != nil {
		t.Fatal(err)
	}
	defer otherStore.Close()
	s.store = otherStore
	if err := s.cleanupExport(ctx, job); err == nil {
		t.Fatal("changed namespace erased an export's last ownership reference")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := StageExportProjectCleanupTx(ctx, tx, job.ProjectID); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if _, err := tx.Exec(`DELETE FROM export_files WHERE job_id=$1`, job.ID); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := s.CleanupProject(ctx, job.ProjectID); err == nil {
		t.Fatal("project cleanup accepted the wrong namespace")
	}
	var retained string
	if err := s.db.QueryRow(`SELECT storage_id FROM project_deletion_files WHERE project_id=$1`, job.ProjectID).Scan(&retained); err != nil || retained != storage {
		t.Fatal("staging discarded the original binding", retained, err)
	}
	otherS3.mu.Lock()
	if len(otherS3.parts) != 0 || len(otherS3.objects) != 0 || otherS3.aborts != 0 || otherS3.deletes != 0 {
		t.Fatal("wrong namespace was touched")
	}
	otherS3.mu.Unlock()
	s.store = oldStore
	if _, err := s.db.Exec(`UPDATE project_deletion_files SET storage_id='' WHERE project_id=$1`, job.ProjectID); err != nil {
		t.Fatal(err)
	}
	if err := s.CleanupProject(ctx, job.ProjectID); err == nil {
		t.Fatal("legacy unknown provenance was silently bound to current storage")
	}
	if _, err := s.db.Exec(`UPDATE project_deletion_files SET storage_id=$1 WHERE project_id=$2`, storage, job.ProjectID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.control.Exec(`CREATE TRIGGER fail_inventory BEFORE INSERT ON managed_artifact_ownership BEGIN SELECT RAISE(ABORT,'fixture unavailable'); END`); err != nil {
		t.Fatal(err)
	}
	if err := s.CleanupProject(ctx, job.ProjectID); err == nil {
		t.Fatal("cleanup discarded its checkpoint without durable inventory")
	}
	oldS3.mu.Lock()
	_, exists := oldS3.parts[upload]
	oldS3.mu.Unlock()
	if !exists {
		t.Fatal("cleanup aborted an upload before ownership persisted")
	}
	if _, err := s.control.Exec(`DROP TRIGGER fail_inventory`); err != nil {
		t.Fatal(err)
	}
	if err := s.CleanupProject(ctx, job.ProjectID); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := s.control.QueryRow(`SELECT count(*) FROM managed_artifact_ownership WHERE project_id=$1 AND storage_id=$2 AND object_key=$3 AND upload_id=$4`, job.ProjectID, storage, key, upload).Scan(&count); err != nil || count != 1 {
		t.Fatal("physical-purge ownership was lost", count, err)
	}
}
