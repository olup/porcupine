package output

import (
	"context"
	"errors"
	"testing"
)

func TestProjectExportCleanupRetainsCheckpointsAndIsolatesDestinations(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s3 := &exportTestS3{}
	s, job := exportWorkerFixture(t, s3)
	other := "other"
	s.db.Exec(`INSERT INTO projects(id,tenant_id,name,created_at) VALUES('other','default','other','now')`)
	otherJob, err := s.Submit(ctx, other, ExportRequest{IdempotencyKey: "other", SQL: "SELECT uuid FROM events"}, fixtureActor())
	if err != nil {
		t.Fatal(err)
	}

	// Checkpoint three cases: an unfinished upload, a completed object whose
	// completion was not recorded in SQLite, and another project's export.
	add := func(jobID, attempt string, complete bool) (key, uploadID string) {
		t.Helper()
		key = jobID + "/" + attempt + "/part-000001.jsonl.gz"
		up, err := s.store.Begin(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		if complete {
			if _, err = up.Write([]byte("fixture")); err != nil {
				t.Fatal(err)
			}
			if err = up.Complete(ctx); err != nil {
				t.Fatal(err)
			}
		}
		if _, err = s.db.Exec(`INSERT INTO export_files(job_id,attempt_id,file_index,object_key,upload_id,storage_id)
		    VALUES(?,?,1,?,?,?)`, jobID, attempt, key, up.ID(), s.store.OwnershipID()); err != nil {
			t.Fatal(err)
		}
		return key, up.ID()
	}
	owned, _ := add(job.ID, "a", true)
	_, incomplete := add(job.ID, "b", false)
	foreign, _ := add(otherJob.ID, "c", true)
	s3.mu.Lock()
	s3.objects["/private-exports/destination/committed-manifest"] = []byte("customer copy")
	s3.mu.Unlock()

	tx, err := s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for range 2 {
		if err = StageExportProjectCleanupTx(ctx, tx, job.ProjectID); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = tx.Exec(`DELETE FROM export_files WHERE job_id=?`, job.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(`DELETE FROM export_jobs WHERE id=?`, job.ID); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}

	runtime := s.store
	s.store = nil
	if err := s.CleanupProject(ctx, job.ProjectID); !errors.Is(err, errExportSink) {
		t.Fatalf("disabled storage erased pending artifacts: %v", err)
	}
	if err := s.CleanupProject(ctx, other); err != nil {
		t.Fatalf("a project with no staged files requires no store: %v", err)
	}
	s.store = runtime
	s3.mu.Lock()
	s3.failDelete = true
	s3.mu.Unlock()
	if err := s.CleanupProject(ctx, job.ProjectID); err == nil {
		t.Fatal("lost object-delete failure")
	}
	var n int
	if err := s.db.QueryRow(`SELECT count(*) FROM project_deletion_files WHERE project_id=?`, job.ProjectID).Scan(&n); err != nil || n != 2 {
		t.Fatalf("lost durable cleanup checkpoints after S3 failure: %d %v", n, err)
	}
	s3.mu.Lock()
	s3.failDelete = false
	s3.mu.Unlock()
	for range 2 {
		if err := s.CleanupProject(ctx, job.ProjectID); err != nil {
			t.Fatal(err)
		}
	}
	var inventory int
	if err := s.control.QueryRow(`SELECT count(*) FROM managed_artifact_ownership WHERE tenant_id='default' AND project_id=$1 AND kind='export' AND storage_id=$2`, job.ProjectID, s.store.OwnershipID()).Scan(&inventory); err != nil || inventory != 2 {
		t.Fatal("physical-purge ownership inventory was lost or duplicated", inventory, err)
	}
	if err := s.control.QueryRow(`SELECT count(*) FROM managed_artifact_ownership WHERE project_id=$1`, other).Scan(&inventory); err != nil || inventory != 0 {
		t.Fatal("cleanup inventoried another project's objects", inventory, err)
	}
	s3.mu.Lock()
	defer s3.mu.Unlock()
	if _, ok := s3.parts[incomplete]; ok {
		t.Fatal("multipart upload left behind")
	}
	if _, ok := s3.objects["/private-exports/isolated/"+owned]; ok {
		t.Fatal("completed export left behind")
	}
	if _, ok := s3.objects["/private-exports/isolated/"+foreign]; !ok {
		t.Fatal("deleted another project's export")
	}
	if _, ok := s3.objects["/private-exports/destination/committed-manifest"]; !ok {
		t.Fatal("deleted downstream destination data")
	}
}
