package porcupine

import (
	"context"
	"errors"
	"testing"
)

func TestProjectExportCleanupRetainsCheckpointsAndIsolatesDestinations(t *testing.T) {
	ctx := context.Background()
	s3 := &exportTestS3{}
	s, job := exportWorkerFixture(t, s3)
	other, _ := project(t, s)
	otherJob, err := s.createEventExport(exportContext(t, s, adminToken), exportInput(other, "other"))
	if err != nil {
		t.Fatal(err)
	}

	// Checkpoint three cases: an unfinished upload, a completed object whose
	// completion was not recorded in SQLite, and another project's export.
	add := func(jobID, attempt string, complete bool) (key, uploadID string) {
		t.Helper()
		key = jobID + "/" + attempt + "/part-000001.jsonl.gz"
		up, err := s.exports.store.Begin(ctx, key)
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
		if _, err = s.db.Exec(`INSERT INTO export_files(job_id,attempt_id,file_index,object_key,upload_id)
		    VALUES(?,?,1,?,?)`, jobID, attempt, key, up.ID()); err != nil {
			t.Fatal(err)
		}
		return key, up.ID()
	}
	owned, _ := add(job.ID, "a", true)
	_, incomplete := add(job.ID, "b", false)
	foreign, _ := add(otherJob.Body.ID, "c", true)
	s3.mu.Lock()
	s3.objects["/private-exports/destination/committed-manifest"] = []byte("customer copy")
	s3.mu.Unlock()

	tx, err := s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for range 2 {
		if err = s.stageProjectExportCleanup(ctx, tx, job.ProjectID); err != nil {
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

	runtime := s.exports
	s.exports = nil
	if err := s.purgeProjectExports(ctx, job.ProjectID); !errors.Is(err, errExportSink) {
		t.Fatalf("disabled storage erased pending artifacts: %v", err)
	}
	if err := s.purgeProjectExports(ctx, other); err != nil {
		t.Fatalf("a project with no staged files requires no store: %v", err)
	}
	s.exports = runtime
	s3.mu.Lock()
	s3.failDelete = true
	s3.mu.Unlock()
	if err := s.purgeProjectExports(ctx, job.ProjectID); err == nil {
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
		if err := s.purgeProjectExports(ctx, job.ProjectID); err != nil {
			t.Fatal(err)
		}
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
