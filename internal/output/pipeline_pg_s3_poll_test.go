package output

import (
	"bytes"
	"context"
	"testing"
	"time"
)

func TestSharedHostedS3PollPreservesPendingWork(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db, _ := pipelineFixture(t)
	if _, err := db.Exec(PipelineJobsPostgresSchemaV1 + ScheduledPipelinePostgresSchemaV1 + `
UPDATE pipeline_effect_definitions SET state='paused' WHERE id='pipeline';
INSERT INTO hosted_pipeline_configs(project_id,id,tenant_id,name,config,secret,created_at_ms,kind)
VALUES ('project','pipeline','tenant','Scheduled export','{}','',0,'s3');
INSERT INTO hosted_s3_pipeline_state(project_id,pipeline_id,cursor_ms,interval_ms)
VALUES ('project','pipeline',0,60000);
INSERT INTO hosted_s3_pipeline_runs(id,project_id,pipeline_id,window_start_ms,window_end_ms,snapshot_catalog_id,snapshot_version,state,attempt_id,next_attempt,created_at_ms,reserved_bytes)
VALUES ('queued','project','pipeline',0,60000,'catalog',0,'queued','',clock_timestamp()+interval '1 hour',0,1),
       ('retired','project','pipeline',-60000,0,'catalog',0,'canceled','',clock_timestamp()+interval '1 hour',0,1);`); err != nil {
		t.Fatal(err)
	}
	jobs, err := newPipelineRuntime(&PipelineConfig{Key: bytes.Repeat([]byte{1}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	jobs.db = db
	var scheduledChecks, retirementChecks int
	scheduledPoll := jobs.hostedPipelinePoll(hostedScheduledPresence, func(context.Context) (bool, error) {
		scheduledChecks++
		return false, nil
	})
	retirementPoll := jobs.hostedPipelinePoll(hostedScheduledRetirementPresence, func(context.Context) (bool, error) {
		retirementChecks++
		return false, nil
	})
	for _, seconds := range []int{1, 2, 4, 8, 10, 10} {
		want := time.Duration(seconds) * time.Second
		if delay, err := scheduledPoll(ctx); delay != want || scheduledChecks != 0 || err != nil {
			t.Fatalf("paused schedule: delay %s, step calls %d, error %v; want %s and no step", delay, scheduledChecks, err, want)
		}
		if delay, err := retirementPoll(ctx); delay != want || retirementChecks != 0 || err != nil {
			t.Fatalf("finished retirement: delay %s, step calls %d, error %v; want %s and no step", delay, retirementChecks, err, want)
		}
	}

	// Work that can become due before the next idle poll keeps the fast cadence.
	if _, err = db.Exec(`UPDATE hosted_s3_pipeline_runs SET attempt_id='uploading' WHERE id='retired'`); err != nil {
		t.Fatal(err)
	}
	if delay, err := retirementPoll(ctx); delay != time.Second || retirementChecks != 1 || err != nil {
		t.Fatalf("pending cleanup with paused definition: delay %s, step calls %d, error %v", delay, retirementChecks, err)
	}
	if _, err = db.Exec(`UPDATE pipeline_effect_definitions SET state='active' WHERE id='pipeline'`); err != nil {
		t.Fatal(err)
	}
	if delay, err := scheduledPoll(ctx); delay != time.Second || scheduledChecks != 1 || err != nil {
		t.Fatalf("active schedule awaiting a queued retry: delay %s, step calls %d, error %v", delay, scheduledChecks, err)
	}
}
