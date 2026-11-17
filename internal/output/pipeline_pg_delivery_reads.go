package output

import (
	"context"
	"database/sql"
	"errors"
)

const pipelineRunFields = `id,pipeline_id,CASE state WHEN 'pending' THEN 'queued' WHEN 'running' THEN 'sending'
 WHEN 'succeeded' THEN 'delivered' ELSE CASE WHEN error_code IN ('','event_deleted') THEN 'canceled' ELSE 'failed' END END,
 attempts,(extract(epoch from created_at)*1000)::bigint,coalesce((extract(epoch from completed_at)*1000)::bigint,0),
 error_code,last_status,refs,body_bytes,window_start_ms,window_end_ms`

type pipelineRunScanner interface{ Scan(...any) error }

func readPGWebhookRun(row pipelineRunScanner) (PipelineRun, error) {
	var r PipelineRun
	err := row.Scan(&r.ID, &r.PipelineID, &r.State, &r.Attempts, &r.CreatedAtMS, &r.CompletedAtMS, &r.Error, &r.LastStatus, &r.EventCount, &r.Bytes, &r.WindowStartMS, &r.WindowEndMS)
	return r, err
}

func (s *PipelineJobs) pgWebhookRunPage(ctx context.Context, project, pipeline string, limit, offset int) ([]PipelineRun, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+pipelineRunFields+` FROM pipeline_effect_jobs
 WHERE project_id=$1 AND pipeline_id=$2 AND kind='delivery' ORDER BY created_at DESC,id LIMIT $3 OFFSET $4`, project, pipeline, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	runs := []PipelineRun{}
	for rows.Next() {
		r, err := readPGWebhookRun(rows)
		if err != nil {
			return nil, err
		}
		runs = append(runs, r)
	}
	return runs, rows.Err()
}

func (s *PipelineJobs) pgWebhookRun(ctx context.Context, project, pipeline, id string) (PipelineRun, error) {
	return readPGWebhookRun(s.db.QueryRowContext(ctx, `SELECT `+pipelineRunFields+` FROM pipeline_effect_jobs
 WHERE project_id=$1 AND pipeline_id=$2 AND id=$3 AND kind='delivery'`, project, pipeline, id))
}

func (s *PipelineJobs) pgRetryWebhookRun(ctx context.Context, project, pipeline, id string) (bool, error) {
	var changed bool
	err := s.pg.transaction(ctx, project, func(ctx context.Context, tx *sql.Tx) error {
		var retry bool
		err := tx.QueryRowContext(ctx, `WITH prior AS (
 SELECT j.source_id,j.project_id,j.catalog_id,j.stream_id,j.id,j.version FROM pipeline_effect_jobs j
 JOIN pipeline_effect_definitions p ON (p.project_id,p.tenant_id,p.id,p.incarnation,p.destination_revision,p.creator_id)=
 (j.project_id,j.tenant_id,j.pipeline_id,j.incarnation,j.destination_revision,j.creator_id)
 WHERE j.project_id=$1 AND j.pipeline_id=$2 AND j.id=$3 AND j.kind='delivery' AND p.state!='retired'
 ), retried AS (
 UPDATE pipeline_effect_jobs j SET state='pending',attempt='',attempts=0,next_attempt_at=clock_timestamp(),
 completed_at=NULL,error_code='',version=j.version+1 FROM prior p
 WHERE (j.source_id,j.project_id,j.catalog_id,j.stream_id,j.id,j.version)=(p.source_id,p.project_id,p.catalog_id,p.stream_id,p.id,p.version)
 AND j.state='retired' AND j.error_code IN ('request_failed','invalid_response','http_rejected','attempts_exhausted')
 RETURNING 1)
 SELECT EXISTS(SELECT 1 FROM retried) FROM prior`, project, pipeline, id).Scan(&retry)
		if errors.Is(err, sql.ErrNoRows) {
			_, err = readPGWebhookRun(tx.QueryRowContext(ctx, `SELECT `+pipelineRunFields+` FROM pipeline_effect_jobs WHERE project_id=$1 AND pipeline_id=$2 AND id=$3 AND kind='delivery'`, project, pipeline, id))
		}
		changed = retry
		return err
	})
	return changed, err
}
