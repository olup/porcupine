package output

import "context"

const (
	scheduledRunFields = `SELECT r.id,r.pipeline_id,r.state,r.attempts,r.created_at_ms,r.completed_at_ms,r.error_code,
 0,r.window_start_ms,r.window_end_ms,0,r.row_count,r.byte_count`
	sqliteScheduledRunSelect = scheduledRunFields + ` FROM pipeline_s3_runs r JOIN pipelines p ON p.id=r.pipeline_id
 WHERE p.project_id=$1 AND p.id=$2`
	postgresScheduledRunSelect = scheduledRunFields + ` FROM hosted_s3_pipeline_runs r WHERE r.project_id=$1 AND r.pipeline_id=$2`
	sqliteWebhookRunSelect     = `SELECT id,pipeline_id,state,attempts,created_at_ms,completed_at_ms,error_code,last_status,
 window_start_ms,window_end_ms,event_count,0,bytes FROM webhook_deliveries WHERE project_id=$1 AND pipeline_id=$2`
	postgresWebhookRunSelect = `SELECT id,pipeline_id,CASE state WHEN 'pending' THEN 'queued' WHEN 'running' THEN 'sending'
 WHEN 'succeeded' THEN 'delivered' ELSE CASE WHEN error_code IN ('','event_deleted') THEN 'canceled' ELSE 'failed' END END,
 attempts,(extract(epoch from created_at)*1000)::bigint,coalesce((extract(epoch from completed_at)*1000)::bigint,0),
 error_code,last_status,window_start_ms,window_end_ms,refs,0,body_bytes FROM pipeline_effect_jobs
 WHERE project_id=$1 AND pipeline_id=$2 AND kind='delivery'`
)

func (s *PipelineJobs) pipelineRunQuery(p storedPipeline) (query, order, id string) {
	if p.Kind == "s3" {
		query := sqliteScheduledRunSelect
		if s.pg != nil {
			query = postgresScheduledRunSelect
		}
		return query, "r.window_start_ms DESC,r.id", "r.id"
	}
	if s.pg != nil {
		return postgresWebhookRunSelect, "created_at DESC,id", "id"
	}
	return sqliteWebhookRunSelect, "created_at_ms DESC,id DESC", "id"
}

type rowScanner interface{ Scan(...any) error }

func scanPipelineRun(row rowScanner) (PipelineRun, error) {
	var r PipelineRun
	err := row.Scan(&r.ID, &r.PipelineID, &r.State, &r.Attempts, &r.CreatedAtMS, &r.CompletedAtMS, &r.Error, &r.LastStatus, &r.WindowStartMS, &r.WindowEndMS, &r.EventCount, &r.Rows, &r.Bytes)
	return r, err
}

func (s *PipelineJobs) pipelineRunPage(ctx context.Context, p storedPipeline, limit, offset int) ([]PipelineRun, error) {
	query, order, _ := s.pipelineRunQuery(p)
	rows, err := s.db.QueryContext(ctx, query+` ORDER BY `+order+` LIMIT $3 OFFSET $4`, p.ProjectID, p.ID, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	runs := []PipelineRun{}
	for rows.Next() {
		run, err := scanPipelineRun(rows)
		if err != nil {
			return nil, err
		}
		runs = append(runs, run)
	}
	return runs, rows.Err()
}

func (s *PipelineJobs) getPipelineRun(ctx context.Context, p storedPipeline, id string) (PipelineRun, error) {
	query, _, column := s.pipelineRunQuery(p)
	return scanPipelineRun(s.db.QueryRowContext(ctx, query+` AND `+column+`=$3`, p.ProjectID, p.ID, id))
}
