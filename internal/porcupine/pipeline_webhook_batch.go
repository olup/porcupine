package porcupine

import "context"

func (s *Service) webhookPipelineRunPage(ctx context.Context, project, pipeline string, limit, offset int) ([]PipelineRun, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id,pipeline_id,state,attempts,created_at_ms,completed_at_ms,error_code,last_status,window_start_ms,window_end_ms,event_count,bytes FROM webhook_deliveries WHERE project_id=$1 AND pipeline_id=$2 ORDER BY created_at_ms DESC,id DESC LIMIT $3 OFFSET $4`, project, pipeline, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []PipelineRun{}
	for rows.Next() {
		var run PipelineRun
		if err = rows.Scan(&run.ID, &run.PipelineID, &run.State, &run.Attempts, &run.CreatedAtMS, &run.CompletedAtMS, &run.Error, &run.LastStatus, &run.WindowStartMS, &run.WindowEndMS, &run.EventCount, &run.Bytes); err != nil {
			return nil, err
		}
		result = append(result, run)
	}
	return result, rows.Err()
}
func (s *Service) getWebhookPipelineRun(ctx context.Context, project, pipeline, id string) (PipelineRun, error) {
	var run PipelineRun
	err := s.db.QueryRowContext(ctx, `SELECT id,pipeline_id,state,attempts,created_at_ms,completed_at_ms,error_code,last_status,window_start_ms,window_end_ms,event_count,bytes FROM webhook_deliveries WHERE project_id=$1 AND pipeline_id=$2 AND id=$3`, project, pipeline, id).Scan(&run.ID, &run.PipelineID, &run.State, &run.Attempts, &run.CreatedAtMS, &run.CompletedAtMS, &run.Error, &run.LastStatus, &run.WindowStartMS, &run.WindowEndMS, &run.EventCount, &run.Bytes)
	return run, err
}
func (s *Service) retryWebhookPipelineRun(ctx context.Context, project, pipeline, id string) (bool, error) {
	retried, err := s.pipelines.webhooks.Retry(ctx, project, pipeline, id)
	if err == nil && retried {
		s.pipelineWake()
	}
	return retried, err
}
