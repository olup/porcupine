package output

import "context"

func (s *PipelineJobs) CleanupProject(ctx context.Context, project string) error {
	for {
		worked, err := s.cleanupRetired(ctx, project)
		if err != nil {
			return err
		}
		if !worked {
			break
		}
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id FROM pipelines WHERE project_id=$1 AND kind='s3' ORDER BY id`, project)
	if err != nil {
		return err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	// Activity has drained. A crashed attempt can still be recorded as running.
	if _, err := s.db.ExecContext(ctx, `UPDATE pipeline_s3_runs SET state='queued',error_code='interrupted' WHERE state='running' AND pipeline_id IN (SELECT id FROM pipelines WHERE project_id=$1)`, project); err != nil {
		return err
	}
	for _, id := range ids {
		if err := s.deleteS3Pipeline(ctx, project, id); err != nil {
			return err
		}
	}
	return nil
}
