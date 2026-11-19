package output

import (
	"context"
	"database/sql"
	"errors"
)

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
			_, err = scanPipelineRun(tx.QueryRowContext(ctx, postgresWebhookRunSelect+` AND id=$3`, project, pipeline, id))
		}
		changed = retry
		return err
	})
	return changed, err
}
