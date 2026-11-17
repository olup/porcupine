package porcupine

import (
	"context"
	"encoding/json"
	"github.com/olup/porcupine/internal/output"
)

type storedExport struct {
	output.ExportJob
	ActorID             string
	SQL                 string
	Parameters          map[string]string
	AttemptID           string
	EventWatermark      int64
	MutationWatermark   int64
	CancelRequested     bool
	ExpiresAtMS         int64
	CleanupPending      bool
	ExternalTenantID    string
	ExternalIssuer      string
	ExternalExpiresAtMS int64
}

func (s *Service) loadExport(ctx context.Context, project, id string) (storedExport, error) {
	var job storedExport
	var params string
	public, err := s.exports.Get(ctx, project, id)
	if err != nil {
		return job, err
	}
	job.ExportJob = public
	err = s.db.QueryRowContext(ctx, `SELECT actor_id,sql,parameters,attempt_id,event_watermark,mutation_watermark,cancel_requested,expires_at_ms,cleanup_pending,coalesce((SELECT tenant_id FROM export_external_authority a WHERE a.job_id=j.id),''),coalesce((SELECT issuer FROM export_external_authority a WHERE a.job_id=j.id),''),coalesce((SELECT expires_at_ms FROM export_external_authority a WHERE a.job_id=j.id),0) FROM export_jobs j WHERE project_id=? AND id=?`, project, id).Scan(&job.ActorID, &job.SQL, &params, &job.AttemptID, &job.EventWatermark, &job.MutationWatermark, &job.CancelRequested, &job.ExpiresAtMS, &job.CleanupPending, &job.ExternalTenantID, &job.ExternalIssuer, &job.ExternalExpiresAtMS)
	if err == nil {
		err = json.Unmarshal([]byte(params), &job.Parameters)
	}
	return job, err
}
