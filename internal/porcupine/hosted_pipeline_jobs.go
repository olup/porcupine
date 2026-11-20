package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/olup/porcupine/internal/auth"
	"github.com/olup/porcupine/internal/delivery"
	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/output"
	"github.com/olup/porcupine/internal/usage"
)

type hostedPipelineAuthority struct {
	service *Service
}

func (a hostedPipelineAuthority) ResolveCreatorTx(ctx context.Context, tx *sql.Tx, tenant, project, creator string) (bool, error) {
	var owner string
	err := tx.QueryRowContext(ctx, `SELECT p.tenant_id FROM projects p JOIN tenants t ON t.id=p.tenant_id WHERE p.id=$1 AND NOT EXISTS(SELECT 1 FROM project_deletions d WHERE d.project_id=p.id) AND NOT EXISTS(SELECT 1 FROM tenant_deletions d WHERE d.tenant_id=t.id) FOR SHARE OF p,t`, project).Scan(&owner)
	if errors.Is(err, sql.ErrNoRows) || err == nil && owner != tenant {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	var grantID, humanID, clientID string
	err = tx.QueryRowContext(ctx, `SELECT id,human_id,client_id FROM oauth_interactive_grants WHERE principal_id=$1 AND expires_at_ms>extract(epoch FROM clock_timestamp())*1000 FOR SHARE`, creator).Scan(&grantID, &humanID, &clientID)
	if errors.Is(err, sql.ErrNoRows) {
		return (hostedPipelineNativeAuthority{}).ResolveCreatorTx(ctx, tx, tenant, project, creator)
	}
	if err != nil {
		return false, err
	}
	if a.service.externalAuthEnabled() || !a.service.interactiveOAuthEnabled() {
		return false, nil
	}

	// The grant, its resource, and each participant remain current through the
	// consuming transaction. Reuse the builtin resolver for scope intersection.
	for _, lock := range []struct {
		statement string
		args      []any
	}{
		{`SELECT id FROM access_keys WHERE id IN ($1,$2,$3) ORDER BY id FOR SHARE`, []any{creator, humanID, clientID}},
		{`SELECT principal_id FROM email_humans WHERE principal_id=$1 FOR SHARE`, []any{humanID}},
		{`SELECT client_id FROM oauth_clients WHERE client_id=$1 FOR SHARE`, []any{clientID}},
		{`SELECT tenant_id FROM tenant_memberships WHERE human_id=$1 AND tenant_id=$2 FOR SHARE`, []any{humanID, tenant}},
		{`SELECT tenant_id FROM tenant_credentials WHERE principal_id=$1 FOR SHARE`, []any{clientID}},
		{`SELECT tenant_id FROM oauth_grant_tenants WHERE grant_id=$1 FOR SHARE`, []any{grantID}},
	} {
		if _, err = tx.ExecContext(ctx, lock.statement, lock.args...); err != nil {
			return false, err
		}
	}
	actor, err := a.service.loadInteractiveOAuthActor(ctx, tx, grantID)
	if errors.Is(err, errInteractiveGrant) || errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return auth.CanExecutePipeline(actor, project), nil
}

type hostedPipelineExecution struct {
	snapshotCut    output.ExportSnapshotCut
	deleteGate     func(context.Context, *sql.Tx, lakeapi.HostedProjectScope, string) (bool, error)
	beginAttemptTx func(context.Context, *sql.Tx, string, string) error
	endAttempt     func(context.Context, string, string) error
}

func (s *Service) newHostedPipelines(c *output.PipelineConfig, effects *output.PGPipelineEffects, coordinator *lakeapi.PostgresProjectCoordinator, sourceID string, execution hostedPipelineExecution) (*output.PipelineJobs, error) {
	if s.pipelineClient == nil {
		s.pipelineClient = delivery.NewWebhookHTTPClient()
	}
	d := output.HostedPipelineDependencies{
		PipelineDependencies: output.PipelineDependencies{
			Control: s.control, Clock: func() time.Time { return s.clock() },
			WebhookClient: s.pipelineClient, Acquire: s.queries.TryBackground,
			Usage: usage.NewRecorder(s.control, "pipelines-"+sourceID, s.clock),
		},
		Effects: effects,
		ProjectTransaction: func(ctx context.Context, project string, work func(context.Context, *sql.Tx) error) error {
			key := lakeapi.ProjectWriteKey{SourceID: sourceID, ProjectID: project}
			session, err := coordinator.Acquire(ctx, key)
			if err != nil {
				return err
			}
			defer session.Release()
			return session.RunControlTx(ctx, work)
		},
	}
	if s.lake != nil {
		d.Reader = s.lake
	}
	if execution.snapshotCut != nil {
		d.BeginActivity = s.hosted.incarnation.beginOutputWork
		if s.lake == nil || execution.beginAttemptTx == nil || execution.endAttempt == nil {
			return nil, errors.New("hosted scheduled pipelines require retained analytical snapshots")
		}
		d.Scheduled = output.HostedScheduledDependencies{Reader: s.lake, SnapshotCut: execution.snapshotCut, BeginAttemptTx: execution.beginAttemptTx, EndAttempt: execution.endAttempt}
	}
	return output.NewHostedPipelines(d, c)
}

func (s *Service) attachHostedWebhookWorker(p *output.PipelineJobs, flusher *hostedLiveFlusher, execution hostedPipelineExecution) error {
	if !p.Enabled() {
		return nil
	}
	if execution.deleteGate == nil || execution.beginAttemptTx == nil || execution.endAttempt == nil {
		return errors.New("hosted webhooks require coordinated deletion and activity")
	}
	config := p.Config()
	limits := output.HostedWebhookLimits{MaxAttempts: config.WebhookMaxAttempts, Timeout: time.Duration(config.WebhookTimeoutSeconds) * time.Second}
	worker := newHostedPipelineDelivery(flusher, p.HostedWebhookDestinationTx, limits, s.pipelineClient,
		execution.deleteGate, execution.beginAttemptTx, execution.endAttempt)
	// The flusher needs PipelineJobs for publication, and the worker needs the flusher.
	p.SetHostedWebhookWorker(worker.Step)
	return nil
}
