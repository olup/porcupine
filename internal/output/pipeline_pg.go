package output

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/olup/porcupine/internal/auth"
	eventapi "github.com/olup/porcupine/internal/event"
)

const PipelineJobsPostgresSchemaV1 = `
CREATE TABLE hosted_pipeline_configs (
 project_id TEXT NOT NULL, id TEXT NOT NULL, tenant_id TEXT NOT NULL,
 name TEXT NOT NULL, config TEXT NOT NULL, secret BYTEA NOT NULL,
 created_at_ms BIGINT NOT NULL,
 PRIMARY KEY(project_id,id),
 FOREIGN KEY(project_id,id) REFERENCES pipeline_effect_definitions(project_id,id) ON DELETE CASCADE);
CREATE INDEX hosted_pipeline_configs_tenant ON hosted_pipeline_configs(tenant_id);
`

type HostedPipelineDependencies struct {
	PipelineDependencies
	Effects            *PGPipelineEffects
	Scheduled          HostedScheduledDependencies
	ProjectTransaction func(context.Context, string, func(context.Context, *sql.Tx) error) error
}

type hostedPipelineRuntime struct {
	effects     *PGPipelineEffects
	scheduled   HostedScheduledDependencies
	transaction func(context.Context, string, func(context.Context, *sql.Tx) error) error
	step        func(context.Context) (bool, error)
	client      *http.Client
}

func NewHostedPipelines(d HostedPipelineDependencies, config *PipelineConfig) (*PipelineJobs, error) {
	if d.Control == nil || d.Effects == nil || d.ProjectTransaction == nil || d.Scheduled.ready() && d.Acquire == nil {
		return nil, ErrPipelineConflict
	}
	d.Journal = d.Control
	p, err := newPipelineJobs(d.PipelineDependencies, config)
	if err != nil {
		return nil, err
	}
	p.pg = &hostedPipelineRuntime{effects: d.Effects, scheduled: d.Scheduled, transaction: d.ProjectTransaction, client: d.WebhookClient}
	return p, nil
}

// SetHostedWebhookWorker wires execution before Start.
func (s *PipelineJobs) SetHostedWebhookWorker(step func(context.Context) (bool, error)) {
	s.pg.step = step
}

func (s *PipelineJobs) initializeHostedPipelines(ctx context.Context) error {
	if !s.Enabled() {
		var configured bool
		err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM hosted_pipeline_configs)`).Scan(&configured)
		if err == nil && configured {
			return errors.New("existing pipelines require their original encryption key")
		}
		return err
	}
	result, err := s.db.ExecContext(ctx, `INSERT INTO metadata(key,value) VALUES('hosted_pipeline_key_fingerprint',$1)
ON CONFLICT(key) DO UPDATE SET value=excluded.value WHERE metadata.value=excluded.value`, s.fingerprint)
	if err != nil {
		return err
	}
	if pipelineOne(result) != nil {
		return errors.New("pipeline encryption key does not match the shared configuration")
	}
	return nil
}

const hostedPipelineColumns = `c.id,c.project_id,d.creator_id,c.kind,c.name,d.state,c.config,c.secret,c.created_at_ms`
const hostedPipelineJoin = ` FROM hosted_pipeline_configs c JOIN pipeline_effect_definitions d USING(project_id,id,tenant_id)`

func (s *PipelineJobs) hostedPipeline(ctx context.Context, project, id string) (storedPipeline, error) {
	return scanPipeline(s.db.QueryRowContext(ctx, `SELECT `+hostedPipelineColumns+hostedPipelineJoin+` WHERE c.project_id=$1 AND c.id=$2 AND d.state<>'retired'`, project, id))
}

func (s *PipelineJobs) hostedPipelinePage(ctx context.Context, project string, limit, offset int) (*sql.Rows, error) {
	return s.db.QueryContext(ctx, `SELECT c.id,c.project_id,c.kind,c.name,d.state,c.created_at_ms`+hostedPipelineJoin+` WHERE c.project_id=$1 AND d.state<>'retired' ORDER BY c.created_at_ms DESC,c.id LIMIT $2 OFFSET $3`, project, limit, offset)
}

func hostedRuleRef(id, revision string, raw []byte) PipelineRef {
	sum := sha256.Sum256(raw)
	return PipelineRef{Key: "control-rule/" + id + "/" + revision, SHA256: hex.EncodeToString(sum[:]), Bytes: int64(len(raw))}
}

// HostedPipelineRule loads the definition rule frozen by the caller's project
// session. The publisher verifies the same rule reference when it reserves work.
func (s *PipelineJobs) HostedPipelineRule(ctx context.Context, tenant, project string, target PipelineTarget) ([]byte, error) {
	var config string
	err := s.db.QueryRowContext(ctx, `SELECT config`+hostedPipelineJoin+` WHERE c.tenant_id=$1 AND c.project_id=$2 AND c.id=$3 AND d.incarnation=$4 AND d.rule_revision=$5`, tenant, project, target.ID, target.Incarnation, target.RuleRevision).Scan(&config)
	if err != nil {
		return nil, err
	}
	if hostedRuleRef(target.ID, target.RuleRevision, []byte(config)) != target.Rule {
		return nil, ErrPipelineConflict
	}
	return []byte(config), nil
}

func hostedCredentialBinding(tenant, id, incarnation, revision string) string {
	raw, _ := json.Marshal([4]string{tenant, id, incarnation, revision})
	return string(raw)
}

func (s *PipelineJobs) HostedWebhookDestinationTx(ctx context.Context, tx *sql.Tx, tenant, project, id, incarnation, revision string) (WebhookPipelineSecret, error) {
	var destination WebhookPipelineSecret
	var sealed []byte
	err := tx.QueryRowContext(ctx, `SELECT c.secret`+hostedPipelineJoin+` WHERE c.kind='webhook' AND c.tenant_id=$1 AND c.project_id=$2 AND c.id=$3 AND d.incarnation=$4 AND d.destination_revision=$5 AND d.state='active' FOR UPDATE OF d,c`, tenant, project, id, incarnation, revision).Scan(&sealed)
	if err != nil {
		return destination, err
	}
	plain, err := s.OpenSecret(project, hostedCredentialBinding(tenant, id, incarnation, revision), sealed)
	if err != nil || json.Unmarshal(plain, &destination) != nil {
		return WebhookPipelineSecret{}, ErrPipelineConflict
	}
	return destination, nil
}

func pipelineHostedError(err error) error {
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return apiError(404, "pipeline not found")
	case errors.Is(err, ErrPipelineAuthority):
		return apiError(403, "pipeline creator is not authorized")
	case errors.Is(err, ErrPipelineCapacity):
		return apiError(409, "pipeline limit reached")
	case errors.Is(err, ErrPipelineConflict):
		return apiError(409, "pipeline changed; retry the request")
	default:
		var status interface{ GetStatus() int }
		if errors.As(err, &status) {
			return err
		}
		return apiError(503, "pipeline storage unavailable")
	}
}

func (s *PipelineJobs) defineHostedPipeline(ctx context.Context, project string, request PipelineRequest, caller auth.Principal) (*Pipeline, error) {
	if !s.Enabled() {
		return nil, apiError(503, "pipelines are not configured")
	}
	if request.Kind == "s3" && !s.pg.scheduled.ready() {
		return nil, apiError(503, "hosted query pipelines are unavailable")
	}
	if caller.External != nil {
		return nil, pipelineHostedError(ErrPipelineAuthority)
	}
	if strings.TrimSpace(request.Name) == "" || len(request.Name) > 200 {
		return nil, apiError(400, "invalid pipeline name")
	}
	p := Pipeline{ID: eventapi.NewID(), ProjectID: project, Kind: request.Kind, Name: request.Name, State: "active", CreatedAtMS: s.clock().UnixMilli()}
	t := PipelineTarget{ID: p.ID, Incarnation: eventapi.NewID(), RuleRevision: eventapi.NewID(), DestinationRevision: eventapi.NewID(), CreatorID: caller.ID}
	config, private, err := s.encodePipelineDestinationPlain(ctx, project, p.Kind, request.Webhook, request.S3, request.Credentials)
	if err != nil {
		return nil, err
	}
	t.Rule = hostedRuleRef(t.ID, t.RuleRevision, config)
	if p.Kind == "s3" {
		t.Rule.Key = hostedScheduledRulePrefix + t.Rule.Key
	}
	err = s.pg.transaction(ctx, project, func(ctx context.Context, tx *sql.Tx) error {
		var tenant string
		if err := tx.QueryRowContext(ctx, `SELECT tenant_id FROM projects WHERE id=$1`, project).Scan(&tenant); err != nil {
			return err
		}
		if err := s.EnsureHostedPipelineCapacityTx(ctx, tx, tenant, project); err != nil {
			return err
		}
		var total, projectCount, tenantCount int
		if err := tx.QueryRowContext(ctx, `SELECT count(*),count(*) FILTER(WHERE project_id=$1),count(*) FILTER(WHERE tenant_id=$2) FROM hosted_pipeline_configs`, project, tenant).Scan(&total, &projectCount, &tenantCount); err != nil {
			return err
		}
		if total >= 512 || projectCount >= 64 || tenantCount >= 256 {
			return ErrPipelineCapacity
		}
		sealed, err := s.SealSecret(project, hostedCredentialBinding(tenant, t.ID, t.Incarnation, t.DestinationRevision), private)
		if err != nil {
			return err
		}
		if err = s.pg.effects.CompareAndSwapDefinitionTx(ctx, tx, nil, PipelineDefinition{TenantID: tenant, ProjectID: project, State: "active", Target: t}); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO hosted_pipeline_configs(project_id,id,tenant_id,name,config,secret,created_at_ms,kind) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, project, p.ID, tenant, p.Name, string(config), sealed, p.CreatedAtMS, p.Kind)
		if err != nil || p.Kind != "s3" {
			return err
		}
		return createHostedScheduledState(ctx, tx, p, config)
	})
	if err != nil {
		return nil, pipelineHostedError(err)
	}
	s.Wake()
	public, err := publicPipeline(storedPipeline{Pipeline: p, Config: config})
	return &public, err
}

func (s *PipelineJobs) changeHostedPipeline(ctx context.Context, project, id, state string, replacement *PipelineChange) (*Pipeline, error) {
	if (state == "active" || replacement != nil) && !s.Enabled() {
		return nil, apiError(503, "pipelines are not configured")
	}
	if state == "retired" {
		p, err := s.hostedPipeline(ctx, project, id)
		if err != nil {
			return nil, pipelineHostedError(err)
		}
		if p.Kind == "s3" && p.State != "paused" {
			if _, err = s.changeHostedPipeline(ctx, project, id, "paused", nil); err != nil {
				return nil, err
			}
		}
	}
	var p storedPipeline
	err := s.pg.transaction(ctx, project, func(ctx context.Context, tx *sql.Tx) error {
		var tenant, config string
		var t PipelineTarget
		t.ID = id
		p.ID, p.ProjectID = id, project
		err := tx.QueryRowContext(ctx, `SELECT c.tenant_id,c.kind,c.name,d.state,c.config,c.secret,c.created_at_ms,d.creator_id,d.incarnation,d.rule_revision,d.destination_revision,d.rule_key,d.rule_version,d.rule_sha,d.rule_bytes`+hostedPipelineJoin+` WHERE c.project_id=$1 AND c.id=$2 AND d.state<>'retired'`, project, id).Scan(&tenant, &p.Kind, &p.Name, &p.State, &config, &p.Secret, &p.CreatedAtMS, &t.CreatorID, &t.Incarnation, &t.RuleRevision, &t.DestinationRevision, &t.Rule.Key, &t.Rule.VersionID, &t.Rule.SHA256, &t.Rule.Bytes)
		if err != nil {
			return err
		}
		p.Config = json.RawMessage(config)
		if state == "retired" && p.Kind == "s3" && p.State != "paused" {
			return ErrPipelineConflict
		}
		next := t
		if replacement != nil {
			change := *replacement
			private, err := s.OpenSecret(project, hostedCredentialBinding(tenant, id, t.Incarnation, t.DestinationRevision), p.Secret)
			if err != nil {
				return err
			}
			if change.Webhook == nil && change.S3 == nil {
				if p.Kind == "s3" {
					var spec S3PipelineSpec
					if json.Unmarshal(p.Config, &spec) != nil {
						return ErrPipelineConflict
					}
					change.S3 = &spec
				} else {
					var spec WebhookPipelineSpec
					var old WebhookPipelineSecret
					if json.Unmarshal(p.Config, &spec) != nil || json.Unmarshal(private, &old) != nil {
						return ErrPipelineConflict
					}
					spec.Endpoint = old.Endpoint
					change.Webhook = &spec
				}
			}
			newConfig, newPrivate, err := s.encodePipelineDestinationPlain(ctx, project, p.Kind, change.Webhook, change.S3, change.Credentials)
			if err != nil {
				return err
			}
			if !bytes.Equal(p.Config, newConfig) {
				if p.Kind == "s3" {
					return apiError(409, "S3 specification cannot change during credential rotation")
				}
				return apiError(409, "event filters cannot change during credential rotation")
			}
			if bytes.Equal(private, newPrivate) {
				return nil
			}
			next.DestinationRevision = eventapi.NewID()
			p.Secret, err = s.SealSecret(project, hostedCredentialBinding(tenant, id, next.Incarnation, next.DestinationRevision), newPrivate)
			if err != nil {
				return err
			}
			state = p.State
		}
		if err = s.pg.effects.CompareAndSwapDefinitionTx(ctx, tx, &t, PipelineDefinition{TenantID: tenant, ProjectID: project, State: state, Target: next}); err != nil {
			return err
		}
		p.State = state
		if state == "retired" {
			if p.Kind == "s3" {
				if err := s.retireHostedScheduledTx(ctx, tx, tenant, p); err != nil {
					return err
				}
			}
			_, err = tx.ExecContext(ctx, `DELETE FROM hosted_pipeline_configs WHERE project_id=$1 AND id=$2 AND tenant_id=$3`, project, id, tenant)
			if err == nil {
				_, err = tx.ExecContext(ctx, `DELETE FROM pipeline_effect_definitions WHERE project_id=$1 AND id=$2 AND tenant_id=$3`, project, id, tenant)
			}
		} else if replacement != nil {
			if p.Kind == "s3" {
				if err := s.cancelReplacedHostedScheduleTx(ctx, tx, tenant, p); err != nil {
					return err
				}
			}
			_, err = tx.ExecContext(ctx, `UPDATE hosted_pipeline_configs SET secret=$4 WHERE project_id=$1 AND id=$2 AND tenant_id=$3`, project, id, tenant, p.Secret)
		}
		return err
	})
	if err != nil {
		return nil, pipelineHostedError(err)
	}
	s.Wake()
	public, err := publicPipeline(p)
	return &public, err
}

// EnsureHostedPipelineCapacityTx provisions finite scope defaults. Operator
// policy already stored for a scope takes precedence over deployment defaults.
func (s *PipelineJobs) EnsureHostedPipelineCapacityTx(ctx context.Context, tx *sql.Tx, tenant, project string) error {
	instanceRefs, instanceBytes := s.config.WebhookMaxQueuedEvents, s.config.WebhookMaxQueuedBytes
	projectRefs, projectBytes := s.config.WebhookProjectMaxQueuedEvents, s.config.WebhookProjectMaxQueuedBytes
	if !s.Enabled() {
		instanceRefs, instanceBytes = 10000, 64<<20
		projectRefs, projectBytes = 1000, 16<<20
	}
	for _, scope := range []struct {
		kind, id string
		demand   PipelineDemand
	}{
		{"instance", "", PipelineDemand{References: int64(instanceRefs), QueuedBytes: instanceBytes, MetadataRows: 65536, ArtifactBytes: 1 << 30, ProofSlots: 128}},
		{"tenant", tenant, PipelineDemand{References: int64(instanceRefs), QueuedBytes: instanceBytes, MetadataRows: 16384, ArtifactBytes: 256 << 20, ProofSlots: 32}},
		{"project", project, PipelineDemand{References: int64(projectRefs), QueuedBytes: projectBytes, MetadataRows: 4096, ArtifactBytes: 128 << 20, ProofSlots: 4}},
	} {
		d := scope.demand
		_, err := tx.ExecContext(ctx, `INSERT INTO pipeline_capacity(scope_kind,scope_id,limit_refs,limit_bytes,limit_rows,limit_artifacts,limit_proofs) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT DO NOTHING`, scope.kind, scope.id, d.References, d.QueuedBytes, d.MetadataRows, d.ArtifactBytes, d.ProofSlots)
		if err != nil {
			return err
		}
	}
	return s.pg.effects.lockCapacity(ctx, tx, PipelineBinding{Scope: PipelineScope{ProjectID: project}, TenantID: tenant}, false)
}

func (s *PipelineJobs) cleanupHostedPipelineProject(ctx context.Context, project string) error {
	return s.pg.transaction(ctx, project, func(ctx context.Context, tx *sql.Tx) error {
		var pending bool
		err := tx.QueryRowContext(ctx, `SELECT EXISTS(
			SELECT 1 FROM pipeline_effect_reservations WHERE project_id=$1
			UNION ALL SELECT 1 FROM pipeline_effect_jobs WHERE project_id=$1
			UNION ALL SELECT 1 FROM pipeline_effect_cleanup WHERE project_id=$1)`, project).Scan(&pending)
		if err != nil {
			return err
		}
		if pending {
			return ErrPipelineConflict
		}
		rows, err := tx.QueryContext(ctx, `SELECT c.id,c.tenant_id,c.config FROM hosted_pipeline_configs c WHERE c.project_id=$1 AND c.kind='s3' ORDER BY c.id`, project)
		if err != nil {
			return err
		}
		type scheduled struct {
			p      storedPipeline
			tenant string
		}
		var jobs []scheduled
		for rows.Next() {
			var job scheduled
			var config string
			if err = rows.Scan(&job.p.ID, &job.tenant, &config); err != nil {
				break
			}
			job.p.ProjectID, job.p.Config = project, json.RawMessage(config)
			jobs = append(jobs, job)
		}
		if err = errors.Join(err, rows.Err(), rows.Close()); err != nil {
			return err
		}
		for _, job := range jobs {
			b := PipelineBinding{Scope: PipelineScope{ProjectID: project}, TenantID: job.tenant}
			if err = s.pg.effects.lockCapacity(ctx, tx, b, false); err != nil {
				return err
			}
			if err = s.retireHostedScheduledTx(ctx, tx, job.tenant, job.p); err != nil {
				return err
			}
		}
		_, err = tx.ExecContext(ctx, `DELETE FROM pipeline_effect_definitions WHERE project_id=$1`, project)
		return err
	})
}
