package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"strconv"

	lakeapi "github.com/olup/porcupine/internal/lake"
)

const hostedMutationSchemaV1 = `
CREATE TABLE hosted_mutation_capacity (
	id INTEGER PRIMARY KEY CHECK(id=1),
	retained_bytes BIGINT NOT NULL CHECK(retained_bytes>=0)
);
INSERT INTO hosted_mutation_capacity (id,retained_bytes) VALUES (1,0);
CREATE TABLE hosted_mutation_projects (
	source_id TEXT NOT NULL, project_id TEXT NOT NULL, tenant_id TEXT NOT NULL,
	catalog_id TEXT NOT NULL, stream_id TEXT NOT NULL,
	revision BIGINT NOT NULL DEFAULT 0 CHECK(revision>=0),
	accepted_through BIGINT NOT NULL DEFAULT 0 CHECK(accepted_through>=0),
	settled_through BIGINT NOT NULL DEFAULT 0 CHECK(settled_through BETWEEN 0 AND accepted_through),
	retained_bytes BIGINT NOT NULL DEFAULT 0 CHECK(retained_bytes>=0),
	PRIMARY KEY(source_id,project_id)
);
CREATE TABLE hosted_mutation_intents (
	source_id TEXT NOT NULL, project_id TEXT NOT NULL, tenant_id TEXT NOT NULL,
	catalog_id TEXT NOT NULL, stream_id TEXT NOT NULL, mutation_id TEXT NOT NULL,
	key_hash TEXT NOT NULL, request_hash TEXT NOT NULL, actor_hash TEXT NOT NULL,
	operation TEXT NOT NULL CHECK(operation IN ('edit','delete')),
	expected_revision BIGINT NOT NULL CHECK(expected_revision>=0),
	revision BIGINT NOT NULL CHECK(revision>expected_revision),
	state TEXT NOT NULL CHECK(state IN ('staging','ready','applied','failed','aborted')),
	acceptance_order BIGINT,
	body_key TEXT NOT NULL, body_sha256 TEXT NOT NULL, body_bytes BIGINT NOT NULL CHECK(body_bytes>0),
	body_version TEXT NOT NULL DEFAULT '', body_prefix TEXT NOT NULL,
	created_at TEXT NOT NULL, applied_at TEXT NOT NULL DEFAULT '', error_code TEXT NOT NULL DEFAULT '',
	PRIMARY KEY(source_id,project_id,mutation_id),
	UNIQUE(source_id,project_id,acceptance_order),
	FOREIGN KEY(source_id,project_id) REFERENCES hosted_mutation_projects(source_id,project_id),
	CHECK((acceptance_order IS NULL)=(state IN ('staging','aborted')))
);
CREATE UNIQUE INDEX hosted_mutation_idempotency
	ON hosted_mutation_intents(source_id,project_id,key_hash) WHERE state<>'aborted';
CREATE UNIQUE INDEX hosted_mutation_one_unresolved
	ON hosted_mutation_intents(source_id,project_id) WHERE state IN ('staging','ready');
`

const hostedMutationProjectCapacity int64 = 128 << 20
const hostedMutationInstanceCapacity int64 = 8 << 30

var errHostedMutationBusy = errors.New("a project mutation is unresolved")
var errHostedMutationCapacity = errors.New("hosted mutation storage capacity exhausted")
var errHostedMutationConflict = errors.New("hosted mutation intent conflicts with original request")

type hostedMutationIntent struct {
	scope                                             lakeapi.HostedProjectScope
	tenant, id, keyHash, requestHash, actorHash       string
	operation, state, createdAt, appliedAt, errorCode string
	revision, expected                                int64
	order                                             sql.NullInt64
	body                                              lakeapi.ImmutableObjectRef
	prefix                                            string
}

const hostedMutationColumns = `source_id,project_id,catalog_id,stream_id,tenant_id,mutation_id,key_hash,request_hash,actor_hash,
operation,state,created_at,applied_at,error_code,revision,expected_revision,acceptance_order,body_key,body_sha256,body_bytes,body_version,body_prefix`

type hostedMutationQuery interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func scanHostedMutation(row *sql.Row) (hostedMutationIntent, error) {
	var i hostedMutationIntent
	err := row.Scan(&i.scope.Key.SourceID, &i.scope.Key.ProjectID, &i.scope.CatalogID, &i.scope.StreamID, &i.tenant, &i.id, &i.keyHash, &i.requestHash, &i.actorHash,
		&i.operation, &i.state, &i.createdAt, &i.appliedAt, &i.errorCode, &i.revision, &i.expected, &i.order, &i.body.Key, &i.body.SHA256, &i.body.Bytes, &i.body.VersionID, &i.prefix)
	return i, err
}

func hostedMutationBy(ctx context.Context, query hostedMutationQuery, scope lakeapi.HostedProjectScope, tenant, filter, value string) (hostedMutationIntent, error) {
	return scanHostedMutation(query.QueryRowContext(ctx, `SELECT `+hostedMutationColumns+` FROM hosted_mutation_intents
WHERE source_id=$1 AND project_id=$2 AND catalog_id=$3 AND stream_id=$4 AND tenant_id=$5 AND state<>'aborted' AND `+filter+`=$6`, scope.Key.SourceID, scope.Key.ProjectID, scope.CatalogID, scope.StreamID, tenant, value))
}

func hostedMutationProjectRevision(ctx context.Context, query hostedMutationQuery, scope lakeapi.HostedProjectScope, tenant string) (int64, error) {
	var revision int64
	err := query.QueryRowContext(ctx, `SELECT revision FROM hosted_mutation_projects WHERE source_id=$1 AND project_id=$2
AND catalog_id=$3 AND stream_id=$4 AND tenant_id=$5`, scope.Key.SourceID, scope.Key.ProjectID, scope.CatalogID, scope.StreamID, tenant).Scan(&revision)
	if errors.Is(err, sql.ErrNoRows) {
		err = nil
	}
	return revision, err
}

// reserveHostedMutationTx records exact ownership before PUT. The project
// session serializes staging with mutation application and ordering cuts.
func reserveHostedMutationTx(ctx context.Context, tx *sql.Tx, i hostedMutationIntent) error {
	var retained int64
	if err := tx.QueryRowContext(ctx, `SELECT retained_bytes FROM hosted_mutation_capacity WHERE id=1 FOR UPDATE`).Scan(&retained); err != nil {
		return err
	}
	if i.body.Bytes <= 0 || i.body.Bytes > lakeapi.MaxImmutableEffectObjectBytes || retained > hostedMutationInstanceCapacity-i.body.Bytes {
		return errHostedMutationCapacity
	}
	if err := lockActiveHostedMutationProject(ctx, tx, i.scope.Key.ProjectID, i.tenant); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO hosted_mutation_projects(source_id,project_id,tenant_id,catalog_id,stream_id)
VALUES($1,$2,$3,$4,$5) ON CONFLICT(source_id,project_id) DO NOTHING`, i.scope.Key.SourceID, i.scope.Key.ProjectID, i.tenant, i.scope.CatalogID, i.scope.StreamID); err != nil {
		return err
	}
	var revision, bytes int64
	err := tx.QueryRowContext(ctx, `SELECT revision,retained_bytes FROM hosted_mutation_projects
WHERE source_id=$1 AND project_id=$2 AND tenant_id=$3 AND catalog_id=$4 AND stream_id=$5 FOR UPDATE`, i.scope.Key.SourceID, i.scope.Key.ProjectID, i.tenant, i.scope.CatalogID, i.scope.StreamID).Scan(&revision, &bytes)
	if err != nil {
		return err
	}
	if bytes > hostedMutationProjectCapacity-i.body.Bytes {
		return errHostedMutationCapacity
	}
	if i.revision <= max(i.expected, revision) {
		return errHostedMutationConflict
	}
	var unresolved bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM hosted_mutation_intents
WHERE source_id=$1 AND project_id=$2 AND state IN ('staging','ready'))`, i.scope.Key.SourceID, i.scope.Key.ProjectID).Scan(&unresolved); err != nil {
		return err
	}
	if unresolved {
		return errHostedMutationBusy
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO hosted_mutation_intents
(source_id,project_id,tenant_id,catalog_id,stream_id,mutation_id,key_hash,request_hash,actor_hash,operation,
expected_revision,revision,state,body_key,body_sha256,body_bytes,body_prefix,created_at)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'staging',$13,$14,$15,$16,$17)`,
		i.scope.Key.SourceID, i.scope.Key.ProjectID, i.tenant, i.scope.CatalogID, i.scope.StreamID, i.id, i.keyHash, i.requestHash, i.actorHash, i.operation, i.expected, i.revision, i.body.Key, i.body.SHA256, i.body.Bytes, i.prefix, i.createdAt); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE hosted_mutation_projects SET revision=$3,retained_bytes=retained_bytes+$4
WHERE source_id=$1 AND project_id=$2`, i.scope.Key.SourceID, i.scope.Key.ProjectID, i.revision, i.body.Bytes); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE hosted_mutation_capacity SET retained_bytes=retained_bytes+$1 WHERE id=1`, i.body.Bytes)
	return err
}

func lockActiveHostedMutationProject(ctx context.Context, tx *sql.Tx, project, tenant string) error {
	var id string
	if err := tx.QueryRowContext(ctx, `SELECT id FROM tenants WHERE id=$1 FOR UPDATE`, tenant).Scan(&id); err != nil {
		return err
	}
	if err := tx.QueryRowContext(ctx, `SELECT id FROM projects WHERE id=$1 AND tenant_id=$2 FOR UPDATE`, project, tenant).Scan(&id); err != nil {
		return err
	}
	var deleting bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM project_deletions WHERE project_id=$1)
OR EXISTS(SELECT 1 FROM tenant_deletions WHERE tenant_id=$2)`, project, tenant).Scan(&deleting)
	if err == nil && deleting {
		return sql.ErrNoRows
	}
	return err
}

func readyHostedMutationTx(ctx context.Context, tx *sql.Tx, i hostedMutationIntent, body lakeapi.ImmutableObjectRef) error {
	if body.Key != i.body.Key || body.SHA256 != i.body.SHA256 || body.Bytes != i.body.Bytes {
		return errHostedMutationConflict
	}
	var order int64
	err := tx.QueryRowContext(ctx, `UPDATE hosted_mutation_projects SET accepted_through=accepted_through+1
WHERE source_id=$1 AND project_id=$2 AND tenant_id=$3 AND catalog_id=$4 AND stream_id=$5
AND accepted_through=settled_through AND accepted_through<$6 RETURNING accepted_through`,
		i.scope.Key.SourceID, i.scope.Key.ProjectID, i.tenant, i.scope.CatalogID, i.scope.StreamID, int64(math.MaxInt64)).Scan(&order)
	if err != nil {
		return err
	}
	var id string
	return tx.QueryRowContext(ctx, `UPDATE hosted_mutation_intents SET state='ready',body_version=$4,acceptance_order=$5
WHERE source_id=$1 AND project_id=$2 AND mutation_id=$3 AND state='staging' AND body_sha256=$6 RETURNING mutation_id`,
		i.scope.Key.SourceID, i.scope.Key.ProjectID, i.id, body.VersionID, order, i.body.SHA256).Scan(&id)
}

func (i hostedMutationIntent) response(uuid string) MutationResponse {
	state := i.state
	if state == "ready" {
		state = "pending"
	}
	return MutationResponse{MutationID: i.id, ProjectID: i.scope.Key.ProjectID, UUID: uuid, Revision: strconv.FormatInt(i.revision, 10), Operation: i.operation, State: state, CreatedAt: i.createdAt, AppliedAt: i.appliedAt, ErrorCode: i.errorCode}
}

func finishHostedMutationTx(ctx context.Context, tx *sql.Tx, i hostedMutationIntent, evidence lakeapi.NativeCommitEvidence) error {
	if !i.order.Valid || i.state != "ready" {
		return errHostedMutationConflict
	}
	state := "applied"
	if evidence.Mutation.ErrorCode != "" {
		state = "failed"
	}
	var id string
	err := tx.QueryRowContext(ctx, `UPDATE hosted_mutation_intents SET state=$4,applied_at=$5,error_code=$6
WHERE source_id=$1 AND project_id=$2 AND mutation_id=$3 AND state='ready' AND acceptance_order=$7 AND body_sha256=$8
RETURNING mutation_id`, i.scope.Key.SourceID, i.scope.Key.ProjectID, i.id, state, evidence.AccountedAt.Format("2006-01-02T15:04:05.999999999Z07:00"), evidence.Mutation.ErrorCode, i.order.Int64, i.body.SHA256).Scan(&id)
	if err != nil {
		return err
	}
	var order int64
	return tx.QueryRowContext(ctx, `UPDATE hosted_mutation_projects SET settled_through=$6
WHERE source_id=$1 AND project_id=$2 AND tenant_id=$3 AND catalog_id=$4 AND stream_id=$5
AND accepted_through=$6 AND settled_through=$6-1 RETURNING settled_through`,
		i.scope.Key.SourceID, i.scope.Key.ProjectID, i.tenant, i.scope.CatalogID, i.scope.StreamID, i.order.Int64).Scan(&order)
}

func retireHostedMutationStagingTx(ctx context.Context, tx *sql.Tx, scope lakeapi.HostedProjectScope, tenant, createdBefore string) error {
	_, err := tx.ExecContext(ctx, `UPDATE hosted_mutation_intents SET state='aborted'
WHERE source_id=$1 AND project_id=$2 AND tenant_id=$3 AND catalog_id=$4 AND stream_id=$5
AND state='staging' AND created_at<$6`, scope.Key.SourceID, scope.Key.ProjectID, tenant, scope.CatalogID, scope.StreamID, createdBefore)
	return err
}
