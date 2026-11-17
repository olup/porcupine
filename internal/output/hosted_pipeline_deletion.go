package output

import (
	"context"
	"database/sql"
	"errors"

	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/s3client"
)

const HostedDeletionPipeline = "pipeline"

// HostedPipelineDeletionInventory resolves managed webhook artifacts without
// reading or contacting the pipeline's customer destination.
func HostedPipelineDeletionInventory(ctx context.Context, tx *sql.Tx, tenant string, scope lakeapi.HostedProjectScope, objects *lakeapi.DuckLakeObjectStore) ([]HostedDeletionArtifact, error) {
	var ambiguous bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(
		SELECT 1 FROM managed_artifact_ownership WHERE project_id=$1 AND kind='pipeline_unpublished'
		UNION ALL SELECT 1 FROM pipeline_effect_assemblies a WHERE a.project_id=$1 AND NOT EXISTS (
			SELECT 1 FROM pipeline_effect_cleanup c WHERE c.project_id=a.project_id AND c.tenant_id=a.tenant_id
			AND c.source_id=a.source_id AND c.catalog_id=a.catalog_id AND c.stream_id=a.stream_id
			AND c.owner_id=a.generation AND c.generation=a.attempt)
		UNION ALL SELECT 1 FROM hosted_s3_pipeline_runs WHERE project_id=$1 AND attempt_id<>'' AND state<>'succeeded')`, scope.Key.ProjectID).Scan(&ambiguous)
	if err != nil {
		return nil, err
	}
	if ambiguous {
		return nil, errHostedOutputOwnership
	}
	rows, err := tx.QueryContext(ctx, `WITH owned AS (
		SELECT tenant_id,source_id,catalog_id,stream_id,artifact_prefix AS key,coalesce(nullif(acquisition_id,''),'invalid') AS prefix_owner FROM pipeline_effect_reservations WHERE project_id=$1
		UNION SELECT tenant_id,source_id,catalog_id,stream_id,plan_key,'' FROM pipeline_effect_reservations WHERE project_id=$1
		UNION SELECT tenant_id,source_id,catalog_id,stream_id,prefix,coalesce(nullif(generation,''),'invalid') FROM pipeline_effect_cleanup WHERE project_id=$1
		UNION SELECT tenant_id,source_id,catalog_id,stream_id,body_key,'' FROM pipeline_effect_jobs WHERE project_id=$1
		UNION SELECT tenant_id,source_id,catalog_id,stream_id,assembly_manifest_key,'' FROM pipeline_effect_jobs WHERE project_id=$1
		UNION SELECT tenant_id,source_id,catalog_id,stream_id,'','' FROM pipeline_effect_streams WHERE project_id=$1
		UNION SELECT tenant_id,source_id,catalog_id,stream_id,'','' FROM pipeline_effect_assemblies WHERE project_id=$1
		UNION SELECT tenant_id,source_id,catalog_id,stream_id,'','' FROM pipeline_effect_plans WHERE project_id=$1
		UNION SELECT p.tenant_id,source_id,catalog_id,stream_id,
			CASE WHEN target.rule_key='control-rule/'||target.id||'/'||target.rule_revision AND target.rule_version='' THEN '' ELSE target.rule_key END,''
			FROM pipeline_effect_plans p JOIN pipeline_effect_targets target ON target.plan_id=p.id WHERE p.project_id=$1
		UNION SELECT tenant_id,$2,$3,$4,
			CASE WHEN rule_version='' AND rule_key IN ('control-rule/'||id||'/'||rule_revision, $5||'control-rule/'||id||'/'||rule_revision)
			THEN '' ELSE rule_key END,''
			FROM pipeline_effect_definitions WHERE project_id=$1
	)
	SELECT tenant_id,source_id,catalog_id,stream_id,key,prefix_owner FROM owned`,
		scope.Key.ProjectID, scope.Key.SourceID, scope.CatalogID, scope.StreamID, hostedScheduledRulePrefix)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	type ownedKey struct {
		key, owner string
	}
	var keys []ownedKey
	for rows.Next() {
		var who, source, catalog, stream string
		var owned ownedKey
		if err := rows.Scan(&who, &source, &catalog, &stream, &owned.key, &owned.owner); err != nil {
			return nil, err
		}
		if who != tenant || source != scope.Key.SourceID || catalog != scope.CatalogID || stream != scope.StreamID || owned.owner != "" && owned.key == "" {
			return nil, errHostedOutputOwnership
		}
		if owned.key != "" {
			keys = append(keys, owned)
		}
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return nil, nil
	}
	if objects == nil {
		return nil, errHostedOutputOwnership
	}

	var artifacts []HostedDeletionArtifact
	seen := make(map[s3client.Artifact]struct{})
	storage := objects.HostedEffectStorageID()
	for _, owned := range keys {
		if owned.owner != "" {
			_, expected, err := objects.DescribeImmutableEffectObject(scope, owned.owner, []byte("{}"))
			if err != nil || expected != owned.key {
				return nil, errHostedOutputOwnership
			}
		}
		inventory, err := objects.InventoryHostedEffectArtifacts(ctx, scope, owned.key, owned.owner != "")
		if err != nil {
			return nil, err
		}
		for _, item := range inventory {
			if _, exists := seen[item]; exists {
				continue
			}
			seen[item] = struct{}{}
			artifacts = append(artifacts, HostedDeletionArtifact{
				Kind: HostedDeletionPipeline, StorageID: storage, Key: item.Key, Version: item.VersionID, Upload: item.UploadID,
			})
		}
	}
	return artifacts, nil
}

func DeleteHostedPipelineArtifact(ctx context.Context, scope lakeapi.HostedProjectScope, objects *lakeapi.DuckLakeObjectStore, a HostedDeletionArtifact) error {
	if objects == nil || a.Kind != HostedDeletionPipeline {
		return errHostedOutputOwnership
	}
	return objects.DeleteHostedEffectArtifact(ctx, scope, a.StorageID, s3client.Artifact{
		Key: a.Key, VersionID: a.Version, UploadID: a.Upload,
	})
}
