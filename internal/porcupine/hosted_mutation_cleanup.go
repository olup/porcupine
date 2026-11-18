package porcupine

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"

	lakeapi "github.com/olup/porcupine/internal/lake"
)

func (f *hostedLiveFlusher) hostedMutationCleanupObjects(ctx context.Context, scope lakeapi.HostedProjectScope, tenant string) (_ []hostedCleanupObject, err error) {
	rows, err := f.publisher.control.QueryContext(ctx, `SELECT source_id,tenant_id,catalog_id,stream_id,mutation_id,state,
body_prefix,body_key,body_version,body_sha256,body_bytes FROM hosted_mutation_intents WHERE project_id=$1`, scope.Key.ProjectID)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, rows.Close()) }()
	var artifacts []hostedCleanupObject
	for rows.Next() {
		var src, owner, catalog, stream, id, state, prefix string
		var ref lakeapi.ImmutableObjectRef
		if err := rows.Scan(&src, &owner, &catalog, &stream, &id, &state, &prefix, &ref.Key, &ref.VersionID, &ref.SHA256, &ref.Bytes); err != nil {
			return nil, err
		}
		digest, err := hex.DecodeString(ref.SHA256)
		expected := f.native.Remote.ProjectPrefix(scope.Key.ProjectID) + "mutations/" + scope.StreamID + "/" + id + "/"
		if src != scope.Key.SourceID || owner != tenant || catalog != scope.CatalogID || stream != scope.StreamID || stream == "" ||
			len(id) != 36 || !lakeapi.LakePrefixSegmentPattern.MatchString(id) || state == "ready" ||
			prefix != expected || ref.Key != expected+ref.SHA256+".json" || err != nil || len(digest) != sha256.Size ||
			ref.Bytes <= 0 || ref.Bytes > lakeapi.MaxImmutableEffectObjectBytes {
			return nil, errHostedMutationConflict
		}
		versions, err := f.native.HostedProjectObjectVersions(ctx, scope, ref.Key)
		if err != nil {
			return nil, err
		}
		for _, version := range versions {
			artifacts = append(artifacts, hostedCleanupObject{kind: "analytics", key: ref.Key, version: version})
		}
	}
	return artifacts, rows.Err()
}

func finalizeHostedMutationsTx(ctx context.Context, tx *sql.Tx, scope lakeapi.HostedProjectScope, tenant string) error {
	var charged int64
	var settled bool
	err := tx.QueryRowContext(ctx, `SELECT retained_bytes,accepted_through=settled_through FROM hosted_mutation_projects
WHERE source_id=$1 AND project_id=$2 AND tenant_id=$3 AND catalog_id=$4 AND stream_id=$5 FOR UPDATE`,
		scope.Key.SourceID, scope.Key.ProjectID, tenant, scope.CatalogID, scope.StreamID).Scan(&charged, &settled)
	if errors.Is(err, sql.ErrNoRows) {
		var hasRecords bool
		err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM hosted_mutation_projects WHERE project_id=$1)`, scope.Key.ProjectID).Scan(&hasRecords)
		if err == nil && hasRecords {
			return errHostedMutationConflict
		}
		return err
	}
	if err != nil {
		return err
	}
	if !settled {
		return errHostedMutationConflict
	}
	var inventoryBytes int64
	if err := tx.QueryRowContext(ctx, `SELECT coalesce(sum(body_bytes),0) FROM hosted_mutation_intents
WHERE source_id=$1 AND project_id=$2`, scope.Key.SourceID, scope.Key.ProjectID).Scan(&inventoryBytes); err != nil {
		return err
	}
	if charged != inventoryBytes {
		return errHostedMutationConflict
	}
	for _, bucket := range [][2]string{{"instance", ""}, {"tenant", tenant}} {
		var remaining int64
		if err := tx.QueryRowContext(ctx, `UPDATE hosted_mutation_capacity SET retained_bytes=retained_bytes-$3
WHERE scope_kind=$1 AND scope_id=$2 AND retained_bytes>=$3 RETURNING retained_bytes`, bucket[0], bucket[1], charged).Scan(&remaining); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM hosted_mutation_intents WHERE source_id=$1 AND project_id=$2`, scope.Key.SourceID, scope.Key.ProjectID); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `DELETE FROM hosted_mutation_projects WHERE source_id=$1 AND project_id=$2`, scope.Key.SourceID, scope.Key.ProjectID)
	return err
}
