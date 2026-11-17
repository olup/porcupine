package output

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/olup/porcupine/internal/exportstore"
	"github.com/olup/porcupine/internal/s3client"
)

const HostedDeletionScheduled = "scheduled-s3"

type hostedScheduledFile struct {
	key, upload string
}

func (w *hostedScheduledWork) ownsFile(key string) bool {
	prefix := fmt.Sprintf("%s/%d/%s/part-", w.pipeline.ID, w.start, w.attemptID)
	suffix := "." + w.spec.Format + ".gz"
	if w.attemptID == "" || !strings.HasPrefix(key, prefix) || !strings.HasSuffix(key, suffix) || len(key) != len(prefix)+6+len(suffix) {
		return false
	}
	part, err := strconv.Atoi(key[len(prefix) : len(prefix)+6])
	return err == nil && part >= 1 && part <= hostedScheduledMaxFiles &&
		key[len(prefix):len(prefix)+6] == fmt.Sprintf("%06d", part)
}

func listHostedScheduledFiles(ctx context.Context, tx *sql.Tx, id string) ([]hostedScheduledFile, error) {
	rows, err := tx.QueryContext(ctx, `SELECT object_key,upload_id FROM hosted_s3_pipeline_files WHERE run_id=$1 ORDER BY object_key`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var files []hostedScheduledFile
	for rows.Next() {
		var f hostedScheduledFile
		if err = rows.Scan(&f.key, &f.upload); err != nil {
			return nil, err
		}
		files = append(files, f)
	}
	return files, rows.Err()
}

func exactScheduledFileArtifacts(ctx context.Context, w *hostedScheduledWork, store *exportstore.Store, f hostedScheduledFile) ([]s3client.Artifact, error) {
	if !w.ownsFile(f.key) {
		return nil, errHostedOutputOwnership
	}
	artifacts, err := store.ListExactArtifacts(ctx, f.key)
	if err != nil || f.upload == "" {
		return artifacts, err
	}
	for _, item := range artifacts {
		if item.UploadID == f.upload {
			return artifacts, nil
		}
	}
	present, err := store.ExactUploadExists(ctx, f.key, f.upload)
	if err != nil {
		return nil, err
	}
	if present {
		artifacts = append(artifacts, s3client.Artifact{UploadID: f.upload})
	}
	return artifacts, nil
}

// scheduledManifestPublished distinguishes a customer-owned result from an
// unfinished attempt. An absent current marker with older versions cannot
// authorize reclamation of a checkpointed attempt.
func scheduledManifestPublished(ctx context.Context, w *hostedScheduledWork, store *exportstore.Store) (bool, error) {
	key := pipelineManifestKey(&w.s3PipelineWork)
	remote, err := store.ReadManifest(ctx, key)
	switch {
	case err == nil:
		if w.manifest == "" || !bytes.Equal(remote, []byte(w.manifest)) {
			return false, errHostedOutputOwnership
		}
		return true, nil
	case !errors.Is(err, exportstore.ErrNotFound):
		return false, err
	case w.manifest == "":
		return false, nil
	}
	versions, err := store.ListExactArtifacts(ctx, key)
	if err != nil {
		return false, err
	}
	if len(versions) != 0 {
		return false, errHostedOutputOwnership
	}
	return false, nil
}

func requireScheduledDeletionScope(ctx context.Context, tx *sql.Tx, tenant, project string) error {
	var fenced bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS (
SELECT 1 FROM hosted_project_seals seal
JOIN project_deletions d USING(project_id,tenant_id)
JOIN projects p ON (p.id,p.tenant_id)=(seal.project_id,seal.tenant_id)
WHERE seal.project_id=$1 AND seal.tenant_id=$2 AND d.completed=0
AND NOT EXISTS(SELECT 1 FROM hosted_project_activity WHERE project_id=$1))`, project, tenant).Scan(&fenced); err != nil {
		return err
	}
	if !fenced {
		return errHostedOutputOwnership
	}
	return nil
}

const scheduledDeletionRuns = ` FROM hosted_s3_pipeline_runs r
JOIN pipeline_effect_definitions d ON (d.project_id,d.id)=(r.project_id,r.pipeline_id)
JOIN hosted_pipeline_configs c ON (c.project_id,c.id,c.tenant_id)=(d.project_id,d.id,d.tenant_id)
WHERE r.project_id=$1 AND c.tenant_id=$2 AND c.kind='s3' AND r.attempt_id<>'' AND r.state<>'succeeded'`

func scanScheduledDeletionRun(row interface{ Scan(...any) error }, w *hostedScheduledWork) error {
	var config string
	err := row.Scan(&w.id, &w.pipeline.ID, &w.start, &w.end, &w.attemptID, &w.previousRevision, &w.previousSecret, &w.manifest, &w.incarnation, &config)
	if err != nil {
		return err
	}
	w.pipeline.Config = json.RawMessage(config)
	return json.Unmarshal(w.pipeline.Config, &w.spec)
}

const scheduledDeletionColumns = `SELECT r.id,r.pipeline_id,r.window_start_ms,r.window_end_ms,r.attempt_id,r.destination_revision,r.secret,r.manifest,d.incarnation,c.config`

// HostedScheduledDeletionInventory resolves only objects written by a drained
// project attempt. Matching customer commit markers keep their data files.
func (s *PipelineJobs) HostedScheduledDeletionInventory(ctx context.Context, tx *sql.Tx, tenant, project string) ([]HostedDeletionArtifact, int64, error) {
	if err := requireScheduledDeletionScope(ctx, tx, tenant, project); err != nil {
		return nil, 0, err
	}
	rows, err := tx.QueryContext(ctx, scheduledDeletionColumns+scheduledDeletionRuns, project, tenant)
	if err != nil {
		return nil, 0, err
	}
	var runs []hostedScheduledWork
	for rows.Next() {
		w := hostedScheduledWork{tenant: tenant}
		w.pipeline.ProjectID = project
		if err = scanScheduledDeletionRun(rows, &w); err != nil {
			break
		}
		runs = append(runs, w)
	}
	if err = errors.Join(err, rows.Err(), rows.Close()); err != nil {
		return nil, 0, err
	}
	var out []HostedDeletionArtifact
	var publishedBytes int64
	for _, w := range runs {
		batch, bytes, err := s.inventoryHostedScheduledRun(ctx, tx, &w)
		if err != nil {
			return nil, 0, err
		}
		out = append(out, batch...)
		publishedBytes += bytes
	}
	return out, publishedBytes, nil
}

func (s *PipelineJobs) inventoryHostedScheduledRun(ctx context.Context, tx *sql.Tx, w *hostedScheduledWork) ([]HostedDeletionArtifact, int64, error) {
	files, err := listHostedScheduledFiles(ctx, tx, w.id)
	if err != nil {
		return nil, 0, err
	}
	var manifest pipelineS3Manifest
	if w.manifest != "" {
		manifest, err = readHostedScheduledManifest(w)
		if err != nil || len(manifest.Files) != len(files) {
			return nil, 0, errHostedOutputOwnership
		}
		for i, f := range files {
			if !w.ownsFile(f.key) || manifest.Files[i].Key != w.spec.Destination.Prefix+"/"+f.key {
				return nil, 0, errHostedOutputOwnership
			}
		}
	}
	if len(files) == 0 {
		return nil, 0, nil
	}
	store, err := s.hostedScheduledStore(ctx, w, w.previousRevision, w.previousSecret)
	if err != nil {
		return nil, 0, err
	}
	defer store.Close()
	published, err := scheduledManifestPublished(ctx, w, store)
	if err != nil {
		return nil, 0, err
	}
	if published {
		return nil, pipelineManifestBytes(manifest), nil
	}
	var out []HostedDeletionArtifact
	for _, f := range files {
		identities, err := exactScheduledFileArtifacts(ctx, w, store, f)
		if err != nil {
			return nil, 0, err
		}
		for _, a := range identities {
			out = append(out, HostedDeletionArtifact{Kind: HostedDeletionScheduled, StorageID: store.OwnershipID(), Key: f.key, Version: a.VersionID, Upload: a.UploadID})
		}
	}
	return out, 0, nil
}

// DeleteHostedScheduledArtifact is called after the recovery floor and under
// the same project session as inventory. Recheck the current marker because an
// earlier remote PUT may have had an ambiguous acknowledgement.
func (s *PipelineJobs) DeleteHostedScheduledArtifact(ctx context.Context, tenant, project string, a HostedDeletionArtifact) error {
	if a.Kind != HostedDeletionScheduled {
		return errHostedOutputOwnership
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = requireScheduledDeletionScope(ctx, tx, tenant, project); err != nil {
		return err
	}
	w := hostedScheduledWork{tenant: tenant}
	w.pipeline.ProjectID = project
	err = scanScheduledDeletionRun(tx.QueryRowContext(ctx, scheduledDeletionColumns+scheduledDeletionRuns+`
AND EXISTS(SELECT 1 FROM hosted_s3_pipeline_files WHERE run_id=r.id AND object_key=$3)
AND NOT EXISTS(SELECT 1 FROM hosted_s3_pipeline_files f JOIN hosted_s3_pipeline_runs other ON other.id=f.run_id
 WHERE other.project_id<>$1 AND f.object_key=$3)`, project, tenant, a.Key), &w)
	if err != nil {
		return err
	}
	if !w.ownsFile(a.Key) {
		return errHostedOutputOwnership
	}
	store, err := s.hostedScheduledStore(ctx, &w, w.previousRevision, w.previousSecret)
	if err != nil {
		return err
	}
	defer store.Close()
	if store.OwnershipID() != a.StorageID {
		return errHostedOutputOwnership
	}
	published, err := scheduledManifestPublished(ctx, &w, store)
	if err != nil || published {
		return err
	}
	return store.DeleteExactArtifact(ctx, a.Key, a.Version, a.Upload)
}
