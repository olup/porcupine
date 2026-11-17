package lake

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/olup/porcupine/internal/event"
)

func TestDuckLakeS3SharedPostgresHostedDeletionInventoryAndDrop(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	ctx, a, b, config := sharedNativeStores(t)
	control, coordinator := nativeFenceControl(t)
	if _, err := control.ExecContext(ctx, `CREATE SEQUENCE shared_event_id_sequence AS BIGINT INCREMENT BY 1024 MINVALUE 1 CACHE 1 NO CYCLE`); err != nil {
		t.Fatal(err)
	}
	key := ProjectWriteKey{SourceID: config.SharedSourceID, ProjectID: "delete-selected"}
	scope, err := a.InitializeHostedProject(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	other, err := b.InitializeHostedProject(ctx, ProjectWriteKey{SourceID: config.SharedSourceID, ProjectID: "delete-neighbor"})
	if err != nil {
		t.Fatal(err)
	}
	owner := acquireFenceSession(t, coordinator, key)
	fence := owner.Fence(scope.StreamID)
	if err := a.AdvanceWriteFence(ctx, fence); err != nil {
		t.Fatal(err)
	}
	if _, err := a.InventorySealedHostedProject(ctx, HostedProjectSeal{Scope: scope}); !errors.Is(err, ErrProjectWriteFenceStale) {
		t.Fatal("open catalog accepted", err)
	}
	if err := a.DropSealedHostedProject(ctx, HostedProjectSeal{Scope: scope}); !errors.Is(err, ErrProjectWriteFenceStale) {
		t.Fatal("drop without sealed inventory accepted", err)
	}
	batch := LiveCommitBatch{ID: "inventory-live", TenantID: "tenant-a", Scope: scope, Inputs: []LiveCommitInput{newLiveInput(t, key.ProjectID, "viewed", event.NewID(), "person", `{}`)}}
	commit := commitLiveFixture(t, a, owner, batch, &liveProofStager{objects: a.Remote, attempt: fence.AcquisitionID}).(CommittedLiveBatch)
	// A parser PUT can succeed before native proof commit. Its reserved owner
	// prefix contains the exact new object even if it never appeared in proof.
	parser, err := a.Remote.PutImmutableImportCheckpoint(ctx, scope, "import-job", "import-file", []byte(`{"state":"fixture"}`))
	if err != nil {
		t.Fatal(err)
	}
	parserPrefix, err := a.Remote.HostedImportCheckpointPrefix(scope, "import-job", "import-file")
	if err != nil {
		t.Fatal(err)
	}
	unknownParser := parserPrefix + "other.json"
	if _, err := a.Remote.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(config.S3.Bucket), Key: aws.String(unknownParser), Body: strings.NewReader("fixture"),
	}); err != nil {
		t.Fatal(err)
	}
	upload, err := a.Remote.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(config.S3.Bucket), Key: aws.String(parser.Key),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Remote.client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket: aws.String(config.S3.Bucket), Key: aws.String(parser.Key), UploadId: upload.UploadId,
		PartNumber: aws.Int32(1), Body: strings.NewReader("incomplete parser upload"),
	}); err != nil {
		t.Fatal(err)
	}
	seal, err := a.SealHostedProject(ctx, fence)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.InventorySealedHostedProject(ctx, seal); !errors.Is(err, ErrNativeEvidenceGap) {
		t.Fatal("seal without acknowledged effects accepted", err)
	}
	if err := a.AcknowledgeNativeEffects(ctx, fence, scope, seal.FinalSequence); err != nil {
		t.Fatal(err)
	}

	scheduledSuffix := "main/heads/scheduled.parquet"
	prefix := a.Remote.ProjectPrefix(key.ProjectID)
	scheduled, sibling := prefix+scheduledSuffix, prefix+scheduledSuffix+".sentinel"
	neighbor := a.Remote.ProjectPrefix(other.Key.ProjectID) + "sentinel"
	for _, object := range []string{scheduled, sibling, neighbor} {
		if _, err := a.Remote.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(config.S3.Bucket), Key: aws.String(object), Body: strings.NewReader("fixture"),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := a.SharedCatalog.ExecContext(ctx, "INSERT INTO "+SharedNativeSchema(scope.CatalogID)+
		".ducklake_files_scheduled_for_deletion (data_file_id,path,path_is_relative,schedule_start) VALUES(-1,$1,true,now())", scheduledSuffix); err != nil {
		t.Fatal(err)
	}
	inventory, err := a.InventorySealedHostedProject(ctx, seal)
	if err != nil {
		t.Fatal(err)
	}
	if len(inventory.ParquetKeys) <= 1 || !slices.Contains(inventory.ParquetKeys, scheduled) || slices.Contains(inventory.ParquetKeys, sibling) || slices.Contains(inventory.ParquetKeys, neighbor) {
		t.Fatal("unexpected exact Parquet inventory", inventory.ParquetKeys)
	}
	if len(inventory.LiveManifests) != 1 || inventory.LiveManifests[0] != commit.Evidence.Effects.Manifest {
		t.Fatal("missing exact live manifest", inventory.LiveManifests)
	}
	partsInput := &s3.ListPartsInput{Bucket: aws.String(config.S3.Bucket), Key: aws.String(parser.Key), UploadId: upload.UploadId}
	if parts, err := a.Remote.client.ListParts(ctx, partsInput); err != nil || len(parts.Parts) != 1 {
		t.Fatal("fixture could not verify active upload", parts, err)
	}
	if err := a.AbortHostedProjectObjectUpload(ctx, scope, parser.Key, aws.ToString(upload.UploadId)); err != nil {
		t.Fatal(err)
	}
	var apiErr smithy.APIError
	if _, err := a.Remote.client.ListParts(ctx, partsInput); !errors.As(err, &apiErr) || apiErr.ErrorCode() != "NoSuchUpload" {
		t.Fatal("aborted upload remained available", err)
	}
	if err := a.AbortHostedProjectObjectUpload(ctx, scope, parser.Key, aws.ToString(upload.UploadId)); err != nil {
		t.Fatal("exact upload abort retry", err)
	}
	parserArtifacts, err := a.HostedImportCheckpointArtifacts(ctx, scope, "import-job", "import-file")
	if err != nil || len(parserArtifacts) != 1 || parserArtifacts[0].Key != parser.Key {
		t.Fatal("reserved parser inventory is incorrect", parserArtifacts, err)
	}
	if err := a.DeleteHostedProjectObject(ctx, scope, parser.Key, parserArtifacts[0].VersionID); err != nil {
		t.Fatal(err)
	}
	stale := seal
	stale.FinalSequence++
	if err := a.DropSealedHostedProject(ctx, stale); !errors.Is(err, ErrProjectWriteFenceStale) {
		t.Fatal("wrong seal accepted", err)
	}
	if err := a.DeleteHostedProjectObject(ctx, scope, neighbor, "null"); !errors.Is(err, ErrProjectWriteFenceStale) {
		t.Fatal("neighbor deletion accepted", err)
	}
	for _, object := range append(inventory.ParquetKeys, inventory.LiveManifests[0].Key) {
		versions, err := a.HostedProjectObjectVersions(ctx, scope, object)
		if err != nil || len(versions) != 1 {
			t.Fatal("expected one exact object version", object, versions, err)
		}
		if err := a.DeleteHostedProjectObject(ctx, scope, object, versions[0]); err != nil {
			t.Fatal(err)
		}
		if err := a.DeleteHostedProjectObject(ctx, scope, object, versions[0]); err != nil {
			t.Fatal("exact deletion retry", err)
		}
	}
	if err := b.DropSealedHostedProject(ctx, seal); err != nil {
		t.Fatal("cold drop after deleting native guard files", err)
	}
	if err := a.DropSealedHostedProject(ctx, seal); err != nil {
		t.Fatal("drop retry", err)
	}
	if err := a.DeleteProject(ctx, key.ProjectID); !errors.Is(err, ErrHostedNativeGuardRequired) {
		t.Fatal("legacy retry could sweep project prefix", err)
	}
	record, err := b.registry.lookup(ctx, key.ProjectID)
	if err != nil || !record.Deleted || record.CatalogID != scope.CatalogID {
		t.Fatal("catalog tombstone lost", record, err)
	}
	var schemaExists bool
	if err := b.SharedCatalog.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname=$1)`, SharedNativeSchema(scope.CatalogID)).Scan(&schemaExists); err != nil || schemaExists {
		t.Fatal("catalog schema still present", schemaExists, err)
	}
	if got, err := b.LookupHostedProjectExisting(ctx, other.Key); err != nil || got != other {
		t.Fatal("neighbor catalog changed", got, err)
	}
	for _, object := range []string{sibling, neighbor, unknownParser} {
		if _, err := b.Remote.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(config.S3.Bucket), Key: aws.String(object)}); err != nil {
			t.Fatal("sentinel object removed", object, err)
		}
	}
}
