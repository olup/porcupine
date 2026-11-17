package lake

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/olup/porcupine/internal/event"
)

func TestDuckLakeS3ImmutableEffectVersionPin(t *testing.T) {
	t.Parallel()
	fixture := nativeS3Fixture(t)
	ctx := t.Context()
	config := fixture.storage
	config.Bucket = "porcupine-effects-" + event.NewID()
	if _, err := fixture.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(config.Bucket)}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		// This isolated versioned bucket is used only by this test.
		versions, err := fixture.client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: aws.String(config.Bucket)})
		if err != nil {
			t.Error(err)
			return
		}
		objects := []types.ObjectIdentifier{}
		for _, v := range versions.Versions {
			objects = append(objects, types.ObjectIdentifier{Key: v.Key, VersionId: v.VersionId})
		}
		for _, v := range versions.DeleteMarkers {
			objects = append(objects, types.ObjectIdentifier{Key: v.Key, VersionId: v.VersionId})
		}
		if len(objects) > 0 {
			if _, err := fixture.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String(config.Bucket), Delete: &types.Delete{Objects: objects}}); err != nil {
				t.Error(err)
				return
			}
		}
		if _, err := fixture.client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(config.Bucket)}); err != nil {
			t.Error(err)
		}
	})
	if _, err := fixture.client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{Bucket: aws.String(config.Bucket), VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled}}); err != nil {
		t.Fatal(err)
	}
	normalized, err := NormalizeDuckLakeS3(&config)
	if err != nil {
		t.Fatal(err)
	}
	source := event.NewID()
	objects, err := newDuckLakeObjectStore(ctx, *normalized, source)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(objects.transport.CloseIdleConnections)
	key := ProjectWriteKey{SourceID: source, ProjectID: "project"}
	scope := HostedProjectScope{Key: key, CatalogID: SharedNativeCatalogID(source, key.ProjectID), StreamID: event.NewID()}
	attempt := event.NewID()
	original := []byte(`{"immutable":"body"}`)
	first, err := objects.PutImmutableEffectObject(ctx, scope, attempt, original)
	if err != nil || first.VersionID == "" {
		t.Fatal("versioned object did not retain exact version", first, err)
	}
	// An existing conditional PUT has the same resolution path as a lost PUT
	// response. Recover the original version instead of returning an unpinned ref.
	again, err := objects.PutImmutableEffectObject(ctx, scope, attempt, original)
	if err != nil || again != first {
		t.Fatal("ambiguous PUT recovery lost version pin", again, first, err)
	}
	if _, err := fixture.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(config.Bucket), Key: aws.String(first.Key), Body: bytes.NewReader([]byte(`{"external":"rewrite"}`))}); err != nil {
		t.Fatal(err)
	}
	raw, err := objects.ReadImmutableEffectObject(ctx, scope, first)
	if err != nil || !bytes.Equal(raw, original) {
		t.Fatal("pinned read followed a newer version", string(raw), err)
	}
	unpinned := first
	unpinned.VersionID = ""
	if _, err := objects.ReadImmutableEffectObject(ctx, scope, unpinned); err == nil {
		t.Fatal("changed bytes passed digest validation")
	}
	other := scope
	other.Key.ProjectID = "other"
	other.CatalogID = SharedNativeCatalogID(source, other.Key.ProjectID)
	if _, err := objects.ReadImmutableEffectObject(ctx, other, first); err == nil {
		t.Fatal("cross-project immutable reference accepted")
	}
}
