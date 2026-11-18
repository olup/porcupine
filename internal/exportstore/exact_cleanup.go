package exportstore

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"

	"github.com/olup/porcupine/internal/s3client"
)

// ListExactArtifacts inventories versions, delete markers and unfinished uploads
// for a single managed key. Prefix matches for other keys are ignored.
func (s *Store) ListExactArtifacts(ctx context.Context, key string) ([]s3client.Artifact, error) {
	full, err := s.objectKey(key)
	if err != nil {
		return nil, err
	}
	artifacts, err := s3client.InventoryArtifacts(ctx, s.client, s.bucket, full, func(candidate string) bool { return candidate == full })
	if err != nil {
		return nil, storageError(ctx)
	}
	return artifacts, nil
}

// DeleteExactArtifact removes only the named version or upload.
func (s *Store) DeleteExactArtifact(ctx context.Context, key, version, upload string) error {
	full, err := s.objectKey(key)
	if err != nil {
		return err
	}
	if err := s3client.DeleteArtifact(ctx, s.client, s.bucket, s3client.Artifact{Key: full, VersionID: version, UploadID: upload}); err != nil {
		return storageError(ctx)
	}
	return nil
}

// ExactUploadExists checks a checkpointed upload independently of the listing.
func (s *Store) ExactUploadExists(ctx context.Context, key, upload string) (bool, error) {
	full, err := s.objectKey(key)
	if err != nil {
		return false, err
	}
	if upload == "" || len(upload) > 4096 {
		return false, ErrKey
	}
	_, err = s.client.ListParts(ctx, &s3.ListPartsInput{
		Bucket: aws.String(s.bucket), Key: aws.String(full), UploadId: aws.String(upload), MaxParts: aws.Int32(1),
	})
	if err == nil {
		return true, nil
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && apiErr.ErrorCode() == "NoSuchUpload" {
		return false, nil
	}
	return false, storageError(ctx)
}
