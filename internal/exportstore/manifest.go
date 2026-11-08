package exportstore

import (
	"bytes"
	"context"
	"errors"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

var ErrNotFound = errors.New("S3 object not found")
var ErrExists = errors.New("S3 object already exists")

const MaxManifestBytes = 4 << 20

// ReadManifest reads only a bounded commit marker. Data objects remain streaming.
func (s *Store) ReadManifest(ctx context.Context, key string) ([]byte, error) {
	full, err := s.objectKey(key)
	if err != nil {
		return nil, err
	}
	response, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(full)})
	if err != nil {
		var api smithy.APIError
		if errors.As(err, &api) && (api.ErrorCode() == "NoSuchKey" || api.ErrorCode() == "NotFound") {
			return nil, ErrNotFound
		}
		return nil, storageError(ctx)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, MaxManifestBytes+1))
	if err != nil || len(data) > MaxManifestBytes {
		return nil, storageError(ctx)
	}
	return data, nil
}

// CommitManifest atomically creates an immutable commit marker after all data
// objects and the complete source response have been verified.
func (s *Store) CommitManifest(ctx context.Context, key string, data []byte) error {
	full, err := s.objectKey(key)
	if err != nil {
		return err
	}
	if len(data) == 0 || len(data) > MaxManifestBytes {
		return ErrStorage
	}
	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(full), Body: bytes.NewReader(data), ContentType: aws.String("application/json"), IfNoneMatch: aws.String("*")})
	if err != nil {
		var api smithy.APIError
		if errors.As(err, &api) && (api.ErrorCode() == "PreconditionFailed" || api.ErrorCode() == "ConditionalRequestConflict") {
			return ErrExists
		}
		return storageError(ctx)
	}
	return nil
}
